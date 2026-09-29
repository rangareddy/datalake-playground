"""Write, update and read one small orders table in Iceberg, Hudi and Delta.

Run inside the datalake-playground stack:
  docker cp lakehouse_check.py spark-master:/tmp/
  docker exec spark-master bash -c '$SPARK_HOME/bin/spark-submit --master "local[2]" \
    --driver-memory 1g --jars "$(ls $ICEBERG_HOME/*.jar $DELTA_HOME/*.jar $HUDI_HOME/hudi-spark*-bundle_*.jar \
              | paste -sd, -)" \
    /tmp/lakehouse_check.py'
"""
from pyspark.sql import SparkSession

BASE = "s3a://warehouse/playground_check"

spark = (
    SparkSession.builder.appName("lakehouse-check")
    # Order matters: with Iceberg's extension ahead of Delta's, Delta's SQL UPDATE fails
    # with scala.MatchError: DeltaTableV2(...). Delta first, then Iceberg, works for both.
    .config("spark.sql.extensions", ",".join([
        "io.delta.sql.DeltaSparkSessionExtension",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    ]))
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # Delta must own spark_catalog; Iceberg gets its own catalog; Hudi writes by path.
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ice.type", "hive")
    .config("spark.sql.catalog.ice.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.ice.warehouse", "s3a://warehouse/ice")
    .config("spark.eventLog.enabled", "false")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
sql = spark.sql

orders = spark.createDataFrame(
    [(1001, "PLACED", 120.50), (1002, "PLACED", 75.00), (1003, "PLACED", 310.25)],
    "order_id BIGINT, status STRING, amount DOUBLE",
)
results = []


def check(fmt, rows, shipped):
    ok = rows == 3 and shipped == 1
    results.append(ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {fmt:<8} rows={rows} shipped={shipped}")


print("spark", spark.version)

# Iceberg, through the Hive Metastore
sql("CREATE NAMESPACE IF NOT EXISTS ice.playground_check")
sql("DROP TABLE IF EXISTS ice.playground_check.orders PURGE")
orders.writeTo("ice.playground_check.orders").using("iceberg").create()
sql("UPDATE ice.playground_check.orders SET status = 'SHIPPED' WHERE order_id = 1001")
df = spark.table("ice.playground_check.orders")
check("iceberg", df.count(), df.where("status = 'SHIPPED'").count())

# Hudi, by path
hudi_path = f"{BASE}/hudi_orders"
hudi_opts = {
    "hoodie.table.name": "hudi_orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "amount",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.keygenerator.class":
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
}
orders.write.format("hudi").options(**hudi_opts).mode("overwrite").save(hudi_path)
(orders.where("order_id = 1001").selectExpr("order_id", "'SHIPPED' AS status", "amount")
       .write.format("hudi").options(**hudi_opts)
       .option("hoodie.datasource.write.operation", "upsert").mode("append").save(hudi_path))
df = spark.read.format("hudi").load(hudi_path)
check("hudi", df.count(), df.where("status = 'SHIPPED'").count())

# Delta, by path
delta_path = f"{BASE}/delta_orders"
orders.write.format("delta").mode("overwrite").save(delta_path)
sql(f"UPDATE delta.`{delta_path}` SET status = 'SHIPPED' WHERE order_id = 1001")
df = spark.read.format("delta").load(delta_path)
check("delta", df.count(), df.where("status = 'SHIPPED'").count())

# Clean up everything this script created
sql("DROP TABLE ice.playground_check.orders PURGE")
sql("DROP NAMESPACE ice.playground_check")
# DROP NAMESPACE leaves the namespace's directory marker behind, so remove it too.
jvm = spark._jvm
for leftover in (BASE, "s3a://warehouse/playground_check.db"):
    path = jvm.org.apache.hadoop.fs.Path(leftover)
    path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)

print(f"passed {sum(results)} of {len(results)}")
spark.stop()
raise SystemExit(0 if all(results) else 1)
