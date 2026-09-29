"""Write, update and read one small orders table in Iceberg, Hudi and Delta.

Run inside the datalake-playground stack:
  docker cp lakehouse_check.py spark-master:/tmp/
  docker exec spark-master bash -c '$SPARK_HOME/bin/spark-submit --master "local[2]" \
    --driver-memory 1g \
    --jars "$(ls $ICEBERG_HOME/*.jar $DELTA_HOME/*.jar $HUDI_HOME/hudi-spark*-bundle_*.jar \
              | paste -sd, -)" \
    /tmp/lakehouse_check.py'

Each format gets its own function, so one can be run or debugged on its own. Every
function writes three orders, marks order 1001 as SHIPPED, reads the table back and
returns (row_count, shipped_count). The expected result is (3, 1) for all three.
"""
from pyspark.sql import DataFrame, SparkSession

BASE = "s3a://warehouse/playground_check"
ICEBERG_NAMESPACE = "ice.playground_check"
ICEBERG_TABLE = f"{ICEBERG_NAMESPACE}.orders"
HUDI_PATH = f"{BASE}/hudi_orders"
DELTA_PATH = f"{BASE}/delta_orders"
# Where the Hive Metastore puts the Iceberg namespace's directory. DROP NAMESPACE
# removes the namespace but leaves this marker behind.
ICEBERG_NAMESPACE_DIR = "s3a://warehouse/playground_check.db"


def create_spark_session() -> SparkSession:
    """One session that can use all three formats.

    Delta must own spark_catalog, Iceberg gets its own catalog named `ice`, and Hudi
    writes by path. The extension order matters: with Iceberg's extension ahead of
    Delta's, Delta's SQL UPDATE fails with scala.MatchError: DeltaTableV2(...).
    """
    spark = (
        SparkSession.builder.appName("lakehouse-check")
        .config("spark.sql.extensions", ",".join([
            "io.delta.sql.DeltaSparkSessionExtension",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        ]))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hive")
        .config("spark.sql.catalog.ice.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.ice.warehouse", "s3a://warehouse/ice")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def sample_orders(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(1001, "PLACED", 120.50), (1002, "PLACED", 75.00), (1003, "PLACED", 310.25)],
        "order_id BIGINT, status STRING, amount DOUBLE",
    )


def counts(df: DataFrame) -> tuple:
    return df.count(), df.where("status = 'SHIPPED'").count()


def check_iceberg(spark: SparkSession) -> tuple:
    """Iceberg table in the Hive Metastore, updated with SQL."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_NAMESPACE}")
    spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_TABLE} PURGE")
    sample_orders(spark).writeTo(ICEBERG_TABLE).using("iceberg").create()
    spark.sql(f"UPDATE {ICEBERG_TABLE} SET status = 'SHIPPED' WHERE order_id = 1001")
    return counts(spark.table(ICEBERG_TABLE))


def check_hudi(spark: SparkSession) -> tuple:
    """Hudi table by path, updated with a DataFrame upsert."""
    options = {
        "hoodie.table.name": "hudi_orders",
        "hoodie.datasource.write.recordkey.field": "order_id",
        "hoodie.datasource.write.precombine.field": "amount",
        "hoodie.datasource.write.partitionpath.field": "",
        "hoodie.datasource.write.keygenerator.class":
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    }
    orders = sample_orders(spark)
    orders.write.format("hudi").options(**options).mode("overwrite").save(HUDI_PATH)
    shipped = (orders.where("order_id = 1001")
                     .selectExpr("order_id", "'SHIPPED' AS status", "amount"))
    (shipped.write.format("hudi").options(**options)
            .option("hoodie.datasource.write.operation", "upsert")
            .mode("append").save(HUDI_PATH))
    return counts(spark.read.format("hudi").load(HUDI_PATH))


def check_delta(spark: SparkSession) -> tuple:
    """Delta table by path, updated with SQL."""
    sample_orders(spark).write.format("delta").mode("overwrite").save(DELTA_PATH)
    spark.sql(f"UPDATE delta.`{DELTA_PATH}` SET status = 'SHIPPED' WHERE order_id = 1001")
    return counts(spark.read.format("delta").load(DELTA_PATH))


def cleanup(spark: SparkSession) -> None:
    """Remove everything the checks created, in the catalog and in MinIO."""
    spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_TABLE} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {ICEBERG_NAMESPACE}")
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    for leftover in (BASE, ICEBERG_NAMESPACE_DIR):
        path = jvm.org.apache.hadoop.fs.Path(leftover)
        path.getFileSystem(conf).delete(path, True)


def main() -> int:
    spark = create_spark_session()
    print("spark", spark.version)
    checks = [("iceberg", check_iceberg), ("hudi", check_hudi), ("delta", check_delta)]
    passed = 0
    try:
        for name, check in checks:
            rows, shipped = check(spark)
            ok = (rows, shipped) == (3, 1)
            passed += ok
            print(f"  {'PASS' if ok else 'FAIL'}  {name:<8} rows={rows} shipped={shipped}")
    finally:
        cleanup(spark)
        spark.stop()
    print(f"passed {passed} of {len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
