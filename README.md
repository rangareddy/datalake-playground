# datalake-playground

A local lakehouse on one machine, for the demos on
[rangareddy.github.io](https://rangareddy.github.io/). The `core` profile starts MinIO as
S3, a Hive Metastore, Spark with Hudi, Iceberg and Delta, and Postgres. The `all` profile
adds Kafka (with Schema Registry, REST Proxy, Kafka Connect with Debezium, and Kafka UI),
MySQL, Trino and Jupyter. Everything is on one Docker network called `datalake`.

This repo is **run-only**. It holds the compose files and the start script. The images,
`rangareddy1988/ranga-*:1.0.0`, are pulled from Docker Hub on the first start; nothing
is built here.

## Contents

- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Verify the stack](#verify-the-stack)
- [Run your own PySpark script](#run-your-own-pyspark-script)
- [Use Kafka (all profile)](#use-kafka-all-profile)
- [Choosing a Spark line](#choosing-a-spark-line)
- [Components and ports](#components-and-ports)
- [Credentials and configuration](#credentials-and-configuration)
- [Day-to-day commands](#day-to-day-commands)
- [Troubleshooting](#troubleshooting)
- [Warnings you can ignore](#warnings-you-can-ignore)
- [Repository layout](#repository-layout)

## Prerequisites

| Requirement | Detail |
| ----------- | ------ |
| Docker | With Compose v2 (`docker compose`). The v1 `docker-compose` binary also works |
| Disk | Free space in Docker's VM for the images plus data: about 10 GB for `core`, 20 GB for `all` |
| Memory | 4 GB for the `core` profile, 10 GB for `all`. At idle they use about 1.9 GB and 6.3 to 7 GB; a Spark job adds 1 to 1.5 GB |
| CPU architecture | The images are `linux/amd64`. On Apple Silicon they run under emulation, which works but is slower |
| Free ports | `core`: 5432, 7077, 8080, 9000-9001, 9083, 10000, 10002, 14040-14042, 18080-18081. `all` adds 2181, 3306, 8081-8083, 8888, 9082, 9084, 9092, 9101, 29092 |

## Quick start

```sh
git clone https://github.com/rangareddy/datalake-playground.git
cd datalake-playground
sh run_datalake.sh start
```

The first start pulls the images, about 5 GB on disk for `core` and 16 GB for `all`. Startup is ordered by health checks, so
expect a couple of minutes while Hive initialises its metastore schema. Then:

```sh
sh run_datalake.sh status
```

Every service should read `Up (healthy)`, except the `mc` sidecar, which has no health
check. To include Kafka, MySQL, Trino and Jupyter:

```sh
PROFILE=all sh run_datalake.sh start
```

## Verify the stack

`Up (healthy)` means a port answers, not that the service works. Two checks cover the
parts most demos use.

**The Spark worker is registered with the master:**

```sh
curl -s http://localhost:8080/json/ | python3 -c \
  "import sys, json; print('alive workers:', json.load(sys.stdin)['aliveworkers'])"
```

It should print `alive workers: 1`. If it prints `0`, jobs submitted to
`spark://spark-master:7077` wait forever; run `docker restart spark-worker` and check
again.

**Each table format can write, update and read through MinIO.** `lakehouse_check.py`
in this repo has one function per format, `check_iceberg`, `check_hudi` and
`check_delta`. Each writes three orders, updates one, and reads them back; at the end
the script deletes everything it created:

```sh
docker cp lakehouse_check.py spark-master:/tmp/
docker exec spark-master bash -c '$SPARK_HOME/bin/spark-submit --master "local[2]" \
  --driver-memory 1g \
  --jars "$(ls $ICEBERG_HOME/*.jar $DELTA_HOME/*.jar $HUDI_HOME/hudi-spark*-bundle_*.jar \
            | paste -sd, -)" \
  /tmp/lakehouse_check.py'
```

`--driver-memory 1g` keeps the check inside a tight Docker VM. With the image's default
2 GB driver and less memory than the profile asks for, the kernel can kill the JVM
part-way through, which shows up as exit code 137 rather than an error.

```text
spark 3.5.9
  PASS  iceberg  rows=3 shipped=1
  PASS  hudi     rows=3 shipped=1
  PASS  delta    rows=3 shipped=1
passed 3 of 3
```

## Run your own PySpark script

The connector jars are already in the Spark image, under `$ICEBERG_HOME`, `$HUDI_HOME`
and `$DELTA_HOME`. Copy a script in and pass the jars it needs:

```sh
docker cp my_job.py spark-master:/tmp/
docker exec spark-master bash -c '$SPARK_HOME/bin/spark-submit --master "local[2]" \
  --jars $(ls $ICEBERG_HOME/iceberg-spark-runtime*.jar) /tmp/my_job.py'
```

Use `--master spark://spark-master:7077` to run on the worker instead of in the driver.

The Iceberg catalog settings the blog's scripts use:

```text
spark.sql.extensions                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.ice                org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice.type           hive
spark.sql.catalog.ice.uri            thrift://hive-metastore:9083
spark.sql.catalog.ice.warehouse      s3a://warehouse/
```

**All three formats in one session** need one arrangement: Delta owns `spark_catalog`
(`org.apache.spark.sql.delta.catalog.DeltaCatalog`), Iceberg uses its own named catalog,
and Hudi writes by path. List Delta's extension **before** Iceberg's in
`spark.sql.extensions`: in the other order, Delta's SQL `UPDATE` fails with
`scala.MatchError: DeltaTableV2(...)`. `lakehouse_check.py` shows the full configuration.

Jupyter Lab (`all` profile) runs from the same Spark image at http://localhost:8888,
with the same jars and environment.

## Use Kafka (all profile)

The `all` profile runs a Kafka broker with ZooKeeper, Schema Registry, the REST Proxy,
Kafka Connect and Kafka UI. Inside the network the broker is `kafka:29092`; from your
machine it is `localhost:9092`.

**Produce and consume from the command line:**

```sh
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic orders --partitions 1 --replication-factor 1
printf '1001,PLACED\n1002,PLACED\n1003,SHIPPED\n' | \
  docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic orders
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic orders --from-beginning --max-messages 3
```

**Read a topic from Spark.** The Kafka source and its dependencies
(`spark-sql-kafka-0-10`, `spark-token-provider-kafka-0-10`, `kafka-clients` and
`commons-pool2`) are already in the Spark image, so no `--jars` are needed:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kafka-read").getOrCreate()
df = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "orders")
      .option("startingOffsets", "earliest")
      .load())
df.selectExpr("CAST(value AS STRING) AS value").show(truncate=False)
spark.stop()
```

**Change data capture.** Kafka Connect ships the Debezium **Postgres** connector (there
is no MySQL connector in this image). Its REST API is at http://localhost:8083;
`curl -s localhost:8083/connector-plugins` lists what is installed. Connect scans every
plugin jar before it opens that port, which takes around two minutes after `start`.

Kafka UI at http://localhost:9082 shows topics, consumer groups and connectors.

## Choosing a Spark line

`SPARK_VERSION` picks one of two Spark images; everything else follows from it.

| `SPARK_VERSION` | Image | Scala | Hadoop | Hudi | Iceberg | Delta |
| --------------- | ----- | ----- | ------ | ---- | ------- | ----- |
| `3.5.9` (default) | `ranga-spark` | 2.12 | 3.3.4 | 1.1.1 | 1.11.0 | 3.3.2 |
| `4.1.3` | `ranga-spark4` | 2.13 | 3.4.2 | 1.2.0 | 1.11.0 | 4.1.0 |

Both run on Java 17, and everything in this README was tested on both.

```sh
SPARK_VERSION=4.1.3 sh run_datalake.sh restart               # core profile
PROFILE=all SPARK_VERSION=4.1.3 sh run_datalake.sh restart   # all profile
```

**Pass the profile you started with.** `restart` stops and starts the profile it is
given, and `core` is the default. Run on an `all` stack without `PROFILE=all`, it
restarts the seven `core` containers and removes the other nine (Kafka, MySQL, Trino
and Jupyter) as orphans, without an error.

## Components and ports

Rows marked `all` exist only in the `all` profile.

| Component | URL / port | Profile | Notes |
| --------- | ---------- | ------- | ----- |
| Hive Metastore (thrift) | localhost:9083 | core | `thrift://hive-metastore:9083` inside the network |
| HiveServer2 | localhost:10000 | core | Web UI at http://localhost:10002 |
| Spark master UI | http://localhost:8080 | core | Submit to `spark://spark-master:7077` |
| Spark worker UI | http://localhost:18081 | core | Container port 8081 |
| Spark History Server | http://localhost:18080 | core | |
| Spark application UI | http://localhost:14040 | core | Container 4040-4042 mapped to 14040-14042 |
| MinIO API | http://localhost:9000 | core | `http://minio:9000` inside the network |
| MinIO console | http://localhost:9001 | core | |
| Postgres | localhost:5432 | core | |
| MySQL | localhost:3306 | all | |
| Trino | http://localhost:9084 | all | Container port 8080 |
| Jupyter Lab | http://localhost:8888 | all | Token disabled |
| ZooKeeper | localhost:2181 | all | Runs from the `ranga-kafka` image |
| Kafka broker | localhost:9092 | all | `kafka:29092` inside the network |
| Kafka JMX | localhost:9101 | all | |
| Schema Registry | http://localhost:8081 | all | |
| Kafka REST Proxy | http://localhost:8082 | all | |
| Kafka Connect REST | http://localhost:8083 | all | Debezium Postgres connector |
| Kafka UI | http://localhost:9082 | all | |

The Spark worker UI and Trino are remapped to host ports 18081 and 9084 so they do not
collide with the Schema Registry on 8081 and the Spark master on 8080.

## Credentials and configuration

There are no `.env` files. Everything is set in the compose files, and the defaults are
local demo values:

| What | Value | Where |
| ---- | ----- | ----- |
| MinIO (S3) access key / secret | `admin` / `password` | `environment:` of `spark-master`, `spark-worker`, `mc` (and `jupyter-notebook`) |
| S3 endpoint and region | `http://minio:9000`, `us-east-1`, path-style access | same services |
| Buckets | `warehouse`, `datalake` | created by the `mc` sidecar |
| Postgres | `postgres` / `postgres` | `postgres` service |
| MySQL | `admin` / `password` | `mysql` service (`all`) |

The same credentials are baked into the images' `core-site.xml` and Trino catalog files,
so change them only as a set. Do not expose these ports beyond your machine.

The start script reads three optional variables:

| Variable | Default | Effect |
| -------- | ------- | ------ |
| `PROFILE` | `core` | `all` adds Kafka, MySQL, Trino and Jupyter |
| `SPARK_VERSION` | 3.5.x | `4.1.3` switches to the Spark 4.1 image |
| `PLATFORM` | `linux/amd64` | The platform every service runs as. Leave it: the images are amd64 only |

`IMAGE_VERSION` (default `1.0.0`) selects the image tag, and is read by the compose files.

## Day-to-day commands

On the `all` profile, prefix every command with `PROFILE=all`.

| Task | Command |
| ---- | ------- |
| Start | `sh run_datalake.sh start` |
| Stop, keeping data | `sh run_datalake.sh stop` |
| Restart | `sh run_datalake.sh restart` |
| Status | `sh run_datalake.sh status` |
| Follow logs | `sh run_datalake.sh logs spark-master` |
| Check the compose file | `sh run_datalake.sh validate` |
| Shell into a service | `docker exec -it spark-master bash` |

`stop` keeps data. The MinIO object store, the Postgres data directory and the logs live
under `data/` and `logs/`, next to the compose files. For a clean slate:

```sh
sh run_datalake.sh stop
rm -rf data logs
sh run_datalake.sh start
```

To empty the warehouse without a full reset:

```sh
docker exec mc /usr/bin/mc rm --force --recursive minio/warehouse/
```

## Troubleshooting

| Symptom | Cause and fix |
| ------- | ------------- |
| `no matching manifest for linux/arm64` | `PLATFORM` was set to `linux/arm64`. Unset it; the images are amd64 and run under emulation on Apple Silicon |
| A Spark job submitted to the cluster never starts | No worker is registered. Check `aliveworkers` as in [Verify the stack](#verify-the-stack) and restart `spark-worker` |
| Several unrelated services fail at once | Docker's disk is full. `docker system df`, then `docker builder prune` or `docker image prune` |
| Health checks take minutes, a JVM gets killed | Too little memory for Docker. Give it 4 GB for `core`, 10 GB for `all` |
| A Spark job stops with exit code 137 and no error | The kernel killed the driver for memory. Pass `--driver-memory 1g`, or give Docker more memory |
| Every S3 write fails and the bucket list is empty | The `mc` sidecar did not finish. `sh run_datalake.sh logs mc` |
| Hudi's first write to a new table takes about 20 seconds | Expected: it bootstraps the metadata table. Later writes are faster |
| `kafka-connect` is not healthy for the first two minutes | Expected: Connect scans every plugin jar before it binds 8083 |
| `kafka-connect` restarts over and over (`all` profile) | Docker is short of memory and the kernel kills Connect part-way through its plugin scan. Give Docker 10 GB |
| `Multiple sources found for hudi` | Only the `hudi-spark*-bundle_*.jar` from `$HUDI_HOME` belongs on `--jars`, not every jar there |
| A script run on the host reports an old Spark version | Run Spark scripts inside `spark-master` with `docker exec`, not with a Spark installed on your machine |
| Kafka, MySQL, Trino and Jupyter are gone after a `restart` | The `all` stack was restarted without `PROFILE=all`, which removes them as orphans. Start again with `PROFILE=all` |
| `hive-metastore` logs `Connection to postgres:5432 refused` and `Failed to get schema version` on the first start | Postgres runs its init SQL on a socket-only server and opens TCP a few seconds later; the metastore retries and then initialises its schema. Expected once, on an empty `data/` |

## Warnings you can ignore

Every one of these appeared in a clean, passing run on both Spark lines:

| Where | Message | Why it is harmless |
| ----- | ------- | ------------------ |
| Spark jobs | `NativeCodeLoader: Unable to load native-hadoop library` | Hadoop falls back to its Java implementation |
| Spark jobs with Hudi | `Unable to get Instrumentation` and `Unable to attach Serviceability Agent` | Hudi's object-size estimator cannot attach to the JVM and uses estimates instead |
| Spark 4.1 jobs | `SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"` | One library on the classpath finds no logging backend and stays silent; Spark's own logging is unaffected |
| Spark 4.1 jobs, Trino | `Using incubator modules: jdk.incubator.vector` | The JVM's vector API is switched on |
| Spark reading Kafka, Schema Registry | `These configurations '[...]' were supplied but are not used yet` | A client received settings meant for a different client type |
| Postgres, first start | `relation "VERSION" does not exist` | The metastore checks for its schema before creating it |
| Kafka, ZooKeeper, first start | `No meta.properties file`, `running in standalone mode` | A new single-node broker and ZooKeeper |
| Kafka Connect, Schema Registry | Jersey `contains empty path annotation` / `does not implement any provider interfaces` | REST framework notices at startup |
| Trino, during startup | `Error fetching memory info ... returned status 503` | The coordinator polls itself before it has finished starting |
| MinIO | `Host local has more than 0 drives of set` | Single-drive mode, as expected for a local stack |

## Repository layout

```text
run_datalake.sh          start | stop | restart | status | logs | validate
docker-compose.yml       core profile
docker-compose_all.yml   all profile: core plus MySQL, Trino and Jupyter
db_scripts/              Postgres and MySQL init SQL, mounted into the databases
lakehouse_check.py       writes, updates and reads one table in each format
data/, logs/             created on first start; ignored by git
```

The images are built in a separate, private repository and published as
`rangareddy1988/ranga-*:1.0.0`. This repo changes only when those images or their
compose wiring change.
