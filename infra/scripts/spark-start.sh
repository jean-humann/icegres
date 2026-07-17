#!/usr/bin/env bash
# Start Spark Thrift JDBC/ODBC server (HiveServer2 protocol, port 10000)
# serving the shared Iceberg REST catalog (Lakekeeper) as catalog `lake`.
# Idempotent: exits 0 if already running.
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/tmp/claude-0/-home-user-jean-humann/917b2dd2-1f49-560f-8a42-71e5677bbc01/scratchpad/engines/spark}"
RUN_DIR="${SPARK_RUN_DIR:-$SPARK_HOME/run}"
PORT="${SPARK_THRIFT_PORT:-10000}"

if [ ! -x "$SPARK_HOME/sbin/start-thriftserver.sh" ]; then
  echo "ERROR: Spark not found at $SPARK_HOME (set SPARK_HOME)" >&2
  exit 1
fi

# Already running?
if pgrep -f 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' >/dev/null 2>&1; then
  echo "Spark Thrift server already running (port $PORT)"
  exit 0
fi

mkdir -p "$RUN_DIR"
cd "$RUN_DIR"   # derby metastore_db + derby.log land here, not in the repo

export SPARK_LOG_DIR="$RUN_DIR/logs"
export SPARK_PID_DIR="$RUN_DIR/pids"
export SPARK_LOCAL_IP=127.0.0.1

"$SPARK_HOME/sbin/start-thriftserver.sh" \
  --master 'local[3]' \
  --driver-memory 2g \
  --hiveconf hive.server2.thrift.port="$PORT" \
  --hiveconf hive.server2.thrift.bind.host=127.0.0.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=rest \
  --conf spark.sql.catalog.lake.uri=http://127.0.0.1:8181/catalog \
  --conf spark.sql.catalog.lake.warehouse=lakehouse \
  --conf spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lake.s3.endpoint=http://127.0.0.1:9000 \
  --conf spark.sql.catalog.lake.s3.path-style-access=true \
  --conf spark.sql.catalog.lake.s3.access-key-id=rustfsadmin \
  --conf spark.sql.catalog.lake.s3.secret-access-key=rustfssecret \
  --conf spark.sql.catalog.lake.client.region=us-east-1 \
  --conf spark.ui.enabled=false \
  --conf spark.sql.catalogImplementation=in-memory

# Wait until the thrift port answers (up to 120s)
for i in $(seq 1 120); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then
    exec 3>&- 3<&- || true
    echo "Spark Thrift server ready on 127.0.0.1:$PORT (catalog: lake)"
    exit 0
  fi
  sleep 1
done
echo "ERROR: Spark Thrift server did not open port $PORT within 120s; see $SPARK_LOG_DIR" >&2
exit 1
