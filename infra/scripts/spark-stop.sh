#!/usr/bin/env bash
# Stop the Spark Thrift server. Idempotent: exits 0 if not running.
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/tmp/claude-0/-home-user-jean-humann/917b2dd2-1f49-560f-8a42-71e5677bbc01/scratchpad/engines/spark}"
RUN_DIR="${SPARK_RUN_DIR:-$SPARK_HOME/run}"

export SPARK_LOG_DIR="$RUN_DIR/logs"
export SPARK_PID_DIR="$RUN_DIR/pids"

if [ -x "$SPARK_HOME/sbin/stop-thriftserver.sh" ]; then
  "$SPARK_HOME/sbin/stop-thriftserver.sh" >/dev/null 2>&1 || true
fi

# Belt and braces: kill any leftover thrift server JVM, then verify.
pkill -f 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' 2>/dev/null || true
for i in $(seq 1 30); do
  pgrep -f 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' >/dev/null || { echo "Spark Thrift server stopped"; exit 0; }
  sleep 1
done
pkill -9 -f 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' 2>/dev/null || true
sleep 1
if pgrep -f 'org.apache.spark.sql.hive.thriftserver.HiveThriftServer2' >/dev/null; then
  echo "ERROR: could not stop Spark Thrift server" >&2
  exit 1
fi
echo "Spark Thrift server stopped (forced)"
