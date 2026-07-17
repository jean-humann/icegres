#!/usr/bin/env bash
# A9 JDBC probe wrapper (bench/SPEC.md A9): compiles and runs
# bench/clients/A9JdbcProbe.java against a live icegres server using the
# stock PostgreSQL JDBC driver (pgjdbc), then summarizes PASS/XFAIL/FAIL.
#
# Usage:
#   bench/clients/a9_jdbc_probe.sh            # pgjdbc probe of 127.0.0.1:5439
#   ICEGRES_PROBE_PORT=5440 bench/clients/a9_jdbc_probe.sh
#   bench/clients/a9_jdbc_probe.sh --flight   # bonus lane: Arrow Flight SQL
#                                             # JDBC driver vs :50051
#
# Driver jars are cached in bench/clients/jars/ (gitignored); when absent
# they are downloaded once from repo1.maven.org. With no java/javac on PATH
# the script exits 3 ("skip") so callers can degrade gracefully.
#
# Exit codes: 0 = all green (fail=0), 1 = probe failures, 2 = infrastructure
# error (compile/download), 3 = java not available (skip).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="$SCRIPT_DIR/jars"
MODE=pg
[[ "${1:-}" == "--flight" ]] && MODE=flight

if ! command -v javac >/dev/null 2>&1 || ! command -v java >/dev/null 2>&1; then
  echo "A9 SKIP: java/javac not available (install a JDK, e.g. openjdk-21)" >&2
  exit 3
fi
mkdir -p "$JARS_DIR"

fetch_jar() { # path url
  local jar=$1 url=$2
  [[ -f "$jar" ]] && return 0
  echo "==> downloading $(basename "$jar") from repo1.maven.org" >&2
  if ! curl -fsSL -o "$jar.tmp" "$url"; then
    echo "A9 ERROR: failed to download $url" >&2
    rm -f "$jar.tmp"
    return 1
  fi
  mv "$jar.tmp" "$jar"
}

compile() { # jar source class
  local jar=$1 src=$2 cls=$3
  if [[ ! -f "$JARS_DIR/$cls" || "$src" -nt "$JARS_DIR/$cls" ]]; then
    if ! javac -cp "$jar" -d "$JARS_DIR" "$src" 2> >(grep -v '^Picked up' >&2); then
      echo "A9 ERROR: javac compilation of $(basename "$src") failed" >&2
      return 1
    fi
  fi
}

if [[ "$MODE" == pg ]]; then
  PGJDBC_VERSION="${A9_PGJDBC_VERSION:-42.7.12}"
  JAR="$JARS_DIR/postgresql-$PGJDBC_VERSION.jar"
  fetch_jar "$JAR" "https://repo1.maven.org/maven2/org/postgresql/postgresql/$PGJDBC_VERSION/postgresql-$PGJDBC_VERSION.jar" || exit 2
  compile "$JAR" "$SCRIPT_DIR/A9JdbcProbe.java" A9JdbcProbe.class || exit 2
  OUT=$(java -cp "$JAR:$JARS_DIR" A9JdbcProbe 2> >(grep -v '^Picked up' >&2))
  STATUS=$?
  RESULT_TAG='^A9 RESULT:'
else
  FLIGHT_VERSION="${A9_FLIGHT_JDBC_VERSION:-19.0.0}"
  JAR="$JARS_DIR/flight-sql-jdbc-driver-$FLIGHT_VERSION.jar"
  fetch_jar "$JAR" "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/$FLIGHT_VERSION/flight-sql-jdbc-driver-$FLIGHT_VERSION.jar" || exit 2
  compile "$JAR" "$SCRIPT_DIR/A9FlightJdbcProbe.java" A9FlightJdbcProbe.class || exit 2
  # --add-opens: Arrow's off-heap memory core needs java.nio internals on
  # JDK 17+ (standard requirement documented by Arrow Java).
  OUT=$(java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
        -cp "$JAR:$JARS_DIR" A9FlightJdbcProbe 2> >(grep -v '^Picked up' >&2))
  STATUS=$?
  RESULT_TAG='^A9FLIGHT RESULT:'
fi

echo "$OUT"
SUMMARY=$(echo "$OUT" | grep "$RESULT_TAG" | tail -n 1)
if [[ -z "$SUMMARY" ]]; then
  echo "A9 ERROR: probe produced no RESULT summary (crashed?)" >&2
  exit 2
fi
[[ "$STATUS" == 0 ]] && exit 0 || exit 1
