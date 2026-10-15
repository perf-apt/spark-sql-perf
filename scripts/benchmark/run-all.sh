#!/usr/bin/env bash
# Unattended TPC-DS campaign: stock Spark vs TabbyDB, on Hive and Iceberg, over ONE shared copy of the data.
#
# Usage:  ./run-all.sh [bench.env] <step>
#   gendata   generate unpartitioned, date-sorted Parquet           (01-gendata.scala)
#   tables    Hive external tables + Iceberg add_files registration  (02 + 03)
#   bench     ITERATIONS x 4 combinations, alternating order, resumable (04)
#   report    medians per query, compile vs execution, CSV           (05)
#   all       gendata -> tables -> bench -> report
#   confdiff  show differences between the two distributions' conf/ folders
#
# Every step is safe to re-run: finished tables / finished queries are skipped.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$HERE/bench.env"
if [[ $# -ge 2 ]]; then ENV_FILE="$1"; shift; fi
STEP="${1:-}"
[[ -f "$ENV_FILE" ]] || { echo "Missing $ENV_FILE (copy bench.env.example)"; exit 1; }
# shellcheck disable=SC1090
source "$ENV_FILE"
mkdir -p "$LOG_DIR" "$METASTORE_DIR"

# Optional settings (older bench.env files may not have them).
COMMON_SPARK_CONF_DIR="${COMMON_SPARK_CONF_DIR:-}"
MANAGE_CLUSTER="${MANAGE_CLUSTER:-true}"
MASTER_WEBUI="${MASTER_WEBUI:-http://localhost:8080}"

# Conf folder (spark-env.sh, spark-defaults.conf, workers) for a given SPARK_HOME.
conf_dir_for() { if [[ -n "$COMMON_SPARK_CONF_DIR" ]]; then echo "$COMMON_SPARK_CONF_DIR"; else echo "$1/conf"; fi; }

# ── Standalone cluster management ────────────────────────────────────────────
# In standalone mode executors run from the installation that STARTED THE WORKERS, not from the
# driver's SPARK_HOME. So before an engine's runs, the master + workers are restarted from that
# engine's SPARK_HOME (with the same conf folder). Skipped for local[*] or MANAGE_CLUSTER=false.
CLUSTER_ENGINE=""   # engine the cluster was last started with by this script
ensure_cluster() {
  local engine="$1" home="$2"
  [[ "$MASTER" == spark://* && "$MANAGE_CLUSTER" == "true" ]] || return 0
  [[ "$CLUSTER_ENGINE" == "$engine" ]] && return 0
  local conf; conf="$(conf_dir_for "$home")"
  local expected=1
  [[ -f "$conf/workers" ]] && expected=$(grep -cvE '^\s*(#|$)' "$conf/workers" || true)
  echo ">>> (re)starting standalone cluster from $home  conf=$conf  workers=$expected"
  SPARK_HOME="$home" SPARK_CONF_DIR="$conf" "$home/sbin/stop-all.sh"  >/dev/null 2>&1 || true
  sleep 3
  SPARK_HOME="$home" SPARK_CONF_DIR="$conf" "$home/sbin/start-all.sh" >"$LOG_DIR/cluster-start-$engine.log" 2>&1
  local i alive=0
  for ((i = 0; i < 60; i++)); do
    alive=$(curl -s "$MASTER_WEBUI/json/" | grep -oE '"aliveworkers" *: *[0-9]+' | grep -oE '[0-9]+$' || echo 0)
    [[ "${alive:-0}" -ge "$expected" ]] && break
    sleep 2
  done
  if [[ "${alive:-0}" -lt "$expected" ]]; then
    echo "!!! only ${alive:-0}/$expected workers registered at $MASTER_WEBUI — see $LOG_DIR/cluster-start-$engine.log and $home/logs"
    exit 1
  fi
  echo ">>> cluster up: $alive worker(s), running $engine"
  CLUSTER_ENGINE="$engine"
}

join_jars() { local IFS=,; echo "$*" | sed 's/,,*/,/g; s/^,//; s/,$//'; }

# Hive metastore shared by both distributions (embedded Derby => run one JVM at a time).
HIVE_CONF=(
  --conf spark.sql.catalogImplementation=hive
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=${METASTORE_DIR}/metastore_db;create=true"
  --conf "spark.sql.warehouse.dir=${METASTORE_DIR}/warehouse"
)
# Same Iceberg setup you use today: SparkSessionCatalog, hadoop type.
ICEBERG_CONF=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
  --conf spark.sql.catalog.spark_catalog.type=hadoop
  --conf "spark.sql.catalog.spark_catalog.warehouse=${ICEBERG_WAREHOUSE}"
)
COMMON_BENCH_CONF=(
  --conf "spark.bench.dataDir=${DATA_DIR}"
  --conf "spark.bench.scaleFactor=${SCALE}"
  --conf "spark.bench.useDoubleForDecimal=${USE_DOUBLE_FOR_DECIMAL}"
  --conf "spark.bench.resultDir=${RESULT_DIR}"
  --conf "spark.bench.runId=${RUN_ID}"
)

# shell <engine stock|tabbydb> <format hive|iceberg|none> <script> <log name> [extra --conf ...]
shell() {
  local engine="$1" format="$2" script="$3" log="$4"; shift 4
  local home jars
  if [[ "$engine" == "stock" ]]; then home="$STOCK_SPARK_HOME"; else home="$TABBY_SPARK_HOME"; fi
  jars="$PERF_JAR"
  local fmt_conf=()
  if [[ "$format" == "iceberg" ]]; then
    if [[ "$engine" == "stock" ]]; then jars="$(join_jars "$jars" "$ICEBERG_JAR_STOCK")"
    else jars="$(join_jars "$jars" "$ICEBERG_JAR_TABBY")"; fi
    fmt_conf=("${ICEBERG_CONF[@]}")
  fi
  jars="$(join_jars "$jars" "${EXTRA_JARS:-}")"
  echo ">>> [$engine/$format] $(basename "$script")  SPARK_HOME=$home  log: $LOG_DIR/$log.log"
  ensure_cluster "$engine" "$home"
  # Pin SPARK_HOME and SPARK_CONF_DIR. Otherwise a SPARK_HOME / SPARK_CONF_DIR exported in your
  # shell profile would silently take over (wrong engine or conf). The conf folder is
  # COMMON_SPARK_CONF_DIR if set (same spark-env.sh + spark-defaults.conf for both engines),
  # else the distribution's own conf/. Command-line values from bench.env override spark-defaults.conf.
  # shellcheck disable=SC2086
  SPARK_HOME="$home" SPARK_CONF_DIR="$(conf_dir_for "$home")" \
  "$home/bin/spark-shell" --master "$MASTER" --jars "$jars" \
    $COMMON_CONF $EXTRA_SPARK_CONF \
    "${HIVE_CONF[@]}" ${fmt_conf[@]+"${fmt_conf[@]}"} "${COMMON_BENCH_CONF[@]}" ${@+"$@"} \
    -i "$script" < /dev/null 2>&1 | tee "$LOG_DIR/$log.log"
  # spark-shell exit status is lost in the pipe; scripts print BENCH_STEP_OK only on success.
  grep -q "BENCH_STEP_OK" "$LOG_DIR/$log.log" || { echo "!!! $log failed — see $LOG_DIR/$log.log"; exit 1; }
}

step_gendata() {
  shell "$TABLES_ENGINE" none "$HERE/01-gendata.scala" "01-gendata-sf${SCALE}" \
    --conf "spark.bench.dsdgenDir=${DSDGEN_DIR}" \
    --conf "spark.bench.genPartitions=${GEN_PARTITIONS}"
}

step_tables() {
  shell "$TABLES_ENGINE" hive    "$HERE/02-hive-tables.scala"    "02-hive-tables-sf${SCALE}" \
    --conf "spark.bench.hiveDb=${HIVE_DB}" --conf "spark.bench.analyze=${ANALYZE_HIVE}"
  shell "$TABLES_ENGINE" iceberg "$HERE/03-iceberg-tables.scala" "03-iceberg-tables-sf${SCALE}" \
    --conf "spark.bench.icebergDb=${ICEBERG_DB}"
}

step_bench() {
  local combos=("stock hive" "tabbydb hive" "stock iceberg" "tabbydb iceberg")
  for ((it = 1; it <= ITERATIONS; it++)); do
    local order=("${combos[@]}")
    # Alternate who goes first on even iterations, so neither engine always runs "first".
    if (( it % 2 == 0 )); then order=("tabbydb hive" "stock hive" "tabbydb iceberg" "stock iceberg"); fi
    for c in "${order[@]}"; do
      read -r engine format <<<"$c"
      local db="$HIVE_DB"; [[ "$format" == "iceberg" ]] && db="spark_catalog.${ICEBERG_DB}"
      if [[ -n "${DROP_CACHES_CMD:-}" ]]; then echo ">>> dropping page caches"; bash -c "$DROP_CACHES_CMD" || true; fi
      shell "$engine" "$format" "$HERE/04-run-benchmark.scala" "04-${RUN_ID}-${engine}-${format}-iter${it}" \
        --conf "spark.bench.engine=${engine}" --conf "spark.bench.format=${format}" \
        --conf "spark.bench.db=${db}" --conf "spark.bench.iter=${it}" \
        --conf "spark.bench.queryTimeoutSec=${QUERY_TIMEOUT_SEC}" \
        --conf "spark.bench.minExecutors=${MIN_EXECUTORS:-0}" \
        --conf "spark.bench.queries=${QUERIES:-}"
    done
  done
}

step_report() {
  shell stock none "$HERE/05-report.scala" "05-report-${RUN_ID}"
}

# Show where the two distributions' conf folders differ. Anything listed here is a setting one
# engine runs with and the other doesn't — decide whether it belongs in bench.env (shared) instead.
step_confdiff() {
  local f
  if [[ -n "$COMMON_SPARK_CONF_DIR" ]]; then
    echo "COMMON_SPARK_CONF_DIR=$COMMON_SPARK_CONF_DIR is used by BOTH engines; the distributions' own conf/ folders are not used."
    return 0
  fi
  for f in spark-defaults.conf spark-env.sh hive-site.xml; do
    echo "===== conf/$f   (< stock   > tabbydb)"
    diff <(grep -vE '^\s*(#|$)' "$STOCK_SPARK_HOME/conf/$f" 2>/dev/null | sort) \
         <(grep -vE '^\s*(#|$)' "$TABBY_SPARK_HOME/conf/$f" 2>/dev/null | sort) && echo "(identical or absent in both)"
  done
  return 0
}

case "$STEP" in
  confdiff) step_confdiff ;;
  gendata) step_gendata ;;
  tables)  step_tables ;;
  bench)   step_bench ;;
  report)  step_report ;;
  all)     step_gendata; step_tables; step_bench; step_report ;;
  *) sed -n '2,13p' "$0"; exit 1 ;;
esac
echo ">>> step '$STEP' finished"
