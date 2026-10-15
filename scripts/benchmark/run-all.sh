#!/usr/bin/env bash
# Unattended TPC-DS campaign: stock Spark vs TabbyDB, on Hive and Iceberg, over ONE shared copy of the data.
#
# Usage:  ./run-all.sh [bench.env] <step>
#   gendata   generate unpartitioned, date-sorted Parquet           (01-gendata.scala)
#   tables    Hive external tables + Iceberg add_files registration  (02 + 03)
#   bench     ITERATIONS x 4 combinations, alternating order, resumable (04)
#   report    medians per query, compile vs execution, CSV           (05)
#   all       gendata -> tables -> bench -> report
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
  echo ">>> [$engine/$format] $(basename "$script")  log: $LOG_DIR/$log.log"
  # shellcheck disable=SC2086
  "$home/bin/spark-shell" --master "$MASTER" --jars "$jars" \
    $COMMON_CONF $EXTRA_SPARK_CONF \
    "${HIVE_CONF[@]}" "${fmt_conf[@]}" "${COMMON_BENCH_CONF[@]}" "$@" \
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
        --conf "spark.bench.queries=${QUERIES:-}"
    done
  done
}

step_report() {
  shell stock none "$HERE/05-report.scala" "05-report-${RUN_ID}"
}

case "$STEP" in
  gendata) step_gendata ;;
  tables)  step_tables ;;
  bench)   step_bench ;;
  report)  step_report ;;
  all)     step_gendata; step_tables; step_bench; step_report ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
echo ">>> step '$STEP' finished"
