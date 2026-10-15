# Automated TPC-DS: stock Spark vs TabbyDB, Hive and Iceberg

One copy of unpartitioned, date-sorted Parquet data is shared by:

- **Hive** external tables (`tpcds_hive`)
- **Iceberg** tables (`tpcds_ice`), registered with `add_files`, so no data is copied

Every step is a script, so a campaign runs unattended and the same code runs on a laptop or on GCP.
Only `bench.env` changes between environments.

## Steps

```bash
cp bench.env.example bench.env   # edit paths, SCALE, resources
./run-all.sh gendata             # 01: generate data (skips tables that already have _SUCCESS)
./run-all.sh tables              # 02 + 03: Hive external + Iceberg add_files, both validated against row counts
./run-all.sh bench               # 04: ITERATIONS x 4 combinations, alternating order, resumable
./run-all.sh report              # 05: medians per query, compile vs execution, CSV
# or: ./run-all.sh all
```

## Resuming after an interruption (e.g. Spot preemption)

Re-run the same step with the same `RUN_ID`.

- **gendata** skips tables that are already complete.
- **bench** skips queries already recorded for that `(runId, engine, format, iter)`. Each query's result is written as soon as it finishes.

## Rules

- **Never** run `DROP TABLE ... PURGE`, `expire_snapshots` or `remove_orphan_files` on the Iceberg tables. They share their data files with the Hive tables.
- Keep `COMMON_CONF` and `EXTRA_SPARK_CONF` identical for the whole campaign. Both engines get the same settings.
- `USE_DOUBLE_FOR_DECIMAL=true` replaces DECIMAL with DOUBLE, as in earlier runs. State this in the published methodology.
- The embedded Derby metastore allows only one JVM at a time. The script runs everything sequentially.

## Note on compile-time measurement

`Benchmark.scala` builds each query's `executedPlan` once **before** the timed run (to display the plan, ~line 385).
The timed run then compiles the query a second time. Both engines are treated the same, but:

- total wall time per query includes two compilations
- the timed compile runs with a warm JIT and warm metadata caches

Consider making that pre-plan optional before the 1TB run.
