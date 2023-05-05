

import com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext.tables
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

// Note: Declare "sqlContext" for Spark 2.x version
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
/*
sqlContext.setConf("spark.sql.extensions",
  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
sqlContext.setConf("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
sqlContext.setConf("spark.sql.catalog.spark_catalog.type", "hadoop")
sqlContext.setConf("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg_warehouse")
*/

// Set:
// Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
// Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
val rootDir = "/tmp/tpcds" // root directory of location to create data in.
val stagingDir = "/tmp/tpcds"
val databaseName = "default" // name of database to create.
val scaleFactor = "2" // scaleFactor defines the size of the dataset to generate (in GB).
val format = "parquet"


import com.databricks.spark.sql.perf.tpcds.TPCDS
val tables = new TPCDSTables(sqlContext,
  dsdgenDir = "/Users/ashahid/workspace/spark-tpcds-perf/tpcds-kit/tools", // location of dsdgen
  scaleFactor = scaleFactor,
  useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
  useStringForDate = false) //
tables.createExternalTables(stagingDir, rootDir, "parquet", s"$databaseName", overwrite = true, discoverPartitions = false)


val tpcds = new TPCDS(sqlContext = sqlContext)
// Set:

sqlContext.sql(s"use $databaseName")
sqlContext.sql(s"use catalog spark_catalog")
val resultLocation = "/tmp/tpcds_results" // place to write results
val iterations = 1 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24 * 60 * 60 // timeout, in seconds.
// Run:
val experiment = tpcds.runExperiment(
  queries,
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)