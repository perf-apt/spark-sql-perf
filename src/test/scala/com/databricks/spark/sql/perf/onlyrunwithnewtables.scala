

import com.databricks.spark.sql.perf.mllib.MLBenchmarks.sqlContext.tables
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

val createExternalHiveTables = false
// Note: Declare "sqlContext" for Spark 2.x version
val useHive = true
// val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val sqlContext = if (useHive) {
  SparkSession .builder().appName("SparkSessionExample").enableHiveSupport().getOrCreate().sqlContext
} else {
  throw new RuntimeException(("use hive is false"))
}
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
val rootDir = "/opt/tpcds-benchmark/tpcds-data"  // root directory of location to create data in.
val databaseName = "default" // name of database to create.
val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
val format = "parquet"


import com.databricks.spark.sql.perf.tpcds.TPCDS
val tables = new TPCDSTables(sqlContext,
  dsdgenDir = "/data/tpcds-benchmark/tpcds-kit/tools/", // location of dsdgen
  scaleFactor = scaleFactor,
  useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
  useStringForDate = false) // true to replace DateType with StringType

val numSplits : Option[Int] = None
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.



if (createExternalHiveTables) {
  tables.createExternalTables(rootDir, "parquet", s"$databaseName", overwrite = true, discoverPartitions = false)
} else {
  tables.createInternalTables(rootDir, "parquet", s"$databaseName", overwrite = true,
    discoverPartitions = false, numSplits = numSplits)
}


val tpcds = new TPCDS(sqlContext = sqlContext)
// Set:

sqlContext.sql(s"use $databaseName")

if (!useHive) {
  sqlContext.sql(s"use catalog spark_catalog")
}
val resultLocation = "/data/tpcds-benchmark/results/wf"// place to write results
val iterations = 3 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24 * 60 * 60 // timeout, in seconds.
// Run:
val experiment = tpcds.runExperiment(
  queries,
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)