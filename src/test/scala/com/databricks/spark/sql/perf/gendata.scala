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
val format = "parquet" // valid spark format like parquet "parquet".
// Run:
val tables = new TPCDSTables(sqlContext,
  dsdgenDir = "/Users/ashahid/workspace/spark-tpcds-perf/tpcds-kit/tools", // location of dsdgen
  scaleFactor = scaleFactor,
  useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
  useStringForDate = false) // true to replace DateType with StringType


// Create the specified database
// sqlContext.sql(s"create database $databaseName")


tables.genData(
  location = stagingDir,
  format = format,
  overwrite = true, // overwrite the data that is already there
  partitionTables = false, // create the partitioned fact tables
  clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
  filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
  tableFilter = "", // "" means generate all tables
  numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.

// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(stagingDir, rootDir, "parquet", s"$databaseName", overwrite = true, discoverPartitions = false)

// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, format)

// For CBO only, gather statistics on all columns:
// tables.analyzeTables(databaseName, analyzeColumns = true)



import com.databricks.spark.sql.perf.tpcds.TPCDS



val tpcds = new TPCDS (sqlContext = sqlContext)
// Set:

sqlContext.sql(s"use $databaseName")
sqlContext.sql(s"use catalog spark_catalog")
val resultLocation = "/tmp/tpcds_results" // place to write results
val iterations = 1 // how many iterations of queries to run.
val queries = tpcds.tpcds2_4Queries // queries to run.
val timeout = 24*60*60 // timeout, in seconds.
// Run:
val experiment = tpcds.runExperiment(
  queries,
  iterations = iterations,
  resultLocation = resultLocation,
  forkThread = true)
experiment.waitForFinish(timeout)