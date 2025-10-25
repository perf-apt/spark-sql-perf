import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

val createExternalHiveTables = true
// Note: Declare "sqlContext" for Spark 2.x version
val useHive = true
// val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val sqlContext = if (useHive) {

  SparkSession .builder() .appName("SparkSessionExample").enableHiveSupport().getOrCreate().sqlContext
} else {
  // new org.apache.spark.sql.SQLContext(sc)
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
// val rootDir = "/Users/ashahid/workspace/tpcds-benchmark/generated_data/" // root directory of location to create data in.
val rootDir = "hdfs://ec2-3-149-12-129.us-east-2.compute.amazonaws.com:9000//tpcds/data"
val databaseName = "default" // name of database to create.
val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
val format = "parquet" // valid spark format like parquet "parquet".
// Run:
val tables = new TPCDSTables(sqlContext,
  dsdgenDir = "/mnt/data1/tpcds-benchmark/tpcds-kit/tools/", // location of dsdgen
  scaleFactor = scaleFactor,
  useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
  useStringForDate = false) // true to replace DateType with StringType


// Create the specified database
// sqlContext.sql(s"create database $databaseName")
/*

tables.genData(
  location = rootDir,
  format = format,
  overwrite = true, // overwrite the data that is already there
  partitionTables = false, // create the partitioned fact tables
  clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
  filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
  tableFilter = "", // "" means generate all tables
  numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
*/
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
val numSplits : Option[Int] = None
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.

if (createExternalHiveTables) {
  tables.createExternalTables(rootDir, "parquet", s"$databaseName", overwrite = true, discoverPartitions = false)
} else {
  tables.createInternalTables(rootDir, "parquet", s"$databaseName", overwrite = true,
    discoverPartitions = false, numSplits = numSplits, sortOnCol = true)
}

// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, format)

// For CBO only, gather statistics on all columns:
tables.analyzeTables(databaseName, analyzeColumns = false)

