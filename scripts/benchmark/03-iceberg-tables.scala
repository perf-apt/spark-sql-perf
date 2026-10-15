// 03-iceberg-tables.scala — register the SAME Parquet files as Iceberg tables, without copying.
//
// For each table: create an empty Iceberg table with the Parquet schema, then
// CALL <catalog>.system.add_files(...) pointing at the existing files.
// Only Iceberg metadata is written (into the Iceberg warehouse); data files are shared with Hive.
//
// !! The Iceberg tables do NOT own the data files. NEVER run DROP TABLE ... PURGE,
// !! expire_snapshots or remove_orphan_files on them — that can delete the shared Parquet files.
//
// Params (--conf spark.bench.<name>=...):
//   dataDir         same as 01-gendata
//   scaleFactor     same as 01-gendata
//   icebergCatalog  default spark_catalog (your current SparkSessionCatalog, type=hadoop setup)
//   icebergDb       default tpcds_ice

import com.databricks.spark.sql.perf.tpcds.TPCDSTables

def p(k: String, d: String = null): String =
  spark.conf.getOption(s"spark.bench.$k")
    .orElse(spark.sparkContext.getConf.getOption(s"spark.bench.$k"))
    .orElse(Option(d))
    .getOrElse(throw new IllegalArgumentException(s"Missing --conf spark.bench.$k"))

try {
  val dataDir = p("dataDir").stripSuffix("/")
  val scale   = p("scaleFactor")
  val cat     = p("icebergCatalog", "spark_catalog")
  val db      = p("icebergDb", "tpcds_ice")

  // Safety: the Iceberg warehouse must not overlap the shared data dir,
  // otherwise dropping/recreating a table could touch the source files.
  val warehouse = spark.sparkContext.getConf
    .getOption(s"spark.sql.catalog.$cat.warehouse")
    .getOrElse(throw new IllegalArgumentException(s"spark.sql.catalog.$cat.warehouse is not set"))
    .stripSuffix("/")
  require(!dataDir.startsWith(warehouse) && !warehouse.startsWith(dataDir),
    s"Iceberg warehouse ($warehouse) must not overlap dataDir ($dataDir)")

  val names = new TPCDSTables(spark.sqlContext, dsdgenDir = "", scaleFactor = scale)
    .tables.map(_.name)

  spark.sql(s"CREATE DATABASE IF NOT EXISTS $cat.$db")

  names.foreach { t =>
    val fq  = s"$cat.$db.$t"
    val src = s"$dataDir/$t"
    // Plain DROP (no PURGE) removes only this table's Iceberg metadata in the warehouse.
    spark.sql(s"DROP TABLE IF EXISTS $fq")
    val ddl = spark.read.parquet(src).schema.toDDL
    spark.sql(
      s"""CREATE TABLE $fq ($ddl) USING iceberg
         |TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')""".stripMargin)
    val t0 = System.nanoTime
    // add_files also sets a name mapping, needed because Spark-written Parquet has no Iceberg field IDs.
    val r = spark.sql(
      s"CALL $cat.system.add_files(table => '$cat.$db.$t', source_table => '`parquet`.`$src`')").collect()
    println(f"[iceberg] $t%-24s add_files -> ${r.mkString(",")}  (${(System.nanoTime - t0) / 1e9}%.0f s)")
  }

  // Validate: row counts must match 01-gendata, and a real column must read non-null
  // (if the name mapping were missing, columns would come back as NULL).
  val expected = spark.read.json(s"$dataDir/_rowcounts").collect()
    .map(r => r.getAs[String]("table") -> r.getAs[Long]("rows")).toMap
  var bad = 0
  expected.toSeq.sortBy(_._1).foreach { case (t, n) =>
    val fq = s"$cat.$db.$t"
    val firstCol = spark.table(fq).columns.head
    val got = spark.table(fq).count()
    val nnIce = spark.sql(s"SELECT count(`$firstCol`) FROM $fq").head.getLong(0)
    val nnPq  = spark.sql(s"SELECT count(`$firstCol`) FROM parquet.`$dataDir/$t`").head.getLong(0)
    val ok = got == n && nnIce == nnPq
    if (!ok) bad += 1
    println(f"[iceberg] $t%-24s rows $got%,15d/$n%,15d  non-null($firstCol) $nnIce%,d/$nnPq%,d  ${if (ok) "OK" else "MISMATCH"}")
  }
  if (bad > 0) throw new IllegalStateException(s"$bad table(s) failed validation")
  println(s"[iceberg] OK — ${expected.size} tables in $cat.$db share the files under $dataDir")
  println("BENCH_STEP_OK")
  System.exit(0)
} catch {
  case e: Throwable => e.printStackTrace(); System.exit(1)
}
