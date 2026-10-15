// 01-gendata.scala — generate unpartitioned, date-sorted TPC-DS Parquet data (once).
//
// Resumable: a table whose output dir already has a _SUCCESS marker is skipped,
// so a preempted run can simply be restarted. Pass spark.bench.force=true to regenerate all.
//
// Params (--conf spark.bench.<name>=...):
//   dataDir              root output dir, e.g. file:///data/tpcds/sf100  or  gs://bucket/tpcds/sf1000
//   scaleFactor          e.g. 10, 100, 1000
//   dsdgenDir            tpcds-kit tools dir; must exist at the same path on EVERY worker
//   genPartitions        dsdgen tasks (default 100; use ~2-4x total cores at SF=1000)
//   useDoubleForDecimal  default true (matches your previous runs; mention it in the methodology)
//   tableFilter          optional comma-separated list of tables
//   force                default false

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.hadoop.fs.Path

def p(k: String, d: String = null): String =
  spark.conf.getOption(s"spark.bench.$k")
    .orElse(spark.sparkContext.getConf.getOption(s"spark.bench.$k"))
    .orElse(Option(d))
    .getOrElse(throw new IllegalArgumentException(s"Missing --conf spark.bench.$k"))

try {
  val dataDir   = p("dataDir").stripSuffix("/")
  val scale     = p("scaleFactor")
  val dsdgenDir = p("dsdgenDir")
  val nParts    = p("genPartitions", "100").toInt
  val useDouble = p("useDoubleForDecimal", "true").toBoolean
  val filter    = p("tableFilter", "")
  val force     = p("force", "false").toBoolean

  val tables = new TPCDSTables(spark.sqlContext, dsdgenDir = dsdgenDir, scaleFactor = scale,
    useDoubleForDecimal = useDouble, useStringForDate = false)
  val names: Seq[String] =
    if (filter.isEmpty) tables.tables.map(_.name)
    else filter.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  val fs = new Path(dataDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  def isDone(t: String) = fs.exists(new Path(s"$dataDir/$t/_SUCCESS"))

  println(s"[gendata] SF=$scale -> $dataDir  tables=${names.size}  dsdgen tasks=$nParts")
  names.foreach { t =>
    if (!force && isDone(t)) {
      println(s"[gendata] $t: already generated, skipping")
    } else {
      val t0 = System.nanoTime
      tables.genData(
        location = dataDir,
        format = "parquet",
        overwrite = true,                    // partial output from a preempted run is replaced
        partitionTables = false,             // unpartitioned
        clusterByPartitionColumns = false,
        filterOutNullPartitionValues = false,
        tableFilter = t,
        numPartitions = nParts,
        sortOnCol = true)                    // sortWithinPartitions on the date key (fact tables)
      println(f"[gendata] $t: done in ${(System.nanoTime - t0) / 1e9}%.0f s")
    }
  }

  // Row counts, used later to validate the Hive and Iceberg tables.
  import spark.implicits._
  val counts = names.map(t => (t, spark.read.parquet(s"$dataDir/$t").count()))
  counts.toDF("table", "rows").coalesce(1).write.mode("overwrite").json(s"$dataDir/_rowcounts")
  counts.foreach { case (t, c) => println(f"[gendata] $t%-24s $c%,15d rows") }
  println("[gendata] OK")
  println("BENCH_STEP_OK")
  System.exit(0)
} catch {
  case e: Throwable => e.printStackTrace(); System.exit(1)
}
