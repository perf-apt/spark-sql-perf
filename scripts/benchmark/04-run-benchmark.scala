// 04-run-benchmark.scala — run the TPC-DS v2.4 queries for ONE (engine, format, iteration), resumably.
//
// spark-sql-perf only writes results when a whole experiment finishes, so a Spot preemption
// would lose the entire run. Here each query is its own experiment: its JSON result is written
// as soon as it finishes, and on restart queries already recorded for this
// (runId, engine, format, iter) are skipped.
//
// Params (--conf spark.bench.<name>=...):
//   engine        label, e.g. stock | tabbydb
//   format        label, e.g. hive | iceberg
//   db            database to USE, e.g. tpcds_hive  or  spark_catalog.tpcds_ice
//   resultDir     e.g. file:///data/tpcds/results  or  gs://bucket/tpcds/results
//   runId         groups one benchmark campaign, e.g. sf1000-2026-11
//   iter          iteration number (1, 2, 3 ...)
//   scaleFactor   recorded as a tag only
//   queryTimeoutSec  per-query timeout (default 7200)
//   queries       optional comma-separated query names for debugging, e.g. q1-v2.4,q72-v2.4

import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._

def p(k: String, d: String = null): String =
  spark.conf.getOption(s"spark.bench.$k")
    .orElse(spark.sparkContext.getConf.getOption(s"spark.bench.$k"))
    .orElse(Option(d))
    .getOrElse(throw new IllegalArgumentException(s"Missing --conf spark.bench.$k"))

try {
  val engine     = p("engine")
  val format     = p("format")
  val db         = p("db")
  val resultDir  = p("resultDir").stripSuffix("/")
  val runId      = p("runId")
  val iter       = p("iter")
  val scale      = p("scaleFactor", "")
  val timeoutSec = p("queryTimeoutSec", "7200").toInt
  val only       = p("queries", "").split(",").map(_.trim).filter(_.nonEmpty).toSet

  val rawDir = s"$resultDir/raw"
  val tags = Map("runId" -> runId, "engine" -> engine, "format" -> format,
    "iter" -> iter, "scale" -> scale)

  spark.sql(s"USE $db")
  val tpcds = new TPCDS(spark.sqlContext)

  // Queries already recorded for this (runId, engine, format, iter).
  import spark.implicits._
  val fs = new Path(rawDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  val done: Set[String] =
    if (!fs.exists(new Path(rawDir))) Set.empty
    else spark.read.json(rawDir)
      .where($"tags.runId" === runId && $"tags.engine" === engine &&
             $"tags.format" === format && $"tags.iter" === iter)
      .select(explode($"results.name")).as[String].collect().toSet

  val todo = tpcds.tpcds2_4Queries
    .filter(q => only.isEmpty || only.contains(q.name))
    .filterNot(q => done.contains(q.name))

  println(s"[bench] $engine/$format iter=$iter db=$db: ${done.size} already done, ${todo.size} to run")

  todo.zipWithIndex.foreach { case (q, i) =>
    val t0 = System.nanoTime
    val exp = tpcds.runExperiment(
      Seq(q),
      iterations = 1,
      tags = tags,
      timeout = timeoutSec * 1000L,   // per-query timeout, in ms
      resultLocation = rawDir,
      forkThread = true)
    exp.waitForFinish(timeoutSec + 600)
    println(f"[bench] ${i + 1}%3d/${todo.size} ${q.name}%-12s wall ${(System.nanoTime - t0) / 1e9}%8.1f s")
  }

  // Summary for this invocation.
  val r = spark.read.json(rawDir)
    .where($"tags.runId" === runId && $"tags.engine" === engine &&
           $"tags.format" === format && $"tags.iter" === iter)
    .select(explode($"results").as("r"))
  val compile = expr("coalesce(r.parsingTime,0)+coalesce(r.analysisTime,0)+coalesce(r.optimizationTime,0)+coalesce(r.planningTime,0)")
  r.agg(
      count(lit(1)).as("queries"),
      bround(sum(compile) / 1000, 1).as("compile_s"),
      bround(sum(coalesce($"r.executionTime", lit(0.0))) / 1000, 1).as("execution_s"),
      bround(sum(compile + coalesce($"r.executionTime", lit(0.0))) / 1000, 1).as("total_s"))
    .show(false)
  if (r.columns.nonEmpty && r.select("r.*").columns.contains("failure")) {
    r.where($"r.failure".isNotNull).select($"r.name", $"r.failure.message").show(200, false)
  }
  println(s"[bench] OK $engine/$format iter=$iter")
  println("BENCH_STEP_OK")
  System.exit(0)
} catch {
  case e: Throwable => e.printStackTrace(); System.exit(1)
}
