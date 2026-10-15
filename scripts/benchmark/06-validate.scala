// 06-validate.scala — capture each query's RESULT for one (engine, format), for correctness checking.
//
// Runs separately from the timed benchmark (its own spark-shell, after 04), so it has no effect
// on the performance numbers. Each query runs once; its rows are written to
//   <resultDir>/validate/<runId>/<engine>-<format>/<query>.tsv
// Resumable: queries that already have a .tsv are skipped.
//
// Encoding per cell:  N = null,  D:<double> = float/double (compared with tolerance),  S:<text> = exact
//
// Params (--conf spark.bench.<name>=...): engine, format, db, resultDir, runId, queries (optional)

import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.hadoop.fs.Path

def p(k: String, d: String = null): String =
  spark.conf.getOption(s"spark.bench.$k")
    .orElse(spark.sparkContext.getConf.getOption(s"spark.bench.$k"))
    .orElse(Option(d))
    .getOrElse(throw new IllegalArgumentException(s"Missing --conf spark.bench.$k"))

def enc(v: Any): String = v match {
  case null => "N"
  case d: Double if d.isNaN || d.isInfinite => "S:" + d.toString
  case d: Double => "D:" + java.lang.Double.toString(d)
  case f: Float  => "D:" + java.lang.Double.toString(f.toDouble)
  case b: java.math.BigDecimal => "S:" + b.stripTrailingZeros.toPlainString
  case x => "S:" + x.toString.replace('\t', ' ').replace('\n', ' ')
}

try {
  val engine    = p("engine")
  val format    = p("format")
  val db        = p("db")
  val resultDir = p("resultDir").stripSuffix("/")
  val runId     = p("runId")
  val only      = p("queries", "").split(",").map(_.trim).filter(_.nonEmpty).toSet
  val excluded  = p("excludeQueries", "").split(",").map(_.trim).filter(_.nonEmpty).toSet

  spark.sql(s"USE $db")
  val tpcds = new TPCDS(spark.sqlContext)

  val outDir = s"$resultDir/validate/$runId/$engine-$format"
  val fs = new Path(outDir).getFileSystem(spark.sparkContext.hadoopConfiguration)
  fs.mkdirs(new Path(outDir))

  val todo = tpcds.tpcds2_4Queries
    .filter(q => only.isEmpty || only.contains(q.name))
    .filterNot(q => excluded.contains(q.name))
    .filterNot(q => fs.exists(new Path(s"$outDir/${q.name}.tsv")))
  println(s"[validate] $engine/$format db=$db -> $outDir : ${todo.size} queries to capture")

  todo.zipWithIndex.foreach { case (q, i) =>
    val t0 = System.nanoTime
    val content =
      try {
        val rows = q.newDataFrame().collect()
        (s"#rows=${rows.length}" +: rows.toSeq.map(r => r.toSeq.map(enc).mkString("\t"))).mkString("\n")
      } catch {
        case e: Throwable =>
          s"#error=${e.getClass.getName}: ${String.valueOf(e.getMessage).take(500).replace('\n', ' ')}"
      }
    // write to .tmp then rename, so an interrupted run never leaves a half-written result
    val tmp = new Path(s"$outDir/${q.name}.tsv.tmp")
    val out = fs.create(tmp, true)
    out.write(content.getBytes("UTF-8")); out.close()
    fs.rename(tmp, new Path(s"$outDir/${q.name}.tsv"))
    println(f"[validate] ${i + 1}%3d/${todo.size} ${q.name}%-12s ${content.takeWhile(_ != '\n')}%-30s ${(System.nanoTime - t0) / 1e9}%7.1f s")
  }
  println(s"[validate] OK $engine/$format")
  println("BENCH_STEP_OK")
  System.exit(0)
} catch {
  case e: Throwable => e.printStackTrace(); System.exit(1)
}
