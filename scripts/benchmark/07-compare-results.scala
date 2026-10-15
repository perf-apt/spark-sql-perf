// 07-compare-results.scala — correctness report: do all 4 combinations return the same answers?
//
// Two checks, neither of which touches the timed runs:
//  1. Row counts from the TIMED runs (spark-sql-perf records numRows for every query, every iteration).
//     Free — no extra query execution.
//  2. Full results captured by 06-validate.scala, compared cell by cell against the reference
//     (stock-hive). Floating-point cells may differ in the last digits (different aggregation
//     order), so they match if within a relative tolerance (default 1e-6).
//
// Status per query:
//   MATCH             identical in output order (doubles within tolerance)
//   MATCH(unordered)  same rows, different order (only ties in ORDER BY can cause this)
//   DIFF ...          different results — review manually (TPC-DS has a few ORDER BY ... LIMIT
//                     queries with ties, where engines may legitimately return different rows)
//   ERROR ...         query failed in that combination
//
// Params: resultDir, runId, tolerance (optional, default 1e-6)
// Writes <resultDir>/validate/<runId>/_comparison.csv

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._

def p(k: String, d: String = null): String =
  spark.conf.getOption(s"spark.bench.$k")
    .orElse(spark.sparkContext.getConf.getOption(s"spark.bench.$k"))
    .orElse(Option(d))
    .getOrElse(throw new IllegalArgumentException(s"Missing --conf spark.bench.$k"))

try {
  import spark.implicits._
  val resultDir = p("resultDir").stripSuffix("/")
  val runId     = p("runId")
  val tol       = p("tolerance", "1e-6").toDouble
  val hconf     = spark.sparkContext.hadoopConfiguration

  // ── 1. Row counts from the timed runs ──────────────────────────────────────
  val rawDir = s"$resultDir/raw"
  if (new Path(rawDir).getFileSystem(hconf).exists(new Path(rawDir))) {
    val r = spark.read.json(rawDir).where($"tags.runId" === runId)
      .select($"tags.engine".as("engine"), $"tags.format".as("format"), $"tags.iter".as("iter"),
              explode($"results").as("r"))
      .select(concat_ws("-", $"engine", $"format").as("combo"), $"iter", $"r.name".as("query"),
              $"r.numRows".as("numRows"))
    val perQuery = r.groupBy("query").agg(
        countDistinct("numRows").as("distinct_counts"),
        collect_set(concat_ws(":", $"combo", $"numRows".cast("string"))).as("by_combo"))
    val bad = perQuery.where($"distinct_counts" > 1).orderBy("query")
    val nQ = perQuery.count(); val nBad = bad.count()
    println(s"[compare] timed-run row counts: $nQ queries, ${nQ - nBad} consistent across all combinations/iterations, $nBad differ")
    if (nBad > 0) bad.select($"query", concat_ws("  ", sort_array($"by_combo")).as("rows by combo")).show(200, false)
  } else println(s"[compare] no timed results at $rawDir — skipping row-count check")

  // ── 2. Full results from 06-validate ───────────────────────────────────────
  val base = new Path(s"$resultDir/validate/$runId")
  val fs = base.getFileSystem(hconf)
  require(fs.exists(base), s"No validation results at $base — run './run-all.sh validate' first")
  val combos = fs.listStatus(base).filter(_.isDirectory).map(_.getPath.getName).sorted.toSeq
  val ref = "stock-hive"
  require(combos.contains(ref), s"Reference combination $ref missing under $base (found: ${combos.mkString(", ")})")
  val others = combos.filterNot(_ == ref)

  def readResult(combo: String, q: String): Option[Either[String, Seq[Seq[String]]]] = {
    val f = new Path(base, s"$combo/$q.tsv")
    if (!fs.exists(f)) None
    else {
      val in = fs.open(f)
      val text = try new String(in.readAllBytes(), "UTF-8") finally in.close()
      val lines = text.split("\n", -1).toSeq
      if (lines.head.startsWith("#error=")) Some(Left(lines.head.stripPrefix("#error=")))
      else Some(Right(lines.tail.filter(_.nonEmpty).map(_.split("\t", -1).toSeq)))
    }
  }
  def cellEq(a: String, b: String): Boolean =
    if (a == b) true
    else if (a.startsWith("D:") && b.startsWith("D:")) {
      val (u, v) = (a.drop(2).toDouble, b.drop(2).toDouble)
      math.abs(u - v) <= tol * math.max(1.0, math.max(math.abs(u), math.abs(v)))
    } else false
  def rowsEq(x: Seq[Seq[String]], y: Seq[Seq[String]]): Boolean =
    x.length == y.length && x.zip(y).forall { case (r, s) => r.length == s.length && r.zip(s).forall { case (a, b) => cellEq(a, b) } }
  def sortKey(r: Seq[String]): String =
    r.map(c => if (c.startsWith("D:")) f"D:${c.drop(2).toDouble}%.4e" else c).mkString("\t")
  def status(a: Either[String, Seq[Seq[String]]], b: Either[String, Seq[Seq[String]]]): String = (a, b) match {
    case (Left(_), Left(_)) => "ERROR(both)"
    case (Left(e), _) => s"ERROR(reference only): ${e.take(80)}"
    case (_, Left(e)) => s"ERROR: ${e.take(80)}"
    case (Right(x), Right(y)) =>
      if (rowsEq(x, y)) "MATCH"
      else if (rowsEq(x.sortBy(sortKey), y.sortBy(sortKey))) "MATCH(unordered)"
      else if (x.length != y.length) s"DIFF rows ${x.length} vs ${y.length}"
      else {
        val i = x.zip(y).indexWhere { case (r, s) => !(r.length == s.length && r.zip(s).forall { case (a, b) => cellEq(a, b) }) }
        s"DIFF values (first at row ${i + 1})"
      }
  }

  val queries = fs.listStatus(new Path(base, ref)).map(_.getPath.getName)
    .filter(_.endsWith(".tsv")).map(_.stripSuffix(".tsv"))
    .sortBy(n => (n.takeWhile(_ != '-').drop(1).filter(_.isDigit).toIntOption.getOrElse(999), n)).toSeq

  val rows = queries.map { q =>
    val r = readResult(ref, q).get
    q +: others.map(o => readResult(o, q).map(status(r, _)).getOrElse("MISSING"))
  }

  val header = "query" +: others.map(o => s"$o vs $ref")
  val csv = (header +: rows).map(_.map(c => "\"" + c.replace("\"", "\"\"") + "\"").mkString(",")).mkString("\n")
  val outF = new Path(base, "_comparison.csv")
  val o = fs.create(outF, true); o.write(csv.getBytes("UTF-8")); o.close()

  println(s"[compare] full results: ${queries.size} queries, reference = $ref")
  others.zipWithIndex.foreach { case (c, i) =>
    val st = rows.map(_(i + 1))
    def n(pfx: String) = st.count(_.startsWith(pfx))
    println(f"[compare] $c%-18s MATCH ${n("MATCH")}%3d (unordered ${st.count(_ == "MATCH(unordered)")})   DIFF ${n("DIFF")}%3d   ERROR ${n("ERROR")}%3d   MISSING ${n("MISSING")}%3d")
  }
  val notMatching = rows.filter(_.tail.exists(s => !s.startsWith("MATCH")))
  if (notMatching.nonEmpty) {
    println("[compare] queries needing review:")
    notMatching.foreach(r => println("  " + r.head + "  " + others.zip(r.tail).filterNot(_._2.startsWith("MATCH")).map { case (c, s) => s"$c: $s" }.mkString(" | ")))
  }
  println(s"[compare] details: $outF")
  println("BENCH_STEP_OK")
  System.exit(0)
} catch {
  case e: Throwable => e.printStackTrace(); System.exit(1)
}
