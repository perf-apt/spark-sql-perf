// 05-report.scala — summarise a campaign.
//
//  * summary            median per query per (engine, format), compile vs execution, totals
//  * side-by-side       one table per format (hive, iceberg): per query stock vs TabbyDB AVERAGE time
//                       across iterations, % improvement, speed-up, result row counts, result check.
//                       Written as plain CSV and printed tab-separated (paste straight into Excel).
//
// Params: resultDir, runId, excludeQueries
// Writes to <resultDir>/report/<runId>/: per_query/, summary/, hive_side_by_side.csv, iceberg_side_by_side.csv

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
  val out       = s"$resultDir/report/$runId"
  val excluded  = p("excludeQueries", "").split(",").map(_.trim).filter(_.nonEmpty).toSet

  val raw = spark.read.option("recursiveFileLookup", "true").json(s"$resultDir/raw").where($"tags.runId" === runId)
  if (excluded.nonEmpty) println(s"[report] excluded by EXCLUDE_QUERIES: ${excluded.toSeq.sorted.mkString(", ")}")
  val r = raw.select($"tags.engine".as("engine"), $"tags.format".as("format"),
                     $"tags.iter".as("iter"), explode($"results").as("r"))
  val hasFailure = r.select("r.*").columns.contains("failure")

  val rows = r.select(
    $"engine", $"format", $"iter", $"r.name".as("query"),
    (expr("coalesce(r.parsingTime,0)+coalesce(r.analysisTime,0)+coalesce(r.optimizationTime,0)+coalesce(r.planningTime,0)") / 1000).as("compile_s"),
    (coalesce($"r.executionTime", lit(0.0)) / 1000).as("exec_s"),
    (if (hasFailure) $"r.failure".isNotNull else lit(false)).as("failed"),
    $"r.numRows".cast("long").as("numRows"))
    .withColumn("total_s", $"compile_s" + $"exec_s")

  // Median across iterations (exact percentile; only 2-3 values per query).
  val perQuery = rows.groupBy("engine", "format", "query").agg(
      count(lit(1)).as("iters"),
      bround(expr("percentile(compile_s, 0.5)"), 2).as("compile_s"),
      bround(expr("percentile(exec_s, 0.5)"), 2).as("exec_s"),
      bround(expr("percentile(total_s, 0.5)"), 2).as("total_s"),
      sum($"failed".cast("int")).as("failures"))
    .where(!$"query".isin(excluded.toSeq: _*))
    .orderBy("format", "query", "engine")

  // Totals use only queries that ran successfully in EVERY combination, so all four totals
  // cover exactly the same work. Anything else is listed separately.
  val nCombos = perQuery.select("engine", "format").distinct().count()
  val okQueries = perQuery.groupBy("query")
    .agg(count(lit(1)).as("n"), sum("failures").as("f"))
    .where($"n" === nCombos && $"f" === 0).select("query")
  val leftOut = perQuery.join(okQueries, Seq("query"), "left_anti")
  if (!leftOut.isEmpty) {
    println("[report] NOT in totals (failed or missing in at least one combination):")
    leftOut.groupBy("query").agg(concat_ws("  ", sort_array(collect_list(
        concat($"engine", lit("-"), $"format", lit(" failures="), $"failures".cast("string"), lit("/"), $"iters".cast("string"))))).as("detail"))
      .orderBy("query").show(200, false)
  }

  val summary = perQuery.join(okQueries, Seq("query")).groupBy("engine", "format").agg(
      count(lit(1)).as("queries"),
      sum("failures").as("failures"),
      bround(sum("compile_s"), 1).as("compile_s"),
      bround(sum("exec_s"), 1).as("exec_s"),
      bround(sum("total_s"), 1).as("total_s"))
    .orderBy("format", "engine")

  perQuery.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$out/per_query")
  summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$out/summary")

  summary.show(false)

  // TabbyDB vs stock, per format (positive % = TabbyDB faster).
  val s = summary.collect().map(x => (x.getAs[String]("engine"), x.getAs[String]("format")) -> x.getAs[Double]("total_s")).toMap
  s.keys.map(_._2).toSeq.distinct.sorted.foreach { f =>
    (s.get(("stock", f)), s.get(("tabbydb", f))) match {
      case (Some(a), Some(b)) => println(f"[report] $f%-8s stock $a%10.1f s   tabbydb $b%10.1f s   improvement ${(a - b) / a * 100}%5.1f%%")
      case _ => println(s"[report] $f: missing stock or tabbydb results")
    }
  }
  // ── Side-by-side per format (Excel / charts): AVERAGE across iterations ───────────────────
  val okSet = okQueries.as[String].collect().toSet
  // (avg total seconds, iterations, failures, distinct result row counts)
  val stats: Map[(String, String, String), (Double, Long, Long, Seq[Long])] =
    rows.where(!$"query".isin(excluded.toSeq: _*))
      .groupBy("engine", "format", "query")
      .agg(avg("total_s").as("avg_s"), count(lit(1)).as("iters"),
           sum($"failed".cast("int")).cast("long").as("failures"),
           sort_array(collect_set($"numRows")).as("nrows"))
      .collect()
      .map(x => (x.getString(0), x.getString(1), x.getString(2)) ->
        (x.getDouble(3), x.getLong(4), x.getLong(5), x.getSeq[Long](6)))
      .toMap

  // Full-result check from 07-compare-results (if it has been run): TabbyDB vs the reference.
  def parseCsvLine(l: String): Seq[String] =
    l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).toSeq
      .map(c => c.trim.stripPrefix("\"").stripSuffix("\"").replace("\"\"", "\""))
  val hconf = spark.sparkContext.hadoopConfiguration
  val cmpPath = new org.apache.hadoop.fs.Path(s"$resultDir/validate/$runId/_comparison.csv")
  val cmpFs = cmpPath.getFileSystem(hconf)
  val cmp: Option[(Seq[String], Seq[Seq[String]])] =
    if (cmpFs.exists(cmpPath)) {
      val in = cmpFs.open(cmpPath)
      val txt = try new String(in.readAllBytes(), "UTF-8") finally in.close()
      val ls = txt.split("\n").toSeq.filter(_.trim.nonEmpty).map(parseCsvLine)
      Some((ls.head, ls.tail))
    } else None
  def resultCheck(fmt: String, q: String): String = cmp match {
    case None => "n/a (run validate)"
    case Some((hdr, body)) =>
      val idx = hdr.zipWithIndex.collect { case (h, i) if h.startsWith(s"tabbydb-$fmt") => i }
      body.find(_.head == q) match {
        case Some(row) if idx.nonEmpty =>
          val st = idx.map(i => if (i < row.length) row(i) else "MISSING")
          st.find(!_.startsWith("MATCH")).getOrElse(if (st.forall(_ == "MATCH")) "MATCH" else "MATCH(unordered)")
        case _ => "n/a"
      }
  }

  def qLabel(q: String) = q.stripSuffix("-v2.4")
  def qKey(q: String) = (q.drop(1).takeWhile(_.isDigit).toIntOption.getOrElse(999), q)
  def n2(d: Double) = f"$d%.2f"
  def csvCell(c: String) = if (c.exists(ch => ch == ',' || ch == '"')) "\"" + c.replace("\"", "\"\"") + "\"" else c
  val outFs = new org.apache.hadoop.fs.Path(out).getFileSystem(hconf)

  Seq("hive", "iceberg").foreach { fmt =>
    val qs = stats.keys.filter(_._2 == fmt).map(_._3).toSeq.distinct.sortBy(qKey)
    if (qs.nonEmpty) {
      val header = Seq("query", "stock_avg_s", "tabbydb_avg_s", "improvement_pct", "speedup_x",
        "stock_rows", "tabbydb_rows", "rows_match", "result_check", "iterations", "notes")
      var totS = 0.0; var totT = 0.0; var nTot = 0
      val lines = qs.map { q =>
        val st = stats.get(("stock", fmt, q)); val tb = stats.get(("tabbydb", fmt, q))
        val ok = st.exists(_._3 == 0) && tb.exists(_._3 == 0)
        val notes = Seq(
          st.filter(_._3 > 0).map(x => s"stock failed ${x._3}/${x._2}"),
          tb.filter(_._3 > 0).map(x => s"tabbydb failed ${x._3}/${x._2}"),
          if (st.isEmpty) Some("no stock result") else None,
          if (tb.isEmpty) Some("no tabbydb result") else None,
          if (ok && !okSet.contains(q)) Some("not in totals") else None).flatten.mkString("; ")
        if (ok && okSet.contains(q)) { totS += st.get._1; totT += tb.get._1; nTot += 1 }
        val imp = if (ok && st.get._1 > 0) n2((st.get._1 - tb.get._1) / st.get._1 * 100) else ""
        val spd = if (ok && tb.get._1 > 0) n2(st.get._1 / tb.get._1) else ""
        val rowsMatch = (st, tb) match { case (Some(a), Some(b)) => if (a._4 == b._4) "Y" else "N"; case _ => "" }
        Seq(qLabel(q), st.map(x => n2(x._1)).getOrElse(""), tb.map(x => n2(x._1)).getOrElse(""), imp, spd,
          st.map(_._4.mkString("|")).getOrElse(""), tb.map(_._4.mkString("|")).getOrElse(""),
          rowsMatch, resultCheck(fmt, q), st.orElse(tb).map(_._2.toString).getOrElse(""), notes)
      }
      val total = Seq(s"TOTAL ($nTot queries)", n2(totS), n2(totT),
        if (totS > 0) n2((totS - totT) / totS * 100) else "", if (totT > 0) n2(totS / totT) else "",
        "", "", "", "", "", "same query set as the summary")
      val table = header +: (lines :+ total)

      val f = new org.apache.hadoop.fs.Path(s"$out/${fmt}_side_by_side.csv")
      val o = outFs.create(f, true)
      try o.write(table.map(_.map(csvCell).mkString(",")).mkString("", "\n", "\n").getBytes("UTF-8")) finally o.close()

      println(s"\n[report] ===== $fmt: stock vs TabbyDB, average of iterations (tab-separated, paste into Excel) =====")
      table.foreach(r => println(r.mkString("\t")))
      println(s"[report] ===== end $fmt — also saved as $f =====")
    }
  }

  println(s"[report] CSVs written to $out")
  println("BENCH_STEP_OK")
  System.exit(0)
} catch {
  case e: Throwable => e.printStackTrace(); System.exit(1)
}
