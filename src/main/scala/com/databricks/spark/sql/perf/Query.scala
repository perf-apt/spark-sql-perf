/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.SparkPlan

import java.io.{PrintWriter, StringWriter}


/** Holds one benchmark query and its metadata. */
class Query(
    override val name: String,
    buildDataFrame: => DataFrame,
    val description: String = "",
    val sqlText: Option[String] = None,
    override val executionMode: ExecutionMode = ExecutionMode.ForeachResults)
  extends Benchmarkable with Serializable {

  private implicit def toOption[A](a: A): Option[A] = Option(a)

  override def toString: String = {
    try {
      s"""
         |== Query: $name ==
         |${buildDataFrame.queryExecution.analyzed}
     """.stripMargin
    } catch {
      case e: Exception =>
        s"""
           |== Query: $name ==
           | Can't be analyzed: $e
           |
           | $description
         """.stripMargin
    }
  }

  lazy val tablesInvolved = buildDataFrame.queryExecution.logical collect {
    case r: UnresolvedRelation => r.tableName
  }

  def newDataFrame() = buildDataFrame

  protected override def doBenchmark(
      includeBreakdown: Boolean,
      description: String = "",
      messages: ArrayBuffer[String]): BenchmarkResult = {
    try {
      val dataFrame = buildDataFrame
      val queryExecution = dataFrame.queryExecution
      // We are not counting the time of ScalaReflection.convertRowToScala.
      val parsingTime = measureTimeMs {
        queryExecution.logical
      }
      val analysisTime = measureTimeMs {
        queryExecution.analyzed
      }
      val optimizationTime = measureTimeMs {
        queryExecution.optimizedPlan
      }
      val planningTime = measureTimeMs {
        queryExecution.executedPlan
      }

      val breakdownResults = if (includeBreakdown) {
        val depth = queryExecution.executedPlan.collect { case p: SparkPlan => p }.size
        val physicalOperators = (0 until depth).map(i => (i, queryExecution.executedPlan.p(i)))
        val indexMap = physicalOperators.map { case (index, op) => (op, index) }.toMap
        val timeMap = new mutable.HashMap[Int, Double]
        val maxFields = 999 // Maximum number of fields that will be converted to strings

        physicalOperators.reverse.map {
          case (index, node) =>
            messages += s"Breakdown: ${node.simpleString(maxFields)}"
            val newNode = buildDataFrame.queryExecution.executedPlan.p(index)
            val executionTime = measureTimeMs {
              newNode.execute().foreach((row: Any) => ())
            }
            timeMap += ((index, executionTime))

            val childIndexes = node.children.map(indexMap)
            val childTime = childIndexes.map(timeMap).sum
            messages += s"Breakdown time: $executionTime (+${executionTime - childTime})"

            BreakdownResult(
              node.nodeName,
              node.simpleString(maxFields).replaceAll("#\\d+", ""),
              index,
              childIndexes,
              executionTime,
              executionTime - childTime)
        }
      } else {
        Seq.empty[BreakdownResult]
      }

      // The executionTime for the entire query includes the time of type conversion from catalyst
      // to scala.
      // Note: queryExecution.{logical, analyzed, optimizedPlan, executedPlan} has been already
      // lazily evaluated above, so below we will count only execution time.
      var result: Option[Long] = None
      var numRowsOutput = 0
      var collectedRows: Array[Row] = null
      val executionTime = measureTimeMs {
        executionMode match {
          case ExecutionMode.CollectResults =>
               val x = dataFrame.collect()
               numRowsOutput = x.length
               collectedRows = x

          case ExecutionMode.ForeachResults =>
            numRowsOutput = dataFrame.collect().length
            dataFrame.foreach { _ => ():Unit }

          case ExecutionMode.WriteParquet(location) =>
            dataFrame.write.parquet(s"$location/$name.parquet")
          case ExecutionMode.HashResults =>
            // SELECT SUM(CRC32(CONCAT_WS(", ", *))) FROM (benchmark query)
            numRowsOutput = dataFrame.collect().length
            val row =
              dataFrame
                .selectExpr(s"sum(crc32(concat_ws(',', *)))")
                .head()
            result = if (row.isNullAt(0)) None else Some(row.getLong(0))
        }
      }

      // Outside the timed block above: save the rows this run already collected, for the
      // correctness check (only if spark.sql.perf.captureResultsDir is set). Never fails the query.
      if (collectedRows != null) {
        QueryResultCapture.save(dataFrame.sparkSession, name, Right(collectedRows))
      }

      val joinTypes = dataFrame.queryExecution.executedPlan.collect {
        case k if k.nodeName contains "Join" => k.nodeName
      }

      BenchmarkResult(
        name = name,
        mode = executionMode.toString,
        joinTypes = joinTypes,
        tables = tablesInvolved,
        parsingTime = parsingTime,
        analysisTime = analysisTime,
        optimizationTime = optimizationTime,
        planningTime = planningTime,
        executionTime = executionTime,
        result = result,
        queryExecution = dataFrame.queryExecution.toString,
        breakDown = breakdownResults,
        numRows = numRowsOutput)
    } catch {
      case e: Exception =>
        val sw: StringWriter = new StringWriter();
        val pw: PrintWriter = new PrintWriter(sw);
        e.printStackTrace(pw);
        SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach { ss =>
          QueryResultCapture.save(ss, name, Left(s"${e.getClass.getName}: ${e.getMessage}"))
        }
         BenchmarkResult(
           name = name,
           mode = executionMode.toString,
           failure = Failure(e.getClass.getName, e.getMessage + "...." + sw.toString))
    }
  }

  /** Change the ExecutionMode of this Query to HashResults, which is used to check the query result. */
  def checkResult: Query = {
    new Query(name, buildDataFrame, description, sqlText, ExecutionMode.HashResults)
  }
}

/**
 * Saves a query's result rows (or its error) as <captureResultsDir>/<query>.tsv, so results of the
 * timed runs can be compared across engines afterwards. Enabled by setting the SQL conf
 * spark.sql.perf.captureResultsDir; a no-op otherwise. Called after timing has finished.
 *
 * Format: first line "#rows=N" (or "#error=..."), then one line per row, cells tab-separated:
 *   N = null, D:<double> = float/double (compared with tolerance), S:<text> = compared exactly.
 */
object QueryResultCapture {
  val ConfKey = "spark.sql.perf.captureResultsDir"

  private def enc(v: Any): String = v match {
    case null => "N"
    case d: Double if d.isNaN || d.isInfinite => "S:" + d.toString
    case d: Double => "D:" + java.lang.Double.toString(d)
    case f: Float => "D:" + java.lang.Double.toString(f.toDouble)
    case b: java.math.BigDecimal => "S:" + b.stripTrailingZeros.toPlainString
    case x => "S:" + x.toString.replace('\t', ' ').replace('\n', ' ')
  }

  def save(spark: SparkSession, queryName: String, rows: Either[String, Array[Row]]): Unit = {
    try {
      val dir = spark.conf.getOption(ConfKey).map(_.trim).filter(_.nonEmpty)
      dir.foreach { d =>
        val content = rows match {
          case Right(rs) =>
            (s"#rows=${rs.length}" +: rs.toSeq.map(_.toSeq.map(enc).mkString("\t"))).mkString("\n")
          case Left(err) =>
            "#error=" + String.valueOf(err).take(500).replace('\n', ' ')
        }
        val target = new org.apache.hadoop.fs.Path(s"${d.stripSuffix("/")}/$queryName.tsv")
        val fs = target.getFileSystem(spark.sparkContext.hadoopConfiguration)
        fs.mkdirs(target.getParent)
        val tmp = new org.apache.hadoop.fs.Path(target.toString + ".tmp")
        val out = fs.create(tmp, true)
        try out.write(content.getBytes("UTF-8")) finally out.close()
        fs.delete(target, false)
        fs.rename(tmp, target)
      }
    } catch {
      case e: Throwable =>
        System.err.println(s"[QueryResultCapture] could not save result of $queryName: $e")
    }
  }
}
