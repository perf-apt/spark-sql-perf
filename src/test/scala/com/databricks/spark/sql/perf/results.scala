import org.apache.spark.sql.functions._
val resultLocation = "/Users/ashahid/workspace/tpcds-benchmark/results_stockspark"
val result = spark.read.json(resultLocation).filter("timestamp=1745345780729").select(explode($"results").as("r"))
result.createOrReplaceTempView("result")
spark.sql("select r.name, r.numRows,  bround((r.parsingTime+r.analysisTime+r.optimizationTime+r.planningTime+r.executionTime)/1000.0,1) as Runtime_sec  from result").show(50)


