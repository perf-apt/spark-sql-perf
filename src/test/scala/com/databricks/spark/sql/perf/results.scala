import org.apache.spark.sql.functions._
val resultLocation = "/opt/tpcds-benchmark/tpcds-results/tabbydb/100GB"
val result = spark.read.json(resultLocation).filter("timestamp=1757349405229").select(explode($"results").as("r"))
result.createOrReplaceTempView("result")
spark.sql("select r.name, r.numRows,  bround((r.parsingTime+r.analysisTime+r.optimizationTime+r" +
  ".planningTime+r.executionTime)/1000.0,1) as Runtime_sec  from result").show(1000)


