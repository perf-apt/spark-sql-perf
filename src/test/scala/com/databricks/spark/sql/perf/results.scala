import org.apache.spark.sql.functions._
val resultLocation = "/data/tpcds_benchmark/tpcds_results"
val result = spark.read.json(resultLocation).filter("timestamp=1744849500885").select(explode($"results").as("r"))
result.createOrReplaceTempView("result")
spark.sql("select   bround((r.parsingTime+r.analysisTime+r.optimizationTime+r.planningTime+r.executionTime)/1000.0,1) as Runtime_sec  from result").show(50)


