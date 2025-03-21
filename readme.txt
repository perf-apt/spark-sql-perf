To run the tpcds tests.

build this branch's jar using
sbt clean package 

Once the jar is created.

Go to spark checkout.
spark should be build with hive-thriftserver and hive module enabled. ( -Phive -Phive-thriftserver)

build the icebrg spark-runtime jars.


start the command shell as

./bin/spark-shell --jars /Users/ashahid/workspace/spark-tpcds-perf/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/Users/ashahid/workspace/upstream-iceberg/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.4.0-SNAPSHOT.jar --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf "spark.sql.catalog.spark_catalog.warehouse=/tmp/iceberg_warehouse" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse/" --conf "spark.hive.metastore.warehouse.dir=/tmp/hive-warehouse" --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*"


The last conf is needed for enabling remote debuggig.


 ./bin/spark-shell --jars /Users/ashahid/workspace/spark-tpcds-perf/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/Users/ashahid/workspace/upstream-iceberg/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.4.0-SNAPSHOT.jar --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf "spark.sql.catalog.spark_catalog.warehouse=/tmp/iceberg_warehouse" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse/" --conf "spark.hive.metastore.warehouse.dir=/tmp/hive-warehouse" --driver-memory 4G --executor-memory 2G   --num-executors 2  --executor-cores 2 --conf "spark.sql.execution.broadcastHashJoin.pushKeysAsFilterToScan=true" --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*"


./bin/spark-shell --jars /Users/ashahid/workspace/spark-tpcds-perf/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/Users/ashahid/workspace/upstream-iceberg/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-1.4.0-SNAPSHOT.jar --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf "spark.sql.catalog.spark_catalog.warehouse=/tmp/iceberg_warehouse" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse/" --conf "spark.hive.metastore.warehouse.dir=/tmp/hive-warehouse" --driver-memory 8G --executor-memory 4G   --num-executors 2  --executor-cores 2 --conf "spark.sql.execution.broadcastHashJoin.pushKeysAsFilterToScan=true" --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8080" --conf "spark.sql.execution.broadcastHashJoin.preferBroadcastVarPushDownOverDPP=false" --conf "spark.sql.execution.broadcastHashJoin.preferReuseExchangeOverBroadcastVarPushdown=false"

./bin/spark-shell --jars /Users/ashahid/workspace/spark-tpcds-perf/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/Users/ashahid/workspace/upstream-iceberg/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.13-1.4.0-SNAPSHOT.jar --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf "spark.sql.catalog.spark_catalog.warehouse=/tmp/iceberg_warehouse" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse/" --conf "spark.hive.metastore.warehouse.dir=/tmp/hive-warehouse" --driver-memory 8G --executor-memory 4G   --num-executors 2  --executor-cores 2 --conf "spark.sql.execution.broadcastHashJoin.pushKeysAsFilterToScan=true"

./bin/spark-shell --jars /Users/ashahid/workspace/spark-tpcds-perf/spark-sql-perf/target/scala-2.13/spark-sql-perf_2.13-0.5.1-SNAPSHOT.jar,/Users/ashahid/workspace/personal/perf-apt/wildfire-iceberg/spark/v4.0.0-SNAPSHOT/spark-runtime/build/libs/iceberg-spark-runtime-4.0.0-SNAPSHOT_2.13-1.5.0-SNAPSHOT.jar --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf "spark.sql.catalog.spark_catalog.warehouse=/tmp/iceberg_warehouse" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse/" --conf "spark.hive.metastore.warehouse.dir=/tmp/hive-warehouse" --driver-memory 8G --executor-memory 4G   --num-executors 2  --executor-cores 2 --conf "spark.sql.execution.broadcastHashJoin.pushKeysAsFilterToScan=true"
