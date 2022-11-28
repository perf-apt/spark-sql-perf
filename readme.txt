To run the tpcds tests.

build this branch's jar using
sbt clean package 

Once the jar is created.

Go to spark checkout.
spark should be build with hive-thrift-server and hive module enabled.

build the icebrg spark-runtime jars.


start the command shell as

./bin/spark-shell --jars /Users/ashahid/workspace/spark-tpcds-perf/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/Users/ashahid/workspace/iceberg/spark/v3.2/spark-runtime/build/libs/iceberg-spark-runtime-3.2_2.12-0.14.1.7.2.17.0-SNAPSHOT.jar --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.spark_catalog.type=hadoop --conf "spark.sql.catalog.spark_catalog.warehouse=/tmp/iceberg_warehouse" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse/" --conf "spark.hive.metastore.warehouse.dir=/tmp/hive-warehouse" --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*"


The last conf is needed for enabling remote debuggig.
