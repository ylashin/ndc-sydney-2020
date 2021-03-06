/* 

You need to prepare a docker image with atlas and spark
Then compile spark-atlas-connector inside the container

docker start atlaswithspark

cd /bootcamp
/opt/spark-2.4.6-bin-hadoop2.7/bin/spark-shell \
--jars /opt/spark-atlas-connector/spark-atlas-connector-assembly/target/spark-atlas-connector-assembly-0.1.0-SNAPSHOT.jar \
--conf spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
--conf spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
--conf spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker \
--name BootCampApplication \
-i ./demo.scala

*/

def readCsv(path: String) = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

val orders = readCsv("./orders.csv")

val countries = readCsv("./countries.csv")

val ordersWithCountryName = orders.join(countries, Seq("CountryCode")).drop("CountryCode")
ordersWithCountryName.show(5)

ordersWithCountryName.repartition(1).write.mode("overwrite").option("header", "true").csv("./ordrers-enriched")

sys.exit(0)