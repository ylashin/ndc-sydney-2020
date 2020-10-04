val daysRange = 365

val data = spark.range(100000000)
  .toDF
  .withColumn("start", lit("2019-01-01") cast "date")
  .withColumn("increment", ((rand() * daysRange) % daysRange) cast "int")
  .withColumn("date", date_add($"start", $"increment"))
  .drop("start")
  .withColumn("year", year($"date"))
  .withColumn("month", month($"date"))
  .withColumn("day", dayofmonth($"date"))
  .repartition($"year", $"month", $"day")
  .cache

println(s"Record count: ${data.count}")

data.write
    .partitionBy("year", "month", "day")
    .mode("overwrite")
    .orc("/mnt/c/data/metadata-matters")

data.write
    .partitionBy("year", "month", "day")
    .mode("overwrite")
    .orc("/home/yousry/data/metadata-matters")

// File System impact
// du -h -d1 /home/yousry/data/metadata-matters/year\=2019/
// du -h -d1 /mnt/c/data/metadata-matters/year\=2019/


///////////////////////////////////////////////////////////////////////////////////

//val networkPath = """\\wsl$\Ubuntu-20.04\home\yousry\data\metadata-matters"""
//spark.read.orc(networkPath).where($"year" === 2015 && $"month" === 10 && $"day".between(1,7)).count

spark.read.orc("/mnt/c/data/metadata-matters").where($"year" === 2019 && $"month" === 10 && $"day".between(1,7)).count
spark.read.orc("/home/yousry/data/metadata-matters").where($"year" === 2019 && $"month" === 10 && $"day".between(1,7)).count

val paths = (1 to 7).map(x => s"/mnt/c/data/metadata-matters/year=2019/month=10/day=$x")
spark.read.option("basePath", "/mnt/c/data/metadata-matters")
  .orc(paths: _*)
  .count


///////////////////////////////////////////////////////////////////////////////////
val tableCreationSql = """
CREATE EXTERNAL TABLE events (id LONG, increment INT, date DATE)
PARTITIONED BY (year INT,month INT, day INT)
STORED AS ORC
LOCATION '/mnt/c/data/metadata-matters'
"""
spark.sql("drop table if exists events")
spark.sql(tableCreationSql)
spark.sql("MSCK REPAIR TABLE events")

// Hive needs to be bootstrapped
spark.table("events").where($"year" === 2019 && $"month" === 10 && $"day".between(1,7)).count