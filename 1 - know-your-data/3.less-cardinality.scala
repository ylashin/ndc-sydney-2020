val data = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-"), round(rand() * 10, 0) cast "int" , lit("-suffix")) )

data.write.mode("overwrite").orc("/home/yousry/data/know-your-data/case3")

data.show(5, false)

