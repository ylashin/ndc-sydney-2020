val data = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-"), round(rand() * 1000000, 0) cast "int" , lit("-suffix")) )

data.write.mode("overwrite").orc("/home/yousry/data/know-your-data/case2")

data.show(5, false)


