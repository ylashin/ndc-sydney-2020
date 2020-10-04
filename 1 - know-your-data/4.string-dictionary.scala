val data = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int" , lit("-suffix-double-size-text")) )

data.write.mode("overwrite").orc("/home/yousry/data/know-your-data/case4")

data.show(5, false)


