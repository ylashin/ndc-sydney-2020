val data = spark.range(10000000).toDF

data.write.mode("overwrite").orc("/home/yousry/data/know-your-data/case1")

data.show(5)

