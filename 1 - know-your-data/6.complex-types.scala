val getArray = udf((id: Long) => {
   val rnd = new scala.util.Random(id % 1000)
   (1 to 100).map(x => rnd.nextInt(100))
})


val data = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int", lit("-suffix-double-size-text")) )
    .withColumn("ConstantCol1", lit("HelloBigData"))
    .withColumn("ConstantCol2", lit(123.45))
    .withColumn("ArrayColumn", getArray($"id"))

data.write.mode("overwrite").orc("/home/yousry/data/know-your-data/case6")

data.show(5)

val distinctArrayValuesCount = data.select("ArrayColumn").distinct.count
println(s"Distinct array values count : ${distinctArrayValuesCount}")


