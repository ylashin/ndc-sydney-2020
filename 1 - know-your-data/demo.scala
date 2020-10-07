import sys.process._

"rm -rf /home/yousry/data/know-your-data" !

println("-" * 100)

def printOutputSize() = { 
    
    "du -h -d1 /home/yousry/data/know-your-data" #|  "sort -k 2" ! 

    println("-" * 100)
    readLine()
}

val simpleArray = spark.range(10000000).toDF

simpleArray.write.mode("overwrite").orc("/home/yousry/data/know-your-data/1-simple-array")

simpleArray.show(5)

printOutputSize()

//------------------------------------------------------------------------------------------------

val withStringColumn = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-"), round(rand() * 1000000, 0) cast "int" , lit("-suffix")) )

withStringColumn.write.mode("overwrite").orc("/home/yousry/data/know-your-data/2-with-string-column")

withStringColumn.show(5, false)

printOutputSize()

//------------------------------------------------------------------------------------------------

val withLessCardinality = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-"), round(rand() * 10, 0) cast "int" , lit("-suffix")) )

withLessCardinality.write.mode("overwrite").orc("/home/yousry/data/know-your-data/3-with-less-cardinality")

withLessCardinality.show(5, false)

printOutputSize()

//------------------------------------------------------------------------------------------------

val withWideStringColumn = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int" , lit("-suffix-double-size-text")) )

withWideStringColumn.write.mode("overwrite").orc("/home/yousry/data/know-your-data/4-with-wider-string")

withWideStringColumn.show(5, false)


printOutputSize()

//------------------------------------------------------------------------------------------------

val withConstantColumns = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int" , lit("-suffix-double-size-text")) )
    .withColumn("ConstantCol1", lit("HelloBigData"))
    .withColumn("ConstantCol2", lit(123.45))

withConstantColumns.write.mode("overwrite").orc("/home/yousry/data/know-your-data/5-with-constant-columns")

withConstantColumns.show(5, false)


printOutputSize()

//------------------------------------------------------------------------------------------------

val getArray = udf((id: Long) => {
   val rnd = new scala.util.Random(id % 1000)
   (1 to 100).map(x => rnd.nextInt(100))
})

val withArrayColumn = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int", lit("-suffix-double-size-text")) )
    .withColumn("ConstantCol1", lit("HelloBigData"))
    .withColumn("ConstantCol2", lit(123.45))
    .withColumn("ArrayColumn", getArray($"id"))
    .cache

withArrayColumn.write.mode("overwrite").orc("/home/yousry/data/know-your-data/6-with-array-column")

withArrayColumn.show(5)

val distinctArrayValuesCount = withArrayColumn.select("ArrayColumn").distinct.count
println(s"Distinct array values count : ${distinctArrayValuesCount}")

printOutputSize()

//------------------------------------------------------------------------------------------------

// THIS HACK WORKS BEST WITH LOW CARDINALITY

val getArrayAsString = udf((id: Long) => {
    val rnd = new scala.util.Random(id % 1000)
    val array = (1 to 100).map(x => rnd.nextInt(100))
    
    array.map(_.toString).mkString(",")
})

val withArrayPersistedAsString = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int", lit("-suffix-double-size-text")) )
    .withColumn("ConstantCol1", lit("HelloBigData"))
    .withColumn("ConstantCol2", lit(123.45))
    .withColumn("ArrayColumn", getArrayAsString($"id"))

withArrayPersistedAsString.write.mode("overwrite").orc("/home/yousry/data/know-your-data/7-with-string-array-column")

withArrayPersistedAsString.show(5)

printOutputSize()

//------------------------------------------------------------------------------------------------


