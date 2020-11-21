import sys.process._

"rm -rf /home/yousry/data/know-your-data" !

println("#" * 100)

def printOutputSize(df: org.apache.spark.sql.DataFrame, truncateOutput: Boolean = false) = { 
    
    df.show(5, truncateOutput)

    "du -h -d1 /home/yousry/data/know-your-data" #|  "sort -k 2" ! 

    println("#" * 100)
    println()
    //readLine()
}

val simpleArray = spark.range(10000000).toDF
simpleArray.write.mode("overwrite").orc("/home/yousry/data/know-your-data/1-simple-array")

printOutputSize(simpleArray)

//------------------------------------------------------------------------------------------------

val withStringColumn = simpleArray
    .withColumn("StringCol", concat(lit("prefix-"), round(rand() * 1000000, 0) cast "int" , lit("-suffix")) )

withStringColumn.write.mode("overwrite").orc("/home/yousry/data/know-your-data/2-with-string-column")

printOutputSize(withStringColumn)

//------------------------------------------------------------------------------------------------

val withLessCardinality = simpleArray
    .withColumn("StringCol", concat(lit("prefix-"), round(rand() * 10, 0) cast "int" , lit("-suffix")) )

withLessCardinality.write.mode("overwrite").orc("/home/yousry/data/know-your-data/3-with-less-cardinality")

printOutputSize(withLessCardinality)

//------------------------------------------------------------------------------------------------

val withWideStringColumn = simpleArray
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int" , lit("-suffix-double-size-text")) )

withWideStringColumn.write.mode("overwrite").orc("/home/yousry/data/know-your-data/4-with-wider-string")

printOutputSize(withWideStringColumn)

//------------------------------------------------------------------------------------------------

val withConstantColumns = withWideStringColumn
    .withColumn("ConstantCol1", lit("HelloBigData"))
    .withColumn("ConstantCol2", lit(123.45))

withConstantColumns.write.mode("overwrite").orc("/home/yousry/data/know-your-data/5-with-constant-columns")

printOutputSize(withConstantColumns)

//------------------------------------------------------------------------------------------------

val getArray = udf((id: Long) => {
   val rnd = new scala.util.Random(id % 1000)
   (1 to 100).map(x => rnd.nextInt(100))
})

val withArrayColumn = withConstantColumns
    .withColumn("ArrayColumn", getArray($"id"))
    .cache

withArrayColumn.write.mode("overwrite").orc("/home/yousry/data/know-your-data/6-with-array-column")

val distinctArrayValuesCount = withArrayColumn.select("ArrayColumn").distinct.count
println(s"Distinct array values count : ${distinctArrayValuesCount}")

printOutputSize(withArrayColumn, true)

//------------------------------------------------------------------------------------------------

// THIS HACK WORKS BEST WITH LOW CARDINALITY

val getArrayAsString = udf((id: Long) => {
    val rnd = new scala.util.Random(id % 1000)
    val array = (1 to 100).map(x => rnd.nextInt(100))
    
    array.map(_.toString).mkString(",")
})

val withArrayPersistedAsString = withConstantColumns
    .withColumn("ArrayColumn", getArrayAsString($"id"))

withArrayPersistedAsString.write.mode("overwrite").orc("/home/yousry/data/know-your-data/7-with-string-array-column")

printOutputSize(withArrayPersistedAsString, true)

//------------------------------------------------------------------------------------------------


