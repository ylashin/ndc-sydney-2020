// Same stuff applies to any complex/nested data structures
import java.nio.ByteBuffer
import java.util.Base64


val getArray = udf((id: Long) => {
    val rnd = new scala.util.Random(id % 1000)
    val array = (1 to 100).map(x => rnd.nextInt(100))

    val bytes = new Array[Byte](array.length * 4)
    val buffer = ByteBuffer.wrap(bytes)
    array.foreach(x => buffer.putInt(x))
    Base64.getEncoder.encodeToString(bytes)
})


val data = spark.range(10000000).toDF
    .withColumn("StringCol", concat(lit("prefix-double-size-text-"), round(rand() * 10, 0) cast "int", lit("-suffix-double-size-text")) )
    .withColumn("ConstantCol1", lit("HelloBigData"))
    .withColumn("ConstantCol2", lit(123.45))
    .withColumn("ArrayColumn", getArray($"id"))

data.repartition(12, $"ArrayColumn").write.mode("overwrite").orc("/home/yousry/data/know-your-data/case8")

data.show(5)



//java -jar .\orc-tools-1.6.3-uber.jar data .\data\case8\part-