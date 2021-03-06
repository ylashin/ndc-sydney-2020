val basePath = "/mnt/c/data/skewness"

val df = spark.read.orc(s"${basePath}/raster-table")

df.show

df.where($"sa4_name16" === "Ipswich").coalesce(4).write.mode("overwrite").orc(s"${basePath}/data-coalesced")

df.where($"sa4_name16" === "Ipswich").repartition(4).write.mode("overwrite").orc(s"${basePath}/data-repartitioned")