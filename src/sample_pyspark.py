from pyspark.sql import SparkSession

spark =SparkSession.builder.appName("SampleApp").getOrCreate()
df=spark.createDataFrame([(1,"foo"),(2,"bar"),(3,"kitchen")],["id,value"])
df.show()
