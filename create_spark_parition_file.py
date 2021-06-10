import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("read and create parition file").master("local[*]").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df=spark.read.option("header",True).csv("/home/hartat/data/netflix_titles-orginal.csv")
print(df.rdd.getNumPartitions())
df=df.repartition(10,"type")
print(df.rdd.getNumPartitions())
df.write.format("csv").option("header", "true").mode("overwrite").save("/home/hartat/data/netflix_titles-partition")
spark.stop()
