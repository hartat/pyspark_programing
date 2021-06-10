import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("read and load from hive").master("local[1]").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def main():
    data=extract_data("/home/hartat/data/output/netflix_titles-partition")
    transform_data,country_count=transform(data)
    load_data(transform_data,country_count)

def extract_data(file_name):
    df = spark.read.option("header", True).csv(file_name)
    return df

def transform(df):
    df = df.filter(col("release_year") == '2020')
    transform_data = df.withColumn("country", explode(split(col("country"), ",")))
    country_count = transform_data.groupby(trim(lower("country"))).count().orderBy("count", ascending=False)
    return transform_data,country_count

def load_data(transform_data,country_count):
    transform_data.write.format("csv").option("header", "true").mode("overwrite").save("/home/hartat/data/output/2020_country")
    country_count.repartition(1).write.format("csv").option("header", "true").mode("overwrite").save("/home/hartat/data/output/2020_country_count")

if __name__ == '__main__':
    main()

#comment to check branch merging
