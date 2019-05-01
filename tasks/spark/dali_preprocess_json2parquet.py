from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as F
import platform
import sys

print("Running dali_preprocess_json2parquet.py ...")

conf = SparkConf()
sc = SparkContext(conf = conf)
spark = SQLContext(sc)

# df_old = spark.read.parquet("s3://enx-datascience-dali-dq/dali-data-preprocessed")
# startdate_update = datetime.datetime.combine(df_old.select(F.max("date")).first()['max(date)'], datetime.time.min)+datetime.timedelta(days=1)

URL_parquet = "s3://enx-datascience-dali-dq/Bram-dali-data-preprocessed"
startdate_update = datetime.datetime(2019, 4, 22, 0, 0)
enddate_update = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

print("\tReading json files ...")
df = spark.read.json(["s3://enxt-dl-raw/salvador/sensordata/readings/year=2019/month=04/day=26/*",
                      "s3://enxt-dl-raw/salvador/sensordata/readings/year=2019/month=04/day=27/*",
                      "s3://enxt-dl-raw/salvador/sensordata/readings/year=2019/month=04/day=28/*"])

# df = spark.read.json("s3://enxt-dl-raw/salvador/sensordata/readings/year=2019/*/*/*")

print("\tPreprocessing data ...")
df = df \
    .select('boxid', 'channelid', 'timestamp', 'value') \
    .dropDuplicates(["boxid", "timestamp", "value"]) \
    .withColumn("timestamp", F.col('timestamp').cast('timestamp')) \
    .withColumn('date', F.col('timestamp').cast('date')) \
    .filter(F.col('timestamp').between(startdate_update, enddate_update - datetime.timedelta(minutes=15)))

print("\tWriting parquet files ...")
df.repartition("boxid") \
    .write.parquet(URL_parquet, partitionBy='date', mode='append')

print('Finished dali_preprocess_json2parquet.py')
exit()
