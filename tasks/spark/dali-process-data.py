from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as F
import platform
print('Python version:', platform.python_version())

OUT_URI = 's3://enx-datascience-dali-dq/dali-data-preprocessed-test'

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SQLContext(sc)

df_old = spark.read.parquet("s3://enx-datascience-dali-dq/dali-data-preprocessed")
startdate_update = datetime.datetime.combine(df_old.select(F.max("date")).first()['max(date)'],
                                             datetime.time.min) + datetime.timedelta(days=1)

enddate_update = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# json_path = "s3://enxt-dl-raw/salvador/sensordata/readings/year=2019/month={}/*/*".format(datetime.datetime.now().strftime('%m'))
# print('Load files from:', json_path)
json_path = 's3://enxt-dl-raw/salvador/sensordata/readings/year=2019/month=02/day=21/enxt231-dali-salvador-readings-ingestion-firehose-3-2019-02-21-03-36-30-81743f50-6079-4453-bb10-bb03aba12a9b'
df = spark.read.json(json_path)

df = df \
    .select('boxid', 'channelid', 'timestamp', 'value') \
    .dropDuplicates(["boxid", "timestamp", "value"]) \
    .withColumn("timestamp", F.col('timestamp').cast('timestamp')) \
    .withColumn('date', F.col('timestamp').cast('date')) \
    .filter(F.col('timestamp').between(startdate_update, enddate_update - datetime.timedelta(minutes=15)))

print('Write output')
df.write.parquet(OUT_URI, mode='append')
print('Finished writing output')
exit()
