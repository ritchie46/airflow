from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import platform
print('Python version:', platform.python_version())
import json
import boto3
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

PROCESSED_DATA_URI = 's3://enx-datascience-dali-dq/dali-data-preprocessed-test'
OUT_URI = "s3://enx-datascience-dali-dq/dali-data-completeness-test"

conf = SparkConf()
sc = SparkContext(conf = conf)
spark = SQLContext(sc)

# Get previous result and the last date:
df = spark.read.parquet("s3://enx-datascience-dali-dq/dali-data-completeness-test")
startdate_update = datetime.datetime.combine(df.select(F.max("date")).first()['max(date)'], datetime.time.min)+datetime.timedelta(days=1)
enddate_update = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

duration_period_seconds = (15*60) # number of seconds of intervals
# periods_per_day = 96 # number of measurements in a day
time_window = (enddate_update - startdate_update).days

allowed_delay_s=0

df = spark.read.parquet(PROCESSED_DATA_URI) \
            .dropDuplicates(["boxid", "channelid", "timestamp", "value"])\
            .filter(F.col('timestamp').between(startdate_update,enddate_update-datetime.timedelta(minutes=15))&F.col("channelid").rlike('register:\/\/electricity\/0\/current\/l[1-3]\?avg=15'))

# check if packets are on time or delayed
df = df.withColumn('delayed', F.when( ((F.second('timestamp')>allowed_delay_s) | (F.minute('timestamp')%15 != 0)), 1).otherwise(0))

df_installed = df.groupBy('boxid').agg({'timestamp': 'min'})\
                            .filter(F.hour('min(timestamp)').between(8,17))\
                            .withColumnRenamed('min(timestamp)', 'timestamp_installed')

# get min timestamp of, get # of total and delayed packets
df = df.groupBy('boxid', 'channelid', 'date').agg({'boxid': 'count', 'delayed':'sum'})\
                            .withColumnRenamed('count(boxid)', 'counted')\
                            .withColumnRenamed('sum(delayed)', 'delayed')

df = df_installed.join(df,'boxid', how='right')

df = df.withColumn('timestamp_installed_unix', F.unix_timestamp(F.col('timestamp_installed')))
df = df.withColumn('date_timestamp', df.date.cast('timestamp'))
df = df.withColumn('date_timestamp', F.unix_timestamp(F.col('date_timestamp')))
df = df.withColumn('timestamp_check',F.when(F.col('timestamp_installed_unix')>F.col('date_timestamp'), df.timestamp_installed_unix).otherwise(df.date_timestamp+(24*60*60)))

df = df.withColumn('expected', (df.timestamp_check - df.date_timestamp)/duration_period_seconds)

# determine completeness from counted / expected packets
df = df.withColumn('completeness', F.col('counted')/F.col('expected'))
# order columns and rows
df = df.select('boxid','channelid', 'date','timestamp_installed','expected','counted','completeness','delayed').orderBy(['boxid', 'channelid', 'date'])

df.write.parquet(OUT_URI, partitionBy='date', mode='append')