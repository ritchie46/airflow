from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as F
import platform
import sys

print('Python version:', platform.python_version())

OUT_URI = 's3://enx-datascience-dali-dq/dali-data-preprocessed-test'

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SQLContext(sc)

year = int(sys.argv[0])
month = int(sys.argv[1])
day = int(sys.argv[2])
startdate_update = datetime.datetime(year, month, day, 0, 0) + datetime.timedelta(days=1)

enddate_update = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

dates_to_parse = [startdate_update + datetime.timedelta(i) for i in range((enddate_update-startdate_update).days)]

base_path = "s3://enxt-dl-raw/salvador/sensordata/readings/year={}/month={}/day={}/*"
json_paths = [base_path.format(i.strftime('%Y'), i.strftime('%m'), i.strftime('%d')) for i in dates_to_parse]

# print('Load files from:', json_path)
df = spark.read.json(json_paths)

df = df \
    .select('boxid', 'channelid', 'timestamp', 'value') \
    .dropDuplicates(["boxid", "timestamp", "value"]) \
    .withColumn("timestamp", F.col('timestamp').cast('timestamp')) \
    .withColumn('date', F.col('timestamp').cast('date')) \
    .filter(F.col('timestamp').between(startdate_update, enddate_update - datetime.timedelta(minutes=15)))

print('Write output')
df.write.parquet(OUT_URI, partionBy='date', mode='append')
print('Finished writing output')
exit()
