from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as F
import platform
import sys
import re
import boto3

print("Running dali_preprocess_json2parquet.py ...")

conf = SparkConf()
sc = SparkContext(conf = conf)
spark = SQLContext(sc)

#  def to retrieve folders in a S3 bucket
def get_bucket_keys(full_path):


    ''' 
    Function to get the 'folders' in an S3 bucket/prefix

    :param (str) full_path: string of the full path to the target 'folder' 
        eg: "s3://enx-datascience-dali-dq/Bram-dali-data-preprocessed"

    :param (list<str>) s3_bucket_keys: list with keys of s3 bucket
    '''

    client = boto3.client('s3')

    # strip full_path with regular expressions
    delimiter = '/'
    bucket = re.search("(?<=s3:\/\/)(.*?)(?=\/)", full_path, flags=re.IGNORECASE).group()
    regexpr_get_prefix = r"(?<=s3:\/\/" + re.escape(bucket) + re.escape(delimiter) + r")(.*?)(.*$)"
    prefix = re.search(regexpr_get_prefix, full_path, flags=re.IGNORECASE).group() + delimiter

    # retreive objects from bucket
    objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)

    #
    regexpr_dump_prefix = r"(?<=" + re.escape(prefix) + r")(.*?)(?=\/)"

    result = []
    if objs.get('CommonPrefixes'):
        for obj in objs.get('CommonPrefixes'):
            w_prefix = obj.get('Prefix')
            wo_prefix = re.search(regexpr_dump_prefix, w_prefix, flags=re.IGNORECASE).group()
            result.append(wo_prefix)

    return result

# define full path to s3 bucket and set minimal date
URL_parquet = "s3://enx-datascience-dali-dq/Bram-dali-data-preprocessed"
default_date = datetime.date(2019, 2, 1)


# get already processed dates from S3 bucket
s3_keys = get_bucket_keys(URL_parquet)
dates_s3 = [datetime.datetime.strptime(key, "date=%Y-%m-%d").date() for key in s3_keys];
dates_s3.append(default_date)
last_up_date = max(dates_s3)
# set yesterday as target date to update
yester_date = datetime.date.today() - datetime.timedelta(days=1)

# set dates manually
last_up_date = datetime.date(2019, 3, 10)
yester_date = datetime.date(2019, 3, 13)

date_list = [last_up_date + datetime.timedelta(days=x)\
             for x in range(0, (yester_date - last_up_date).days+1)]

print("\tWill preprocess dates %s untill %s" % (last_up_date, yester_date))

for d in date_list:
    print("\tprocessing: %s" % d)

    # datetime.datetime.combine(last_up_date, datetime.time.min)

    # startdate_update = datetime.datetime(2019, 4, 22, 0, 0)
    # enddate_update = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    print("\t\tReading json files ...")
    df = spark.read.json("s3://enxt-dl-raw/salvador/sensordata/readings/year=%04d/month=%02d/day=%02d/*" % (d.year,d.month,d.day))

    print("\t\tPreprocessing data ...")
    df = df \
        .select('boxid', 'channelid', 'timestamp', 'value') \
        .dropDuplicates(["boxid", "timestamp", "value"]) \
        .withColumn("timestamp", F.col('timestamp').cast('timestamp')) \
        .withColumn('date', F.col('timestamp').cast('date'))
        .withColumn("json_date_delay", F.datediff(F.lit(d), F.to_date("timestamp")))
        # .filter(F.col('timestamp').between(startdate_update, enddate_update - datetime.timedelta(minutes=15)))

    print("\t\tWriting parquet files ...")
    df.repartition("boxid") \
        .write.parquet(URL_parquet, partitionBy='date', mode='append')

print('Finished dali_preprocess_json2parquet.py')
exit()
