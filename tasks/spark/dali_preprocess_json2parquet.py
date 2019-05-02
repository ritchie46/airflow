from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as F
import platform
import sys
import re
import boto3
from urllib.parse import urlparse

print("Running dali_preprocess_json2parquet.py ...")

conf = SparkConf()
sc = SparkContext(conf = conf)
spark = SQLContext(sc)

# #  def to retrieve folders in a S3 bucket
# def get_bucket_keys(full_path):
#
#
#     '''
#     Function to get the 'folders' in an S3 bucket/prefix
#
#     :param (str) full_path: string of the full path to the target 'folder'
#         eg: "s3://enx-datascience-dali-dq/Bram-dali-data-preprocessed"
#
#     :param (list<str>) s3_bucket_keys: list with keys of s3 bucket
#     '''
#
#     client = boto3.client('s3')
#
#     # strip full_path with regular expressions
#     delimiter = '/'
#     bucket = re.search("(?<=s3:\/\/)(.*?)(?=\/)", full_path, flags=re.IGNORECASE).group()
#     regexpr_get_prefix = r"(?<=s3:\/\/" + re.escape(bucket) + re.escape(delimiter) + r")(.*?)(.*$)"
#     prefix = re.search(regexpr_get_prefix, full_path, flags=re.IGNORECASE).group() + delimiter
#
#     # retreive objects from bucket
#     objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
#
#     #
#     regexpr_dump_prefix = r"(?<=" + re.escape(prefix) + r")(.*?)(?=\/)"
#
#     result = []
#     if objs.get('CommonPrefixes'):
#         for obj in objs.get('CommonPrefixes'):
#             w_prefix = obj.get('Prefix')
#             wo_prefix = re.search(regexpr_dump_prefix, w_prefix, flags=re.IGNORECASE).group()
#             result.append(wo_prefix)
#
#     return result



# define location and filename of log
URL_parquet = "s3://enx-datascience-dali-dq/Bram-dali-data-preprocessed"
log_filename = "log_preprocessing.txt"
bottom_date = datetime.date(2019, 2, 1)

# parse for bucket and key S3
parsed = urlparse(URL_parquet + "/" + log_filename)
bucket = parsed.netloc
key = parsed.path.lstrip('/')

# setup S3 resource object
s3 = boto3.resource('s3')
obj_s3 = s3.Object(bucket, key)

# retrieve from S3 dates that have been preprocessed and parse dates
log_body = obj_s3.get()['Body'].read()
log_byte = log_body.decode().split("\r\n")
done_dates = [datetime.datetime.strptime(s[0:10],"%Y-%m-%d").date() for s in log_byte]

# set date limits which need to be present and create range between them
yester_date = datetime.date.today() - datetime.timedelta(days=1)
total_dates = [bottom_date + datetime.timedelta(days=x) for x in range(0, (yester_date - bottom_date).days+1)]

# determine dates to do
to_do_dates = list(set(total_dates) - set(done_dates))
to_do_dates.sort()

print("\tFollowing dates will be preprocessed:")
_ = [print("\t\t\t\t\t\t%s" % td) for td in to_do_dates]

for d in to_do_dates:
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

    # add just preprocessed date + clean and sort list
    done_dates = done_dates + [d]
    done_dates = list(set(done_dates))
    done_dates.sort()

    # ad processing date, encode and write to S3
    log_list = ["%s processed on %s" % (d,datetime.date.today()) for d in done_dates]
    log_byte = str.encode("\r\n".join(log_list))
    _ = obj.put(Body=log_byte)

print('Finished dali_preprocess_json2parquet.py')
exit()
