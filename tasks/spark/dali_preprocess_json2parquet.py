from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as F
from urllib.parse import urlparse
import platform
import sys
import re
import boto3
# import logs3 first need a clean way to upload libs in DAG

''' This script unpacks the DALI json-files, partitioned per measurement-date in parquet-format and saves metadata of this process.

json files are stored structured per year/month/day. 
- Per day a json file is read and unpacked.
- The content is parsed to extract the timestamp of the data.
- The content is saved in parquet format (partitioned per date of the parsed timestamp) in dali-sensordata.
- Metadata of this proces is saved in parquet format in pre-dali.

'''

print("Running dali_preprocess_json2parquet.py ...")

conf = SparkConf()
sc = SparkContext(conf = conf)
spark = SQLContext(sc)

# setup client s3 bucket
client = boto3.client("s3")

# import datetime
# # import boto3

# to put in seperate file if upload is implemented in emr.py
class LogDates():
    ''' This class reads and writes dates into an S3 log file

    '''

    def __init__(self, S3_URL, process_info=True, filename="log_dates.txt"):
        ''' Constructor method of class itself

        :param (str) S3_URL: url of S3 bucket ("S3://<bucket>/<key>")
        :param (bool, optional) process_info: True to add info of porcessing date. Default to True.
        :param (str, optional) filename: filename of the log file. Default to "log_dates.txt"
        '''
        from urllib.parse import urlparse

        self.S3_URL = S3_URL
        self.filename = filename
        self.process_info = process_info

        parsed = urlparse(S3_URL + "/" + filename)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')

        self.s3 = boto3.resource('s3')
        self.obj_s3 = self.s3.Object(parsed.netloc, parsed.path.lstrip('/'))

    def check_file_exists(self):
        ''' This method checks if key/file exists in S3

        If the key/file exists it returns True

        :return (type bool):
        '''
        import botocore

        try:
            self.obj_s3.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise
        else:
            # The object does exist.
            return True

    def clean_file(self):
        ''' This method writes an empty file in S3

        :return:
        '''
        _ = self.obj_s3.put(Body=b"")

    def date_range(self, start_date, end_date):
        ''' This method makes a date range between start and end date

        iyiuyiyi

        :param (str or datetime.date) start_date: the start date of the range given as string or datetime.date
        :param (str or datetime.date) end_date: the start date of the range given as string or datetime.date
        :return (list of datetime.date) dates: range of dates
        '''

        # if string make date
        if (isinstance(start_date, datetime.date) == False):
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        if (isinstance(end_date, datetime.date) == False):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        # make range
        dates = [start_date + datetime.timedelta(days=x) \
                 for x in range(0, (end_date - start_date).days + 1)]
        return dates

    def get_dates(self):
        ''' This method reads the file and returns de dates in it

        :return (list of datetime.date): dates that are found in file
        '''
        # retrieve from S3 dates that have been preprocessed and parse dates
        body_byte = self.obj_s3.get()['Body'].read()
        if len(body_byte):
            string_list = body_byte.decode().split("\r\n")
            dates = [datetime.datetime.strptime(s[0:10], "%Y-%m-%d").date() for s in string_list]
            dates = sorted(set(dates))
        else:
            dates = []
        return dates

    def write_dates(self, dates):
        ''' This method writes the file with only the dates given

        :param (list of datetime.date) dates: dates to write
        :return:
        '''
        # write dates

        # force a list and sort
        if isinstance(dates, datetime.date): dates = [dates]
        dates = sorted(set(dates))

        # format according to procces_info
        log_list = ["%s" % d for d in dates]
        if self.process_info:
            log_list = ["%s processed on %s" % (item, datetime.date.today()) for item in log_list]
        # encode and write to file
        log_byte = str.encode("\r\n".join(log_list))
        _ = self.obj_s3.put(Body=log_byte)

    def add_dates(self, dates):
        ''' This method adds dates to the dates already in the log file

        :param (list of datetime.date) dates: dates to add
        :return:
        '''
        # add dates and write

        # make sure it is a list
        if isinstance(dates, datetime.date): dates = [dates]

        # add dates + write to file
        dates = self.get_dates() + dates
        self.write_dates(dates)

    def remove_dates(self, dates):
        ''' This method removes dates from the log file

        :param (list of datetime.date) dates: dates to remove
        :return:
        '''

        # make sure it is a list
        if isinstance(dates, datetime.date): dates = [dates]

        # remove dates + write to file
        dates = list(set(self.get_dates()) - set(dates))
        self.write_dates(dates)

# check if s3 key exists
def s3_key_exists(date):
    try:
        parsed = urlparse(URL_target)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/") + "/" + "date=%s" % date
        _ = client.list_objects(Bucket=bucket, Prefix=key)["Contents"]
        # Key does exist
        return True
    except:
        # Key does not exist
        return False


# define location and filename of log
URL_target = "s3://enx-datascience-trusted/dali-sensordata"
log_filename = "log_preprocessing.txt"
bottom_date = datetime.date(2019, 2, 19)

URL_pre = "s3://enx-datascience-trusted/dali-pre"

# make object (and s3 file) for logging dates
log_obj = LogDates(URL_target, True)
if log_obj.check_file_exists() == False: log_obj.clean_file()

# retrieve from S3 dates that have been preprocessed
done_dates = log_obj.get_dates()

# set date limits which need to be present and create range between them
yester_date = datetime.date.today() - datetime.timedelta(days=1)
total_dates = log_obj.date_range(bottom_date, yester_date)

# determine dates to do
to_do_dates = sorted(set(total_dates) - set(done_dates))

print("\tFollowing dates will be preprocessed:")
_ = [print("\t\t\t\t\t\t%s" % td) for td in to_do_dates]

for d in to_do_dates:
    print("\tprocessing: %s" % d)

    print("\t\tReading json files ...")
    df = spark.read.json(
        "s3://enxt-dl-raw/salvador/sensordata/readings/year=%04d/month=%02d/day=%02d/*" % (d.year, d.month, d.day))

    print("\t\tPreprocessing data ...")
    df = df \
        .select('boxid', 'channelid', 'timestamp', 'value') \
        .dropDuplicates(["boxid", "timestamp", "channelid", "value"]) \
        .withColumn("timestamp", F.col('timestamp').cast('timestamp')) \
        .withColumn('date', F.col('timestamp').cast('date')) \
        .withColumn("json_date_delay", F.datediff(F.lit(d), F.to_date("timestamp")))
    # .filter(F.col('timestamp').between(startdate_update, enddate_update - datetime.timedelta(minutes=15)))

    print("\t\tWriting parquet files data...")
    df.repartition("boxid") \
            .write.parquet(URL_target, partitionBy='date', mode='append')

    # create pre file with timestamps
    print("\t\tAppending parquet files pre...")
    df_pre_d = df.groupBy("boxid", "channelid") \
        .agg(F.min(df.timestamp).alias('timestamp_first_in_file'), \
             F.max(df.timestamp).alias('timestamp_last_in_file')) \
        .withColumn("date_trusted_file", F.lit(d)) \
        .withColumn("timestamp_trusted_to_pre", F.lit(datetime.datetime.now())) \
        .withColumn('date_last_in_file', F.col('timestamp_last_in_file').cast('date'))
    df_pre_d.repartition("boxid") \
        .write.parquet(URL_pre, partitionBy='date_last_in_file', mode='append')

    # add just processed date
    if s3_key_exists(d):
        log_obj.add_dates(d)
    else:
        print("\t\tNo output result for %s!" % d)

print('Finished dali_preprocess_json2parquet.py')
exit()
