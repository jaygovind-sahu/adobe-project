import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from src.glue.search_keyword_performance.processor import generate_perf_report

spark = SparkSession.builder \
    .appName("adobe_project") \
    .enableHiveSupport() \
    .getOrCreate()

args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'data_s3_bucket',
        'data_s3_key',
    ]
)

generate_perf_report(
    hit_data_bucket=args['data_s3_bucket'],
    hit_data_file=args['data_s3_key'],
    spark=spark,
)
