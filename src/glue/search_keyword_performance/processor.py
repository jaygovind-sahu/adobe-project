from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (coalesce, col, explode_outer, lit,
                                   row_number, split)
from pyspark.sql.functions import sum as fsum
from pyspark.sql.functions import udf, when
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

from src.glue.search_keyword_performance.parse_hit_data import (
    parse_referrer_data, parse_revenue)
from src.glue.search_keyword_performance.search_engines import SEARCH_ENGINE_QS_PARAM_MAPPING

OUTPUT_FILE_NAME_FORMAT = '{iso_date}_SearchKeywordPerformance.tab'


def load_raw_data(input_data_path: str, spark: SparkSession) -> DataFrame:
    """Load the data at given input data path into a Spark DataFrame

    Args:
        input_data_path (str): input data path
        spark (SparkSession): the spark session

    Returns:
        DataFrame: the output DataFrame
    """
    print(f"Loading input data from {input_data_path}")
    return spark.read.csv(
        path=input_data_path,
        sep="\t",
        header=True,
    )


def rank_hit_data_by_time(raw_data: DataFrame) -> DataFrame:
    """Group the hit data by IP address, and rank events by time - 

    The transformation is based on the assumption that, for every IP,
    the first event is when the user searches for something - so, we 
    can the search engine, and keyword information from that event. The
    last event, on the other hand, is when the purchase event may 
    happen, so we can get the revenue information from that event.

    Args:
        raw_data (DataFrame): the raw data

    Returns:
        DataFrame: a DataFrame with a event sequence number and other relevant columns
    """

    window_event_1 = Window.partitionBy(['ip']).orderBy('hit_time_gmt') \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)

    search_engine_list = list(SEARCH_ENGINE_QS_PARAM_MAPPING.keys())

    hit_journeys = raw_data \
        .withColumn('is_purchase', when(col('event_list') == 1, True).otherwise(False)) \
        .withColumn('is_search', col('referrer')
                    .rlike('|'.join(search_engine_list))) \
        .withColumn('journey_num',
                    fsum(when(col('is_purchase'), 1)
                         .otherwise(0)).over(window_event_1)) \
        .withColumn('journey_num', coalesce('journey_num', lit(0)))

    window_time = Window.partitionBy(['ip', 'journey_num', 'is_search']) \
        .orderBy(col('hit_time_gmt').desc())

    return hit_journeys.withColumn('event_seq_no', row_number().over(window_time)) \
        .where(col('event_seq_no') == 1) \
        .select(['ip', 'journey_num', 'is_search', 'is_purchase', 'referrer', 'product_list'])


def summarize_hit_data(ranked_by_time: DataFrame) -> DataFrame:
    """Select only the first and last event for an IP - 
    - First event should have the referrer data (search engine, keyword)
    - Last event should have the product (revenue) data

    Args:
        ranked_by_time (DataFrame): a DataFrame with events ranked by time for an IP

    Returns:
        DataFrame: a DataFrame with first and last events for an IP
    """
    first_hit = ranked_by_time \
        .where(col('is_search')).select(['ip', 'journey_num', 'referrer'])
    last_hit = ranked_by_time \
        .where(col('is_purchase')).select(['ip', 'journey_num', 'product_list'])

    return first_hit.join(
        last_hit,
        on=[first_hit.ip == last_hit.ip,
            first_hit.journey_num == last_hit.journey_num],
        how='left'
    ).select(['referrer', explode_outer(split('product_list', ',')).alias('product')])


def parse_referrer_revenue_data(hit_summary: DataFrame) -> DataFrame:
    """Parse referrer (search engine, keyword) and revenue data

    Args:
        hit_summary (DataFrame): a DataFrame with first and last events for an IP

    Returns:
        DataFrame: a DataFrame with seach engine, keyword, and revenue data
    """
    referrer_data_schema = StructType([
        StructField("search_engine", StringType(), True),
        StructField("search_keyword", StringType(), True),
    ])
    udf_parse_referrer_data = udf(parse_referrer_data, referrer_data_schema)
    udf_parse_revenue = udf(parse_revenue)

    return hit_summary \
        .select(udf_parse_referrer_data("referrer").alias("referrer_data"), "product") \
        .select(
            col("referrer_data.search_engine").alias("search_engine_domain"),
            col("referrer_data.search_keyword").alias("search_keyword"),
            udf_parse_revenue(col("product")).alias("revenue"),
        )


def agg_keyword_revenue(search_data: DataFrame) -> DataFrame:
    """Aggregate revenue for each search engine, and keyword combination

    Args:
        search_data (DataFrame): a DataFrame with seach engine, keyword, and revenue data

    Returns:
        DataFrame: a DataFrame with revenue information for each search engine and keyword
    """
    return search_data.groupBy(['search_engine_domain', 'search_keyword']) \
        .agg(fsum('revenue').alias('revenue')) \
        .sort(col('revenue').desc())


def write_output(perf_report: DataFrame, data_bucket: str) -> None:
    """Write output in the given data bucket (s3)

    Args:
        perf_report (DataFrame): input DataFrame
        data_bucket (str): S3 bucket where output needs to be written
    """
    iso_date = datetime.now().strftime("%Y-%m-%d")
    output_file = OUTPUT_FILE_NAME_FORMAT.format(iso_date=iso_date)
    output_path = f"s3://{data_bucket}/output/{output_file}"
    print(f"Writing output at {output_path}")
    perf_report.toPandas().to_csv(output_path, sep="\t", index=False)


def generate_perf_report(hit_data_bucket: str, hit_data_file: str, spark: SparkSession) -> None:
    """Generate search keyword performance report

    Args:
        hit_data_bucket (str): S3 bucket where hit data is present
        hit_data_file (str): a file in S3 with hit data
        spark (SparkSession): the Spark session
    """
    input_data_path = f"s3://{hit_data_bucket}/{hit_data_file}"
    raw_data = load_raw_data(input_data_path, spark)
    ranked_by_time = rank_hit_data_by_time(raw_data)
    hit_summary = summarize_hit_data(ranked_by_time)
    search_data = parse_referrer_revenue_data(hit_summary)
    perf_report = agg_keyword_revenue(search_data)
    perf_report.show(truncate=False)
    write_output(perf_report, hit_data_bucket)
