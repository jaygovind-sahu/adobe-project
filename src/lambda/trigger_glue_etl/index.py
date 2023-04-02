import logging
from pathlib import PurePath
from urllib.parse import unquote

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = "adobe-search-keyword-performance"


def stage_data_file(data_bucket: str, data_object: str) -> str:
    """Stage the input data file by sanitizing the file name

    Args:
        data_bucket (str): event S3 bucket
        data_object (str): event S3 object

    Returns:
        str: the staging key
    """
    file_name = PurePath(data_object).name

    staging_key = "staging/" + "".join(
        letter if (letter.isalnum() or letter in ".-_")
        else '_'
        for letter in file_name
    )

    s3_resource = boto3.resource('s3')
    s3_resource.Object(data_bucket, staging_key).copy_from(
        CopySource=f"/{data_bucket}/{data_object}")

    return staging_key


def handler(event, context):
    """The lambda handler -

    Start the Glue ETL job to process the input file which triggered this function

    Args:
        event (json): the event which triggered lambda function
        context (object): context

    Returns:
        json: response from glue start_job_run function
    """
    event_s3_info = event['Records'][0]['s3']
    logger.info(event_s3_info)

    data_bucket = event_s3_info["bucket"]["name"]
    data_object = event_s3_info["object"]["key"]

    staging_key = stage_data_file(data_bucket, unquote(data_object))

    glue_client = boto3.client('glue')
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--data_s3_bucket": data_bucket,
            "--data_s3_key": staging_key,
        }
    )
    logger.info(f"Glue job run id: {response['JobRunId']}")
    return response
