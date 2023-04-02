from os import path

from aws_cdk import IgnoreMode, Stack
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda
from aws_cdk import aws_lambda_event_sources as event_source
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as assets
from constructs import Construct


class AdobeProjectStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # path to src - used for defining assets
        src_path = path.join(path.dirname('.'), 'src')

        # role for the glue job
        glue_job_role = iam.Role(
            self,
            id="adobe-glue-role",
            role_name="AdobeGlueRole",
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSGlueServiceRole'),
            ],
        )

        # the s3 bucket to store hit data
        adobe_bucket = s3.Bucket(
            self,
            id="adobe-bucket",
            bucket_name="jays-adobe-project",
            access_control=s3.BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # glue job should have read/write permission on s3
        adobe_bucket.grant_read_write(glue_job_role)

        # role for the lambda function
        lambda_role = iam.Role(
            self,
            id="adobe-lambda-role",
            role_name="AdobeLambdaRole",
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
        )

        # lambda function should have read/write permission on s3
        adobe_bucket.grant_read_write(lambda_role)

        # lambda function should be able start the glue job, and write logs
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:StartJobRun",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=["*"],
                effect=iam.Effect.ALLOW,
            )
        )

        # the lambda function which triggers the glue job
        lambda_function = aws_lambda.Function(
            self,
            id="adobe-lambda-trigger-glue-etl",
            function_name="adobe-lambda-trigger-glue-etl",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=aws_lambda.Code.from_asset(
                path=path.join(src_path, 'lambda', 'trigger_glue_etl'),
            ),
            role=lambda_role,
        )

        # lambda function should be triggered by s3 event
        lambda_function.add_permission(
            's3-service-principal',
            principal=iam.ServicePrincipal('s3.amazonaws.com'),
        )

        lambda_function.add_event_source(
            event_source.S3EventSource(
                adobe_bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(
                    prefix="input/", suffix=".tsv")],
            )
        )

        # asset for the glue job
        glue_job_lib_asset = assets.Asset(
            self,
            id="glue-asset",
            path=path.dirname('.'),
            exclude=['*', '*.*', '!.gitignore', '!src', '!src/**/__init__.py',
                     '!src/glue', '!src/glue/**/*', 'handler.py'],
            ignore_mode=IgnoreMode.GIT,
        )

        glue_job_main_asset = assets.Asset(
            self,
            id="adobe-glue-job-main-asset",
            path=path.join(
                src_path, 'glue',
                'search_keyword_performance', 'handler.py'
            ),
        )

        glue_job_lib_asset.grant_read(glue_job_role)
        glue_job_main_asset.grant_read(glue_job_role)

        # the glue job which processes the hit data file
        glue.CfnJob(
            self,
            id="adobe-glue-job-search-keyword-performance",
            name="adobe-search-keyword-performance",
            description="Glue job to process Adobe hit data and calculate performance for search keywords",
            glue_version="4.0",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=glue_job_main_asset.s3_object_url,
            ),
            role=glue_job_role.role_arn,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            timeout=10,
            number_of_workers=1,
            worker_type="Standard",
            default_arguments={
                "--additional-python-modules": "tldextract",
                "--extra-py-files": glue_job_lib_asset.s3_object_url,
            },
        )
