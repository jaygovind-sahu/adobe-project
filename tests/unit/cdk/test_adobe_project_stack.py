import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk.adobe_project_stack import AdobeProjectStack

app = core.App()
stack = AdobeProjectStack(app, "adobe-project")
template = assertions.Template.from_stack(stack)


def test_resource_counts():
    template.resource_count_is('AWS::S3::Bucket', 1)
    template.resource_count_is('AWS::Lambda::Function', 2)
    template.resource_count_is('AWS::IAM::Role', 3)
    template.resource_count_is('AWS::Glue::Job', 1)


def test_s3_bucket_properties():
    template.has_resource_properties('AWS::S3::Bucket', {
        "BucketName": "jays-adobe-project",
        "AccessControl": "BucketOwnerFullControl",
        "BucketEncryption":  {
            "ServerSideEncryptionConfiguration": [{
                "ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}
            }]
        },
        "PublicAccessBlockConfiguration": {
            "BlockPublicAcls": True,
            "BlockPublicPolicy": True,
            "IgnorePublicAcls": True,
            "RestrictPublicBuckets": True
        },
    })
