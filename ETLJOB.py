import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1685646743319 = glueContext.create_dynamic_frame.from_catalog(
    database="database",
    table_name="csvsource",
    transformation_ctx="AWSGlueDataCatalog_node1685646743319",
)

# Script generated for node Change Schema
ChangeSchema_node1685646779076 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1685646743319,
    mappings=[
        ("user_overall_rating", "long", "user_overall_rating", "long"),
        ("predicted_overall_rating", "double", "predicted_overall_rating", "double"),
        ("cleanliness", "long", "cleanliness", "int"),
        ("location", "long", "location", "long"),
        ("value", "long", "value", "long"),
        ("rooms", "long", "rooms", "long"),
        ("service", "long", "service", "long"),
        ("sleep_quality", "long", "sleep_quality", "long"),
    ],
    transformation_ctx="ChangeSchema_node1685646779076",
)

# Script generated for node Amazon S3
AmazonS3_node1685646992959 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1685646779076,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://s3bucket7487/target/", "partitionKeys": []},
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1685646992959",
)

job.commit()
