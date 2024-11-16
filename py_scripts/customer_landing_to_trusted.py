import sys
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initializing Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Reading data from S3 landing zone into a DynamicFrame
customer_landing_df_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/landing_zone/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_df_node",
)

# Filtering the DynamicFrame to include only records where 'shareWithResearchAsOfDate' is not 0
filtered_customer_landing_df_node = Filter.apply(
    frame=customer_landing_df_node,
    f=lambda row: not (row.get("shareWithResearchAsOfDate", 0) == 0),
    transformation_ctx="filtered_customer_landing_df_node",
)

# Writing the filtered DynamicFrame to the trusted zone in S3
trusted_customer_write_node = glueContext.write_dynamic_frame.from_options(
    frame=filtered_customer_landing_df_node,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/trusted_zone/customer_trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE", 
    },
    transformation_ctx="trusted_customer_write_node",
)

# Commit job
job.commit()
