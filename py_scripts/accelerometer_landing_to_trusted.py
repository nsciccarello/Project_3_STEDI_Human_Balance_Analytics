import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initializing Glue and Spark Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Reading customer_trusted and accelerometer_landing data from Glue catalog
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="customer_trusted"
)
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="accelerometer_landing"
)

# Joining accelerometer data with customer data to keep only consented records
joined_data = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"]
)

# Writing the joined data to the Trusted Zone in S3
glueContext.write_dynamic_frame.from_options(
    frame=joined_data,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/trusted_zone/accelerometer_trusted/"
    },
    format="parquet"
)

# Commit the job
job.commit()
