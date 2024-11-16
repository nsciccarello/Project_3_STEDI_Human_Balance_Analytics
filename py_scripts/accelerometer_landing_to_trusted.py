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

# Reading data from the Glue Data Catalog table "customer_trusted" in the "stedi_project_database" database
customer_trusted_df_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_df_node",
)

# Reading data from the Glue Data Catalog table "accelerometer_landing" in the "stedi_project_database" database
accelerometer_landing_df_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_df_node",
)

# Performing a join between the "accelerometer_landing" and "customer_trusted" DynamicFrames
joined_df_node = Join.apply(
    frame1=accelerometer_landing_df_node,
    frame2=customer_trusted_df_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="joined_df_node",
)

# Dropping unnecessary fields from the joined DynamicFrame
cleaned_df_node = DropFields.apply(
    frame=joined_df_node,
    paths=[
        "customername",
        "email",
        "phone",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "birthday",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="cleaned_df_node",
)

# Writing the cleaned DynamicFrame to the trusted zone in S3
S3bucket_trusted_write_node = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_df_node,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/trusted_zone/accelerometer_trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="S3bucket_trusted_write_node",
)

# Commit the job
job.commit()
