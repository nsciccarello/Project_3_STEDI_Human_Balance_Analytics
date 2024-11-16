import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initializing Glue and Spark Context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Reading data from the Glue Data Catalog table "customers_curated" in the "stedi_project_database" database
customers_curated_df_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="customers_curated",
    transformation_ctx="customers_curated_df_node",
)

# Reading data from the S3 landing zone for "step_trainer_landing" into a DynamicFrame
step_trainer_landing_df_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/landing_zone/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_df_node",
)

# Performing a join between "step_trainer_landing" and "customers_curated" DynamicFrames
step_trainer_customers_joined_df_node = Join.apply(
    frame1=step_trainer_landing_df_node,
    frame2=customers_curated_df_node,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="step_trainer_customers_joined_df_node",
)

# Dropping unnecessary fields from the joined DynamicFrame
cleaned_step_trainer_customers_df_node = DropFields.apply(
    frame=step_trainer_customers_joined_df_node,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="cleaned_step_trainer_customers_df_node",
)

# Writing the cleaned DynamicFrame to the Glue Data Catalog table "step_trainer_trusted" in the database
write_step_trainer_trusted_node = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_step_trainer_customers_df_node,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/curated_zone/step_trainer_trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="write_step_trainer_trusted_node",
)

# Commit the job
job.commit()
