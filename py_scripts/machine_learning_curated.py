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

# Reading data from the Glue Data Catalog table "accelerometer_trusted" in the "stedi_project_database" database
accelerometer_trusted_df_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_df_node",
)

# Reading data from the Glue Data Catalog table "step_trainer_trusted" in the "stedi_project_database" database
step_trainer_trusted_df_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_df_node",
)

# Performing a join between "step_trainer_trusted" and "accelerometer_trusted" DynamicFrames
step_trainer_accelerometer_joined_df_node = Join.apply(
    frame1=step_trainer_trusted_df_node,
    frame2=accelerometer_trusted_df_node,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="step_trainer_accelerometer_joined_df_node",
)

# Dropping unnecessary fields from the joined DynamicFrame
cleaned_step_trainer_accelerometer_df_node = DropFields.apply(
    frame=step_trainer_accelerometer_joined_df_node,
    paths=["user"],
    transformation_ctx="cleaned_step_trainer_accelerometer_df_node",
)

# Writing the cleaned DynamicFrame to the Glue Data Catalog table "machine_learning_curated" in the "stedi_project_database" database
write_machine_learning_curated_node = glueContext.write_dynamic_frame.from_options(
    frame=cleaned_step_trainer_accelerometer_df_node,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/curated_zone/machine_learning_curated/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="write_machine_learning_curated_node",
)

# Commit the job
job.commit()
