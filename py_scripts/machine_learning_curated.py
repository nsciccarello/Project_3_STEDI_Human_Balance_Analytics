import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Loading Accelerometer Trusted data from S3 and rename time column to "timestamp"
AccelerometerTrustedData = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/trusted_zone/accelerometer_trusted/"]
    },
    transformation_ctx="AccelerometerTrustedData",
)

AccelerometerTrustedData = AccelerometerTrustedData.rename_field("<actual_column_name>", "timestamp")  

# Loading Step Trainer Trusted data from S3 and rename time column to "sensorReadingTime"
StepTrainerTrusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/trusted_zone/step_trainer_trusted/"]
    },
    transformation_ctx="StepTrainerTrusted",
)

StepTrainerTrusted = StepTrainerTrusted.rename_field("<actual_column_name>", "sensorReadingTime")  

# Performing Join operation on renamed columns
JoinNode = Join.apply(
    frame1=AccelerometerTrustedData,
    frame2=StepTrainerTrusted,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinNode",
)

# Drop unnecessary fields
DropUnnecessaryFields = DropFields.apply(
    frame=JoinNode,
    paths=["user"],
    transformation_ctx="DropUnnecessaryFields",
)

# Write to S3 in Parquet format
SendDataToS3 = glueContext.write_dynamic_frame.from_options(
    frame=DropUnnecessaryFields,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/curated_zone/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="SendDataToS3",
)

job.commit()
