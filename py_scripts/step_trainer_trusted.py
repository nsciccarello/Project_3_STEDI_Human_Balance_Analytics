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

# Loading Customer Curated data and remove duplicates based on serialNumber
CustomerCurated = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://stedi-human-balance-analytics-project/curated_zone/customer_curated/"]},
    transformation_ctx="CustomerCurated",
)

CustomerCurated = DynamicFrame.fromDF(
    CustomerCurated.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "CustomerCurated"
)

# Loading Step Trainer Landing data (no deduplication applied here)
StepTrainerLanding = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/landing_zone/step_trainer_landing/"]
    },
    transformation_ctx="StepTrainerLanding",
)

# Performing Join operation on serialNumber
Join_function = Join.apply(
    frame1=StepTrainerLanding,
    frame2=CustomerCurated,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_function",
)

# Selecting relevant columns to reduce output size
DropFields = ApplyMapping.apply(
    frame=Join_function,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="DropFields",
)

# Writing Step Trainer Trusted data to Trusted Zone in S3
StepTrainerTrusted = glueContext.write_dynamic_frame.from_options(
    frame=DropFields,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/trusted_zone/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted",
)

job.commit()
