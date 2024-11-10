import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initializing Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Loading Customer Trusted data from S3
CustomerTrusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/trusted_zone/customer_trusted/"]
    },
    transformation_ctx="CustomerTrusted",
)

# Loading Accelerometer Landing data from S3
AccelerometerLanding = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-analytics-project/landing_zone/accelerometer_landing/"]
    },
    transformation_ctx="AccelerometerLanding",
)

# Performing Join operation on email and user columns
CustomerPrivacyFilter = Join.apply(
    frame1=CustomerTrusted,
    frame2=AccelerometerLanding,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyFilter",
)

# Deduplicating data based on customer information (e.g., serialNumber) to get unique customers
# This assumes that `serialNumber` or another unique identifier is present in the joined data
UniqueCustomers = DynamicFrame.fromDF(
    CustomerPrivacyFilter.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "UniqueCustomers"
)

# Mapping and dropping unnecessary fields
DropFields = ApplyMapping.apply(
    frame=UniqueCustomers,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="DropFields",
)

# Writing curated data to S3 in Customer Curated folder
CustomerCurated = glueContext.write_dynamic_frame.from_options(
    frame=DropFields,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/curated_zone/customer_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated",
)

job.commit()
