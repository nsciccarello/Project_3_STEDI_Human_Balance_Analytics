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

# Loading customer landing data from Glue catalog
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_project_database",
    table_name="customer_landing"
)

# Filtering to include only customers who agreed to share their data (shareWithResearchAsOfDate is not null)
customer_trusted = Filter.apply(frame=customer_landing, f=lambda x: x["shareWithResearchAsOfDate"] is not None)

# Writing filtered data to the trusted zone in S3 (trusted_zone/customer_trusted/)
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-human-balance-analytics-project/trusted_zone/customer_trusted/"
    },
    format="parquet"
)

# Commit job
job.commit()
