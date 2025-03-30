import sys
import json
import logging
import boto3
import psycopg2
from sqlalchemy import create_engine, text
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from etl.etl import process_dim_ce_contract_pharmacies  # Import your function

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Adjust level as needed

logger.info("Glue job started...")

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME','db_user', 'db_password', 'db_host', 'db_name'])

# Extract arguments
db_user = args['db_user']
db_password = args['db_password']
db_host = args['db_host']
db_name = args['db_name']

# Initialize Glue and Spark
sc = SparkContext()  # Fix: Properly initialize SparkContext in Glue
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read JSON from S3
s3_bucket = "and-health-bucket"
s3_key = "OPA_CE_DAILY_PUBLIC.JSON"

logger.info(f"Reading JSON from S3: s3://{s3_bucket}/{s3_key}")

s3 = boto3.client('s3')

try:
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    json_content = response['Body'].read().decode('utf-8')
    data = json.loads(json_content)
    logging.info("Successfully read JSON file from S3")
except Exception as e:
    logging.error(f"Failed to read JSON file from S3: {e}")
    sys.exit(1)

# Extract 'coveredEntities' list
covered_entities = data.get("coveredEntities", [])
if not covered_entities:
    logging.warning("No 'coveredEntities' found in the JSON file.")
    sys.exit(0)

# Connect to PostgreSQL
try:
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}")
    conn = engine.connect()
    logging.info("Successfully connected to PostgreSQL")
except Exception as e:
    logging.error(f"Failed to connect to PostgreSQL: {e}")
    sys.exit(1)

# Process each document in 'coveredEntities'
for entity in covered_entities[0:2]:
    try:
        process_dim_ce_contract_pharmacies(entity, conn, "prod")
    except Exception as e:
        logging.error(f"Error processing entity {entity}: {e}")

# Close the database connection
conn.close()
logging.info("Glue job completed successfully.")

job.commit()
