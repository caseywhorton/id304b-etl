import sys
import json
import logging
import boto3
import pg8000
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from etl.etl import process_dim_ce_contract_pharmacies, process_dim_ce_medicaid, process_dim_ce_npi, process_dim_ce_street_address, process_dim_ce_billing_address, process_dim_ce_shipping_address, process_dim_ce

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Adjust level as needed
logger.info("Glue job started...")

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'db_user', 'db_password', 'db_host', 'db_name', 'bucket', 'key'])

# Extract arguments
db_user = args['db_user']
db_password = args['db_password']
db_host = args['db_host']
db_name = args['db_name']
bucket_name = args['bucket']
file_key = args['key']

# Initialize Glue and Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read JSON from S3
s3 = boto3.client('s3')
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    json_content = response['Body'].read().decode('utf-8')
    data = json.loads(json_content)
    logger.info("Successfully read JSON file from S3")
except Exception as e:
    logger.error(f"Failed to read JSON file from S3: {e}")
    sys.exit(1)

# Initialize database connection
# This was missing in your original script
try:
    conn = pg8000.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        database=db_name,
        port=5432  # Default PostgreSQL port, adjust if needed
    )
    logger.info("Successfully connected to the database")
except Exception as e:
    logger.error(f"Failed to connect to database: {e}")
    sys.exit(1)

# Extract 'coveredEntities' list
covered_entities = data.get("coveredEntities", [])
if not covered_entities:
    logging.warning("No 'coveredEntities' found in the JSON file.")
    sys.exit(0)

# Process each document in 'coveredEntities'
for entity in covered_entities[0:2000]:
    try:
        process_dim_ce_contract_pharmacies(entity, conn, "prod")
        process_dim_ce_medicaid(entity, conn, "prod")
        process_dim_ce_npi(entity, conn, "prod")
        process_dim_ce_street_address(entity, conn, "prod")
        process_dim_ce_billing_address(entity, conn, "prod")
        process_dim_ce_shipping_address(entity, conn, "prod")
        process_dim_ce(entity, conn, "prod")
    except Exception as e:
        logging.error(f"Error processing entity {entity}: {e}")

# Close the database connection
conn.close()
logging.info("Glue job completed successfully.")
job.commit()