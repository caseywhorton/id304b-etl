# id304b-etl
ETL pipeline documentation for id304b covered entity (CE) data.

# services

+ AWS Lambda
+ AWS RDS (PostgreSQL)
+ AWS Glue
+ AWS IAM
+ AWS S3
+ pgAdmin
+ AWS EC2

# Set up

Postgres Admin and VS Code for local testing.



# Business Entities

+ dim_ce
+ dim_ce_medicaid_numbers
+ dim_npi
+ dim_ce_street_address
+ dim_ce_billing_address
+ dim_ce_shipping_address
+ dim_ce_contract_pharmacy
+ dim_pharmacy

# Change Data Capture (CDC)

The composite key of id304b and ceId should be the unique identifier for the JSON documents in the JSON file downloaded every day.  
Using this composite key, we can search for existing data and UPDATE instead of only inserting.  
Update the expiration date for the old record.

# Source Data


#

# ETL Process

Changes
