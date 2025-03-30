DROP TABLE IF EXISTS local_dev.dim_ce_contract_pharmacy;

CREATE TABLE local_dev.dim_ce_contract_pharmacy (
    id340b VARCHAR(50) NOT NULL,
    ce_id VARCHAR(50) NOT NULL,
    contract_id VARCHAR(50) NOT NULL DEFAULT 'N/A',
    pharmacy_id VARCHAR(50) NOT NULL DEFAULT 'N/A',
    "name" varchar(100) NOT NULL DEFAULT 'N/A',
    address_line1 VARCHAR(100) NOT NULL DEFAULT 'N/A',
    address_city VARCHAR(50) NOT NULL DEFAULT 'N/A',
    address_state VARCHAR(2) NOT NULL DEFAULT 'NA',
    address_zip VARCHAR(10) NOT NULL DEFAULT 'N/A',
    phone_number VARCHAR(15) NOT NULL DEFAULT 'N/A',
    effective_stp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_stp TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    current_version_flg CHAR(1) NOT NULL DEFAULT 'Y',
    change_type VARCHAR(20) NOT NULL DEFAULT 'N/A'
);