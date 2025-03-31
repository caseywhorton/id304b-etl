DROP TABLE IF EXISTS prod.dim_pharmacy;

CREATE TABLE prod.dim_pharmacy (
    pharmacy_id VARCHAR(50) NOT NULL,
    address_line1 VARCHAR(100) NOT NULL,
    address_city VARCHAR(50) NOT NULL,
    address_state VARCHAR(2) NOT NULL,
    address_zip VARCHAR(10) NOT NULL,
    phone_number VARCHAR(15) NOT NULL,
    effective_stp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_stp TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    current_version_flg CHAR(1) NOT NULL DEFAULT 'Y',
    change_type VARCHAR(20)
);