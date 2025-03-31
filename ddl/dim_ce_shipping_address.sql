DROP TABLE IF EXISTS prod.dim_ce_shipping_address;

CREATE TABLE prod.dim_ce_shipping_address (
    id340b VARCHAR(50) NOT NULL,
    ce_id VARCHAR(50) NOT NULL,
    is_340b_street_address_flg CHAR(1) NOT NULL DEFAULT 'Y',
    address_line1 VARCHAR(100) NOT NULL DEFAULT 'N/A',
    address_line2 VARCHAR(100) NOT NULL DEFAULT 'N/A',
    city VARCHAR(50) NOT NULL DEFAULT 'N/A',
    state VARCHAR(2) NOT NULL DEFAULT 'NA',
    zip VARCHAR(10) NOT NULL DEFAULT 'N/A',
    effective_stp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_stp TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    current_version_flg CHAR(1) NOT NULL DEFAULT 'Y',
    change_type VARCHAR(20) NOT NULL DEFAULT 'N/A'
);