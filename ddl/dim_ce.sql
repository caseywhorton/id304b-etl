DROP TABLE IF EXISTS prod.dim_ce;

CREATE TABLE prod.dim_ce (
    id340b VARCHAR(50) NOT NULL,
    ce_id VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    subname VARCHAR(100) NOT NULL DEFAULT 'N/A',
    participating_flg CHAR(1) NOT NULL DEFAULT 'Y',
    participating_start_stp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    grant_number VARCHAR(50) NOT NULL DEFAULT 'N/A',
    cert_decert_stp TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    edit_stp TIMESTAMP,
    auth_official_name VARCHAR(100) NOT NULL DEFAULT 'N/A',
    auth_official_title VARCHAR(100) NOT NULL DEFAULT 'N/A',
    auth_official_phone_number VARCHAR(15) NOT NULL DEFAULT 'N/A',
    primary_contact_name VARCHAR(100) NOT NULL DEFAULT 'N/A',
    primary_contact_title VARCHAR(100) NOT NULL DEFAULT 'N/A',
    primary_contact_phone_number VARCHAR(15) NOT NULL DEFAULT 'N/A',
    primary_contact_phone_ext VARCHAR(10) NOT NULL DEFAULT 'N/A',
    effective_stp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_stp TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    current_version_flg CHAR(1) NOT NULL DEFAULT 'Y',
    change_type VARCHAR(20) NOT NULL DEFAULT 'N/A'
);