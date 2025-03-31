DROP TABLE IF EXISTS prod.dim_ce_medicaid;

CREATE TABLE prod.dim_ce_medicaid (
    id340b VARCHAR(50) NOT NULL,
    ce_id VARCHAR(50) NOT NULL,
    medicaid_number VARCHAR(50) NOT NULL DEFAULT 'N/A',
    state VARCHAR(2) NOT NULL DEFAULT 'NA',
    effective_stp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expiration_stp TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    current_version_flg CHAR(1) NOT NULL DEFAULT 'Y',
    change_type VARCHAR(20) NOT NULL DEFAULT 'N/A'
);