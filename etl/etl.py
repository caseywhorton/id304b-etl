import logging
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_dim_ce_contract_pharmacies(data, conn):
    try:
        id340b_value = data['id340B']
        ce_id_value = str(data['ceId'])
        contract_pharmacies = data['contractPharmacies']

        check_sql = text("""
            SELECT contract_id FROM local_dev.dim_ce_contract_pharmacy 
            WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
        """)
        
        existing_records = conn.execute(check_sql, {"id340b": id340b_value, "ce_id": ce_id_value}).fetchall()
        existing_contract_ids = {row.contract_id for row in existing_records}
        incoming_contract_ids = {str(pharmacy['contractId']) for pharmacy in contract_pharmacies}

        deleted_contracts = existing_contract_ids - incoming_contract_ids

        if deleted_contracts:
            update_deleted_sql = text("""
                UPDATE local_dev.dim_ce_contract_pharmacy
                SET expiration_stp = CURRENT_TIMESTAMP, 
                    current_version_flg = 'N',
                    change_type = 'DELETE'
                WHERE id340b = :id340b AND ce_id = :ce_id AND contract_id IN :deleted_contracts
            """)
            conn.execute(update_deleted_sql, {
                "id340b": id340b_value, 
                "ce_id": ce_id_value, 
                "deleted_contracts": tuple(deleted_contracts)
            })
            logging.info(f"Marked {len(deleted_contracts)} contract pharmacies as deleted.")

        for pharmacy in contract_pharmacies:
            contract_id = str(pharmacy['contractId'])
            pharmacy_id = pharmacy.get('pharmacyId', 'N/A')
            name = pharmacy.get('name', 'N/A')
            address = pharmacy.get('address', {})
            address_line1 = address.get('addressLine1', 'N/A')
            address_city = address.get('city', 'N/A')
            address_state = address.get('state', 'NA')
            address_zip = address.get('zip', 'N/A')
            phone_number = pharmacy.get('phoneNumber', 'N/A')

            check_sql = text("""
                SELECT * FROM local_dev.dim_ce_contract_pharmacy 
                WHERE id340b = :id340b AND ce_id = :ce_id AND contract_id = :contract_id AND current_version_flg = 'Y'
            """)

            result = conn.execute(check_sql, {
                "id340b": id340b_value,
                "ce_id": ce_id_value,
                "contract_id": contract_id
            })
            existing_record = result.fetchone()

            if existing_record is None:
                insert_sql = text("""
                    INSERT INTO local_dev.dim_ce_contract_pharmacy (
                        id340b, ce_id, contract_id, pharmacy_id, name, address_line1, address_city, 
                        address_state, address_zip, phone_number, effective_stp, expiration_stp, 
                        current_version_flg, change_type
                    ) VALUES (
                        :id340b, :ce_id, :contract_id, :pharmacy_id, :name, :address_line1, :address_city, 
                        :address_state, :address_zip, :phone_number, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'INSERT'
                    )
                """)

                conn.execute(insert_sql, {
                    "id340b": id340b_value,
                    "ce_id": ce_id_value,
                    "contract_id": contract_id,
                    "pharmacy_id": pharmacy_id,
                    "name": name,
                    "address_line1": address_line1,
                    "address_city": address_city,
                    "address_state": address_state,
                    "address_zip": address_zip,
                    "phone_number": phone_number
                })

                logging.info(f"Inserted new contract pharmacy: {contract_id}")

            else:
                if (existing_record.address_line1 != address_line1 or 
                    existing_record.address_city != address_city or 
                    existing_record.address_state != address_state or 
                    existing_record.address_zip != address_zip or 
                    existing_record.name != name or 
                    existing_record.phone_number != phone_number):

                    update_old_sql = text("""
                        UPDATE local_dev.dim_ce_contract_pharmacy
                        SET expiration_stp = CURRENT_TIMESTAMP, 
                            current_version_flg = 'N'
                        WHERE id340b = :id340b AND ce_id = :ce_id AND contract_id = :contract_id AND current_version_flg = 'Y'
                    """)
                    conn.execute(update_old_sql, {
                        "id340b": id340b_value, 
                        "ce_id": ce_id_value, 
                        "contract_id": contract_id
                    })

                    insert_new_sql = text("""
                        INSERT INTO local_dev.dim_ce_contract_pharmacy (
                            id340b, ce_id, contract_id, pharmacy_id, name, address_line1, address_city, 
                            address_state, address_zip, phone_number, effective_stp, expiration_stp, 
                            current_version_flg, change_type
                        ) VALUES (
                            :id340b, :ce_id, :contract_id, :pharmacy_id, :name, :address_line1, :address_city, 
                            :address_state, :address_zip, :phone_number, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'UPDATE'
                        )
                    """)

                    conn.execute(insert_new_sql, {
                        "id340b": id340b_value,
                        "ce_id": ce_id_value,
                        "contract_id": contract_id,
                        "pharmacy_id": pharmacy_id,
                        "name": name,
                        "address_line1": address_line1,
                        "address_city": address_city,
                        "address_state": address_state,
                        "address_zip": address_zip,
                        "phone_number": phone_number
                    })

                    logging.info(f"Updated contract pharmacy: {contract_id}")
                else:
                    logging.info(f"No changes detected for contract pharmacy: {contract_id}")

        conn.commit()
    except Exception as e:
        logging.error(f"Error processing contract pharmacies: {e}")


def process_dim_ce_medicaid(data, conn):
    logging.info("Processing Medicaid data.")
    id340b_value = data['id340B']
    ce_id_value = str(data['ceId'])
    medicaid_numbers = data['medicaidNumbers']

    check_sql = text("""
        SELECT medicaid_number, state FROM local_dev.dim_ce_medicaid 
        WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
    """)
    try:
        existing_records = conn.execute(check_sql, {"id340b": id340b_value, "ce_id": ce_id_value}).fetchall()
    except Exception as e:
        logging.error(f"Error fetching existing Medicaid records: {e}")
        return

    existing_medicaid_set = {(row.medicaid_number, row.state) for row in existing_records}
    incoming_medicaid_set = {(med['medicaidNumber'], med['state']) for med in medicaid_numbers}

    deleted_medicaid = existing_medicaid_set - incoming_medicaid_set

    if deleted_medicaid:
        update_deleted_sql = text("""
            UPDATE local_dev.dim_ce_medicaid
            SET expiration_stp = CURRENT_TIMESTAMP, 
                current_version_flg = 'N',
                change_type = 'DELETE'
            WHERE id340b = :id340b AND ce_id = :ce_id 
            AND (medicaid_number, state) IN :deleted_medicaid
        """)
        try:
            conn.execute(update_deleted_sql, {
                "id340b": id340b_value, 
                "ce_id": ce_id_value, 
                "deleted_medicaid": tuple(deleted_medicaid)
            })
            logging.info(f"Marked {len(deleted_medicaid)} Medicaid numbers as deleted.")
        except Exception as e:
            logging.error(f"Error updating deleted Medicaid records: {e}")

    for med in medicaid_numbers:
        medicaid_number = med['medicaidNumber']
        state = med.get('state', 'NA')

        check_sql = text("""
            SELECT * FROM local_dev.dim_ce_medicaid 
            WHERE id340b = :id340b AND ce_id = :ce_id 
            AND medicaid_number = :medicaid_number AND state = :state 
            AND current_version_flg = 'Y'
        """)

        try:
            result = conn.execute(check_sql, {
                "id340b": id340b_value,
                "ce_id": ce_id_value,
                "medicaid_number": medicaid_number,
                "state": state
            })
            existing_record = result.fetchone()
        except Exception as e:
            logging.error(f"Error checking existing Medicaid record: {e}")
            continue

        if existing_record is None:
            insert_sql = text("""
                INSERT INTO local_dev.dim_ce_medicaid (
                    id340b, ce_id, medicaid_number, state, effective_stp, expiration_stp, 
                    current_version_flg, change_type
                ) VALUES (
                    :id340b, :ce_id, :medicaid_number, :state, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'INSERT'
                )
            """)

            try:
                conn.execute(insert_sql, {
                    "id340b": id340b_value,
                    "ce_id": ce_id_value,
                    "medicaid_number": medicaid_number,
                    "state": state
                })
                logging.info(f"Inserted new Medicaid number: {medicaid_number} ({state})")
            except Exception as e:
                logging.error(f"Error inserting Medicaid record: {e}")
        else:
            logging.info(f"No changes detected for Medicaid number: {medicaid_number} ({state})")

    try:
        conn.commit()
        logging.info("Database commit successful.")
    except Exception as e:
        logging.error(f"Error committing transaction: {e}")


def process_dim_ce_npi(data, conn):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        id340b_value = data['id340B']
        ce_id_value = str(data['ceId'])
        npi_numbers = data.get('npiNumbers', [])

        check_sql = text("""
            SELECT npi, state FROM local_dev.dim_ce_npi 
            WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
        """)

        existing_records = conn.execute(check_sql, {"id340b": id340b_value, "ce_id": ce_id_value}).fetchall()
        existing_npi_set = {(row.npi, row.state) for row in existing_records}
        incoming_npi_set = {(npi['npiNumber'], npi.get('state', 'NA')) for npi in npi_numbers}

        deleted_npi = existing_npi_set - incoming_npi_set

        if deleted_npi:
            update_deleted_sql = text("""
                UPDATE local_dev.dim_ce_npi
                SET expiration_stp = CURRENT_TIMESTAMP, 
                    current_version_flg = 'N',
                    change_type = 'DELETE'
                WHERE id340b = :id340b AND ce_id = :ce_id 
                AND (npi, state) IN :deleted_npi
            """)
            conn.execute(update_deleted_sql, {
                "id340b": id340b_value, 
                "ce_id": ce_id_value, 
                "deleted_npi": tuple(deleted_npi)
            })
            logging.info(f"Marked {len(deleted_npi)} NPI numbers as deleted.")

        for npi in npi_numbers:
            npi_value = npi['npiNumber']
            state = npi.get('state', 'NA')

            check_sql = text("""
                SELECT * FROM local_dev.dim_ce_npi 
                WHERE id340b = :id340b AND ce_id = :ce_id 
                AND npi = :npi AND state = :state 
                AND current_version_flg = 'Y'
            """)

            result = conn.execute(check_sql, {
                "id340b": id340b_value,
                "ce_id": ce_id_value,
                "npi": npi_value,
                "state": state
            })
            existing_record = result.fetchone()

            if existing_record is None:
                insert_sql = text("""
                    INSERT INTO local_dev.dim_ce_npi (
                        id340b, ce_id, npi, state, effective_stp, expiration_stp, 
                        current_version_flg, change_type
                    ) VALUES (
                        :id340b, :ce_id, :npi, :state, CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'INSERT'
                    )
                """)

                conn.execute(insert_sql, {
                    "id340b": id340b_value,
                    "ce_id": ce_id_value,
                    "npi": npi_value,
                    "state": state
                })

                logging.info(f"Inserted new NPI: {npi_value} ({state})")
            else:
                logging.info(f"No changes detected for NPI: {npi_value} ({state})")

        conn.commit()
    except Exception as e:
        logging.error(f"Error processing dim_ce_npi: {e}")
        conn.rollback()


def process_dim_ce_street_address(data, conn):
    """Updates or inserts a record in the dim_ce_street_address table."""
    id340b_value = data["id340B"]
    ce_id = str(data["ceId"])

    # Extract street address values from JSON
    address_line1 = data["streetAddress"]["addressLine1"]
    address_line2 = data["streetAddress"].get("addressLine2", "N/A")
    city = data["streetAddress"]["city"]
    state = data["streetAddress"]["state"]
    zip_code = data["streetAddress"]["zip"]

    check_sql = text("""
        SELECT * FROM local_dev.dim_ce_street_address 
        WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
    """)

    result = conn.execute(check_sql, {"id340b": id340b_value, "ce_id": ce_id})
    existing_record = result.fetchone()

    if existing_record is None:
        # INSERT - New record
        insert_sql = text("""
            INSERT INTO local_dev.dim_ce_street_address (
                id340b, ce_id, address_line1, address_line2, city, state, zip, 
                effective_stp, expiration_stp, current_version_flg, change_type
            ) VALUES (
                :id340b, :ce_id, :address_line1, :address_line2, :city, :state, :zip,
                CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'INSERT'
            )
        """)

        conn.execute(insert_sql, {
            "id340b": id340b_value,
            "ce_id": ce_id,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": city,
            "state": state,
            "zip": zip_code
        })
        print("New record inserted successfully!")
    else:
        # Check if data has changed
        if (existing_record.address_line1 != address_line1 or 
            existing_record.address_line2 != address_line2 or 
            existing_record.city != city or 
            existing_record.state != state or 
            existing_record.zip != zip_code):

            update_old_sql = text("""
                UPDATE local_dev.dim_ce_street_address
                SET expiration_stp = CURRENT_TIMESTAMP, 
                    current_version_flg = 'N'
                WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
            """)

            conn.execute(update_old_sql, {"id340b": id340b_value, "ce_id": ce_id})

            insert_new_sql = text("""
                INSERT INTO local_dev.dim_ce_street_address (
                    id340b, ce_id, address_line1, address_line2, city, state, zip, 
                    effective_stp, expiration_stp, current_version_flg, change_type
                ) VALUES (
                    :id340b, :ce_id, :address_line1, :address_line2, :city, :state, :zip,
                    CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'UPDATE'
                )
            """)

            conn.execute(insert_new_sql, {
                "id340b": id340b_value,
                "ce_id": ce_id,
                "address_line1": address_line1,
                "address_line2": address_line2,
                "city": city,
                "state": state,
                "zip": zip_code
            })
            print("Record updated - old version expired and new version inserted!")
        else:
            print("No changes detected - record remains the same")

    conn.commit()


def process_dim_ce_billing_address(data, conn):
    """
    Processes and inserts/updates the billing address record in the dim_ce_billing_address table.

    Args:
        data (dict): JSON document containing billing address details.
        conn (sqlalchemy.engine.base.Connection): Active database connection.

    Returns:
        None
    """

    # Extract key values
    id340b_value = data.get("id340B")
    ce_id_value = str(data.get("ceId", ""))  # Ensure it's a string

    # Extract billing address details with defaults
    billing_address = data.get("billingAddress", {})
    organization = billing_address.get("organization", "N/A")
    address_line1 = billing_address.get("addressLine1", "N/A")
    address_line2 = billing_address.get("addressLine2", "N/A")
    city = billing_address.get("city", "N/A")
    state = billing_address.get("state", "N/A")
    zip_code = billing_address.get("zip", "N/A")

    # SQL to check if the record exists
    check_sql = text("""
        SELECT organization, address_line1, address_line2, city, state, zip 
        FROM local_dev.dim_ce_billing_address 
        WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
    """)

    result = conn.execute(check_sql, {"id340b": id340b_value, "ce_id": ce_id_value})
    existing_record = result.fetchone()

    if existing_record is None:
        # New record insertion
        insert_sql = text("""
            INSERT INTO local_dev.dim_ce_billing_address (
                id340b, ce_id, organization, address_line1, address_line2, city, state, zip, 
                effective_stp, expiration_stp, current_version_flg, change_type
            ) VALUES (
                :id340b, :ce_id, :organization, :address_line1, :address_line2, :city, :state, :zip,
                CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'INSERT'
            )
        """)

        conn.execute(insert_sql, {
            "id340b": id340b_value,
            "ce_id": ce_id_value,
            "organization": organization,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": city,
            "state": state, 
            "zip": zip_code
        })

        print("New billing address inserted successfully!")

    else:
        # Check if data has changed
        if (existing_record.organization != organization or
            existing_record.address_line1 != address_line1 or 
            existing_record.address_line2 != address_line2 or 
            existing_record.city != city or 
            existing_record.state != state or 
            existing_record.zip != zip_code):

            # Expire the current record
            update_old_sql = text("""
                UPDATE local_dev.dim_ce_billing_address
                SET expiration_stp = CURRENT_TIMESTAMP, 
                    current_version_flg = 'N'
                WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
            """)

            conn.execute(update_old_sql, {"id340b": id340b_value, "ce_id": ce_id_value})

            # Insert new version of the record
            insert_new_sql = text("""
                INSERT INTO local_dev.dim_ce_billing_address (
                    id340b, ce_id, organization, address_line1, address_line2, city, state, zip, 
                    effective_stp, expiration_stp, current_version_flg, change_type
                ) VALUES (
                    :id340b, :ce_id, :organization, :address_line1, :address_line2, :city, :state, :zip,
                    CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'UPDATE'
                )
            """)

            conn.execute(insert_new_sql, {
                "id340b": id340b_value,
                "ce_id": ce_id_value,
                "organization": organization,
                "address_line1": address_line1,
                "address_line2": address_line2,
                "city": city,
                "state": state,
                "zip": zip_code
            })

            print("Billing address updated - old version expired and new version inserted!")
        else:
            print("No changes detected - record remains the same")

    conn.commit()


def process_dim_ce_shipping_address(data, conn):
    """
    Processes and updates the shipping addresses in the dim_ce_shipping_address table.

    Args:
        data (dict): JSON document containing shipping address details.
        conn (sqlalchemy.engine.base.Connection): Active database connection.

    Returns:
        None
    """
    # Extract key values
    id340b_value = data.get("id340B")
    ce_id_value = str(data.get("ceId", ""))

    # Extract shipping addresses from JSON
    new_shipping_addresses = {
        (addr["addressLine1"], addr["city"], addr["state"], addr["zip"],
         "Y" if addr.get("is340BStreetAddress", False) else "N")
        for addr in data.get("shippingAddresses", [])
    }

    # Fetch existing records from DB
    check_sql = text("""
        SELECT address_line1, city, state, zip, is_340b_street_address_flg
        FROM local_dev.dim_ce_shipping_address 
        WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
    """)

    result = conn.execute(check_sql, {"id340b": id340b_value, "ce_id": ce_id_value})
    existing_addresses = {
        (row.address_line1, row.city, row.state, row.zip, row.is_340b_street_address_flg)
        for row in result
    }

    addresses_to_insert = new_shipping_addresses - existing_addresses
    addresses_to_delete = existing_addresses - new_shipping_addresses

    # Mark removed addresses as inactive
    for addr in addresses_to_delete:
        update_old_sql = text("""
            UPDATE local_dev.dim_ce_shipping_address
            SET expiration_stp = CURRENT_TIMESTAMP, 
                current_version_flg = 'N', 
                change_type = 'DELETE'
            WHERE id340b = :id340b AND ce_id = :ce_id AND address_line1 = :address_line1 
              AND city = :city AND state = :state AND zip = :zip
        """)
        conn.execute(update_old_sql, {
            "id340b": id340b_value, "ce_id": ce_id_value,
            "address_line1": addr[0], "city": addr[1], "state": addr[2], "zip": addr[3]
        })
        logger.info(f"Marked address {addr} as deleted.")

    # Insert new addresses
    for addr in addresses_to_insert:
        insert_sql = text("""
            INSERT INTO local_dev.dim_ce_shipping_address (
                id340b, ce_id, is_340b_street_address_flg, address_line1, city, state, zip, 
                effective_stp, expiration_stp, current_version_flg, change_type
            ) VALUES (
                :id340b, :ce_id, :is_340b_street_address, :address_line1, :city, :state, :zip,
                CURRENT_TIMESTAMP, '9999-12-31 23:59:59', 'Y', 'INSERT'
            )
        """)
        conn.execute(insert_sql, {
            "id340b": id340b_value, "ce_id": ce_id_value,
            "is_340b_street_address": addr[4], "address_line1": addr[0],
            "city": addr[1], "state": addr[2], "zip": addr[3]
        })
        logger.info(f"Inserted new shipping address {addr}.")

    conn.commit()
    logger.info("Shipping addresses processed successfully.")


def process_dim_ce(data, conn):
    try:
        # Safely extract values with default handling
        params = {
            "id340b": data.get('id340B', ''),
            "ce_id": str(data.get('ceId', '')),
            "name": data.get('name', ''),
            "subname": data.get('subName', ''),
            "participating_flg": (
                'Y' if data.get('participating') == 'TRUE' 
                else 'N' if data.get('participating') == 'FALSE' 
                else '-'
            ),
            "participating_start_stp": data.get('participatingStartDate', None),
            "grant_number": data.get('grantNumber', ''),
            "cert_decert_stp": data.get('certifiedDecertifiedDate', None),
            "edit_stp": data.get('editDate', None),
            "auth_official_name": data.get('authorizingOfficial', {}).get('name', ''),
            "auth_official_title": data.get('authorizingOfficial', {}).get('title', ''),
            "auth_official_phone_number": data.get('authorizingOfficial', {}).get('phoneNumber', ''),
            "primary_contact_name": data.get('primaryContact', {}).get('name', ''),
            "primary_contact_title": data.get('primaryContact', {}).get('title', ''),
            "primary_contact_phone_number": data.get('primaryContact', {}).get('phoneNumber', ''),
            "primary_contact_phone_ext": data.get('primaryContact', {}).get('phoneNumberExtension', '')
        }

        # Define insert_sql outside of conditional blocks
        insert_sql = text("""
            INSERT INTO local_dev.dim_ce (
                id340b, ce_id, name, subname, participating_flg, 
                participating_start_stp, 
                grant_number, cert_decert_stp, edit_stp, auth_official_name, 
                auth_official_title, auth_official_phone_number,
                 primary_contact_name, 
                primary_contact_title, primary_contact_phone_number, 
                primary_contact_phone_ext, effective_stp, expiration_stp, 
                current_version_flg, change_type
            ) VALUES (
                :id340b, :ce_id, :name, :subname, :participating_flg,
                 :participating_start_stp, 
                :grant_number, :cert_decert_stp, :edit_stp,
                 :auth_official_name, 
                :auth_official_title, :auth_official_phone_number,
                 :primary_contact_name, 
                :primary_contact_title,
                 :primary_contact_phone_number, 
                :primary_contact_phone_ext, CURRENT_TIMESTAMP, 
                '9999-12-31 23:59:59', 'Y', 'INSERT'
            )
        """)

        # Check existing record
        check_sql = text("""
            SELECT * FROM local_dev.dim_ce
            WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
        """)
        result = conn.execute(check_sql, params)
        existing_record = result.fetchone()

        # Insert or update logic
        if existing_record is None:
            conn.execute(insert_sql, params)
            logger.info(f"New record for id340b {params['id340b']} inserted successfully!")
        else:
            # Compare existing record with new data
            if any([
                existing_record.name != params['name'],
                existing_record.subname != params['subname'],
                existing_record.participating_flg != params['participating_flg'],
                existing_record.grant_number != params['grant_number'],
                existing_record.auth_official_name != params['auth_official_name'],
                existing_record.primary_contact_name != params['primary_contact_name']
            ]):
                update_old_sql = text("""
                    UPDATE local_dev.dim_ce
                    SET expiration_stp = CURRENT_TIMESTAMP, 
                        current_version_flg = 'N'
                    WHERE id340b = :id340b AND ce_id = :ce_id AND current_version_flg = 'Y'
                """)
                conn.execute(update_old_sql, params)
                conn.execute(insert_sql, params)
                logger.info(f"Record for id340b {params['id340b']} updated - old version expired and new version inserted!")
            else:
                logger.info(f"No changes detected for id340b {params['id340b']} - record remains the same")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing record for id340b {params['id340b']}: {e}")
        raise
