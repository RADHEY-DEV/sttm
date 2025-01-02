import pyodbc
from datetime import datetime

# Set up the database connection (change your connection string as needed)
def get_connection():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=edf2.database.windows.net;DATABASE=edf;UID=radheybaps;PWD=kAlAm@100')
    return conn

# Function to fetch the current (active) data
def fetch_data():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM data_mapping WHERE is_current = 1")
    rows = cursor.fetchall()
    data = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    cursor.close()
    conn.close()
    return data

# Check for duplicate based on key columns
def is_duplicate(data):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM data_mapping
        WHERE is_current = 1 AND
              malcode = ? AND schema_name = ? AND table_name = ? AND field_name = ? AND
              target_table_name = ? AND target_field_name = ?
        """,
        data['malcode'], data['schema_name'], data['table_name'], data['field_name'],
        data['target_table_name'], data['target_field_name']
    )
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result > 0

# Insert new mapping into the table
def insert_mapping(data):
    if is_duplicate(data):
        raise ValueError("Duplicate entry based on key columns detected.")

    conn = get_connection()
    cursor = conn.cursor()

    # Insert the new record with is_current = 1 and valid_from = current timestamp
    cursor.execute(
        """
        INSERT INTO data_mapping (
            malcode, schema_name, table_name, field_name,
            source_field_description, source_business_name,
            source_business_description, source_data_type,
            target_table_name, target_field_name, target_field_description, target_business_name,
            target_business_description, target_data_type,
            data_quality_info, primary_key, mandatory,
            transformation_rule_id, transformation_description,
            join_clause, valid_from, is_current
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 1)
        """,
        data['malcode'], data['schema_name'], data['table_name'], data['field_name'],
        data['source_field_description'], data['source_business_name'],
        data['source_business_description'], data['source_data_type'],
        data['target_table_name'], data['target_field_name'],
        data['target_field_description'], data['target_business_name'],
        data['target_business_description'], data['target_data_type'],
        data['data_quality_info'], data['primary_key'], data['mandatory'],
        data['transformation_rule_id'], data['transformation_description'],
        data['join_clause']
    )

    conn.commit()
    cursor.close()
    conn.close()

# Update an existing mapping (SCD Type 2 approach: soft delete current record, add new one)
def update_mapping(mapping_id, data):
    if is_duplicate(data):
        raise ValueError("Duplicate entry based on key columns detected.")

    conn = get_connection()
    cursor = conn.cursor()

    # Mark the old record as non-current and set valid_to to the current time
    cursor.execute(
        """
        UPDATE data_mapping
        SET is_current = 0, valid_to = GETDATE()
        WHERE id = ? AND is_current = 1
        """,
        mapping_id
    )

    # Insert a new record as a current one with updated data
    cursor.execute(
        """
        INSERT INTO data_mapping (
            malcode, schema_name, table_name, field_name,
            source_field_description, source_business_name,
            source_business_description, source_data_type,
            target_table_name, target_field_name, target_field_description, target_business_name,
            target_business_description, target_data_type,
            data_quality_info, primary_key, mandatory,
            transformation_rule_id, transformation_description,
            join_clause, valid_from, is_current
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 1)
        """,
        data['malcode'], data['schema_name'], data['table_name'], data['field_name'],
        data['source_field_description'], data['source_business_name'],
        data['source_business_description'], data['source_data_type'],
        data['target_table_name'], data['target_field_name'],
        data['target_field_description'], data['target_business_name'],
        data['target_business_description'], data['target_data_type'],
        data['data_quality_info'], data['primary_key'], data['mandatory'],
        data['transformation_rule_id'], data['transformation_description'],
        data['join_clause']
    )

    conn.commit()
    cursor.close()
    conn.close()

# Soft delete a mapping by marking it as non-current
def delete_mapping(mapping_id):
    conn = get_connection()
    cursor = conn.cursor()

    # Mark the record as non-current
    cursor.execute(
        """
        UPDATE data_mapping
        SET is_current = 0, valid_to = GETDATE()
        WHERE id = ? AND is_current = 1
        """,
        mapping_id
    )

    conn.commit()
    cursor.close()
    conn.close()
