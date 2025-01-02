import hashlib
import pandas as pd
import streamlit as st
import pyodbc

# Database Connection
def get_db_connection():
    connection = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=edf2.database.windows.net;"
        "DATABASE=edf;"
        "UID=radheybaps;"
        "PWD=kAlAm@100;"
    )
    return connection

# Utility Functions
def generate_sha256(*args):
    """Generate SHA256 hash from provided arguments."""
    concatenated = ''.join(str(arg) for arg in args)
    return hashlib.sha256(concatenated.encode()).hexdigest()

def fetch_data(query):
    """Fetch data from the database."""
    conn = get_db_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()

def execute_query(query, params=None):
    """Execute a query in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query, params or ())
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        conn.close()

# Streamlit App
def main():
    st.set_page_config(layout="wide")
    st.title("Editable Data Mapping Tool")

    # Fetch existing mappings
    query = """
    SELECT
        sm.Schema_Name AS Source_Schema, sm.Table_Name AS Source_Table, sm.Field_Name AS Source_Field,
        sm.Business_Name, sm.Business_Description, sm.Data_Type AS Source_Data_Type,
        tm.Schema_Name AS Target_Schema, tm.Table_Name AS Target_Table, tm.Field_Name AS Target_Field,
        tm.Field_Description, tm.Data_Type AS Target_Data_Type, tm.Data_Quality_Info, tm.Primary_Key, tm.Mandatory,
        stm.Transformation_Rule_ID
    FROM SourceToTargetMapping stm
    JOIN SourceMapping sm ON sm.SourceID = stm.SourceID
    JOIN TargetMapping tm ON tm.TargetID = stm.TargetID
    """
    existing_mappings = fetch_data(query)

    # Display read-only table
    st.subheader("Existing Mappings (Read-Only)")
    st.dataframe(existing_mappings, use_container_width=True, height=400)

    # Editable table
    st.subheader("Edit Mappings")
    edited_df = st.data_editor(existing_mappings, num_rows="dynamic", key="editable_table")

    # Save changes
    if st.button("Save Changes"):
        for _, row in edited_df.iterrows():
            # Generate IDs dynamically based on the current values
            source_id = generate_sha256(row.Source_Schema, row.Source_Table, row.Source_Field)
            target_id = generate_sha256(row.Target_Schema, row.Target_Table, row.Target_Field)
            mapping_id = generate_sha256(source_id, target_id)

            # Update SourceMapping only if the values changed
            source_query = """
            IF NOT EXISTS (SELECT 1 FROM SourceMapping WHERE SourceID = ?)
            INSERT INTO SourceMapping (SourceID, Schema_Name, Table_Name, Field_Name, Business_Name, Business_Description, Data_Type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ELSE
            UPDATE SourceMapping
            SET Schema_Name = ?, Table_Name = ?, Field_Name = ?, Business_Name = ?, Business_Description = ?, Data_Type = ?
            WHERE SourceID = ?
            """
            source_params = [
                source_id,  # Check SourceID
                source_id, row.Source_Schema, row.Source_Table, row.Source_Field, row.Business_Name, row.Business_Description, row.Source_Data_Type,
                row.Source_Schema, row.Source_Table, row.Source_Field, row.Business_Name, row.Business_Description, row.Source_Data_Type, source_id
            ]
            execute_query(source_query, source_params)

            # Update TargetMapping only if the values changed
            target_query = """
            IF NOT EXISTS (SELECT 1 FROM TargetMapping WHERE TargetID = ?)
            INSERT INTO TargetMapping (TargetID, Schema_Name, Table_Name, Field_Name, Field_Description, Data_Type, Data_Quality_Info, Primary_Key, Mandatory)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ELSE
            UPDATE TargetMapping
            SET Schema_Name = ?, Table_Name = ?, Field_Name = ?, Field_Description = ?, Data_Type = ?, Data_Quality_Info = ?, Primary_Key = ?, Mandatory = ?
            WHERE TargetID = ?
            """
            target_params = [
                target_id,  # Check TargetID
                target_id, row.Target_Schema, row.Target_Table, row.Target_Field, row.Field_Description, row.Target_Data_Type,
                row.Data_Quality_Info, row.Primary_Key, row.Mandatory,
                row.Target_Schema, row.Target_Table, row.Target_Field, row.Field_Description, row.Target_Data_Type,
                row.Data_Quality_Info, row.Primary_Key, row.Mandatory, target_id
            ]
            execute_query(target_query, target_params)

            # Update SourceToTargetMapping with MappingID
            mapping_query = """
            IF NOT EXISTS (SELECT 1 FROM SourceToTargetMapping WHERE MappingID = ?)
            INSERT INTO SourceToTargetMapping (MappingID, SourceID, TargetID, Transformation_Rule_ID)
            VALUES (?, ?, ?, ?)
            ELSE
            UPDATE SourceToTargetMapping
            SET Transformation_Rule_ID = ?
            WHERE MappingID = ?
            """
            mapping_params = [
                mapping_id, source_id, target_id, row.Transformation_Rule_ID,
                row.Transformation_Rule_ID, mapping_id
            ]
            execute_query(mapping_query, mapping_params)

        st.success("Changes saved successfully!")

    # Delete rows
    if st.button("Delete Selected Rows"):
        selected_rows = edited_df.loc[edited_df.index.isin(st.session_state.get("editable_table_selected_rows", []))]
        for _, row in selected_rows.iterrows():
            # Generate IDs dynamically
            source_id = generate_sha256(row.Source_Schema, row.Source_Table, row.Source_Field)
            target_id = generate_sha256(row.Target_Schema, row.Target_Table, row.Target_Field)
            mapping_id = generate_sha256(source_id, target_id)

            # Delete from SourceToTargetMapping
            delete_mapping_query = "DELETE FROM SourceToTargetMapping WHERE MappingID = ?"
            execute_query(delete_mapping_query, [mapping_id])

            # Delete from SourceMapping
            delete_source_query = "DELETE FROM SourceMapping WHERE SourceID = ?"
            execute_query(delete_source_query, [source_id])

            # Delete from TargetMapping
            delete_target_query = "DELETE FROM TargetMapping WHERE TargetID = ?"
            execute_query(delete_target_query, [target_id])

        st.success("Selected rows deleted successfully!")

if __name__ == "__main__":
    main()
