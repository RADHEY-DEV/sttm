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
    st.title("Data Mapping Tool")
    st.markdown(
        """
        **Excel-like Input**: Define source, target, and mapping values in one interface.
        - **CRUD Operations**: Add, edit, delete mappings with validation.
        - **Automatic ID Generation**: Ensures unique entries for all mappings and transformations.
        """
    )

    # Fetch existing mappings
    existing_mappings_query = """
    SELECT
        sm.Schema_Name AS Source_Schema, sm.Table_Name AS Source_Table, sm.Field_Name AS Source_Field,
        sm.Business_Name, sm.Business_Description, sm.Data_Type AS Source_Data_Type,
        tm.Schema_Name AS Target_Schema, tm.Table_Name AS Target_Table, tm.Field_Name AS Target_Field,
        tm.Field_Description, tm.Data_Type AS Target_Data_Type, tm.Data_Quality_Info, tm.Primary_Key, tm.Mandatory,
        stm.Transformation_Rule_ID, tm.Join_Clause
    FROM SourceToTargetMapping stm
    JOIN SourceMapping sm ON sm.SourceID = stm.SourceID
    JOIN TargetMapping tm ON tm.TargetID = stm.TargetID
    """
    existing_mappings = fetch_data(existing_mappings_query)
    st.dataframe(existing_mappings)

    # Excel-like input grid
    st.subheader("Add or Update Mapping")
    num_rows = st.number_input("Number of rows to add", min_value=1, value=1)

    # Create empty data entry rows
    data = {
        "Source_Schema": [""] * num_rows,
        "Source_Table": [""] * num_rows,
        "Source_Field": [""] * num_rows,
        "Business_Name": [""] * num_rows,
        "Business_Description": [""] * num_rows,
        "Source_Data_Type": [""] * num_rows,
        "Target_Schema": [""] * num_rows,
        "Target_Table": [""] * num_rows,
        "Target_Field": [""] * num_rows,
        "Field_Description": [""] * num_rows,
        "Target_Data_Type": [""] * num_rows,
        "Data_Quality_Info": [""] * num_rows,
        "Primary_Key": [""] * num_rows,
        "Mandatory": [""] * num_rows,
        "Join_Clause": [""] * num_rows,
        "Transformation_Rule_ID": [""] * num_rows,
    }

    df = pd.DataFrame(data)
    edited_df = st.data_editor(df, num_rows="dynamic")

    if st.button("Submit Data"):
        for _, row in edited_df.iterrows():
            # Validate input
            required_fields = [
                row.Source_Schema, row.Source_Table, row.Source_Field, row.Business_Name, row.Business_Description,
                row.Source_Data_Type, row.Target_Schema, row.Target_Table, row.Target_Field, row.Field_Description,
                row.Target_Data_Type, row.Data_Quality_Info
            ]
            if not all(required_fields):
                st.error("All fields must be filled for source and target mappings.")
                continue

            # Generate IDs
            source_id = generate_sha256(row.Source_Schema, row.Source_Table, row.Source_Field)
            target_id = generate_sha256(row.Target_Schema, row.Target_Table, row.Target_Field)
            mapping_id = generate_sha256(source_id, target_id)
            rule_id = row.Transformation_Rule_ID

            # Insert SourceMapping
            source_query = """
            IF NOT EXISTS (SELECT 1 FROM SourceMapping WHERE SourceID = ?)
            INSERT INTO SourceMapping (SourceID, Schema_Name, Table_Name, Field_Name, Business_Name, Business_Description, Data_Type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            source_params = [
                source_id,  # First `?` - SourceID check
                source_id,  # Second `?` - SourceID for insert
                row["Source_Schema"],
                row["Source_Table"],
                row["Source_Field"],
                row["Business_Name"],
                row["Business_Description"],
                row["Source_Data_Type"]
            ]
            execute_query(source_query, source_params)

            # Insert TargetMapping
            target_query = """
            IF NOT EXISTS (SELECT 1 FROM TargetMapping WHERE TargetID = ?)
            INSERT INTO TargetMapping (TargetID, Schema_Name, Table_Name, Field_Name, Field_Description, Data_Type, Data_Quality_Info, Primary_Key, Mandatory, Join_Clause)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            target_params = [
                target_id,  # First `?` - TargetID check
                target_id,  # Second `?` - TargetID for insert
                row["Target_Schema"],
                row["Target_Table"],
                row["Target_Field"],
                row["Field_Description"],
                row["Target_Data_Type"],
                row["Data_Quality_Info"],
                int(row.get("Primary_Key", 0)),  # Ensure Primary_Key is an integer
                int(row.get("Mandatory", 0)),   # Ensure Mandatory is an integer
                row["Join_Clause"]
            ]
            execute_query(target_query, target_params)

            # Insert SourceToTargetMapping
            mapping_query = """
            IF NOT EXISTS (SELECT 1 FROM SourceToTargetMapping WHERE MappingID = ?)
            INSERT INTO SourceToTargetMapping (MappingID, SourceID, TargetID, Transformation_Rule_ID)
            VALUES (?, ?, ?, ?)
            """
            mapping_params = [
                mapping_id,  # First `?` - MappingID check
                mapping_id,  # Second `?` - MappingID for insert
                source_id,
                target_id,
                rule_id
            ]
            execute_query(mapping_query, mapping_params)

        st.success("Data submitted successfully!")

if __name__ == "__main__":
    main()
