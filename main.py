import streamlit as st
import pandas as pd
from db import fetch_data, insert_mapping, update_mapping, delete_mapping, is_duplicate

st.set_page_config(page_title="Data Mapping", layout="wide")
st.title("Data Mapping Tool")

tab1, tab2, tab3, tab4 = st.tabs(["Create", "Read", "Update", "Delete"])

KEY_FIELDS = ["malcode", "schema_name", "table_name", "field_name", "target_table_name", "target_field_name"]

with tab1:
    st.header("Create Data Mapping")

    entry_mode = st.radio("Select Entry Mode:", ("Form", "Data Editor"))

    if entry_mode == "Form":
        with st.form("create_form", clear_on_submit=True):
            malcode = st.text_input("Malcode")
            schema_name = st.text_input("Schema Name")
            table_name = st.text_input("Table Name")
            field_name = st.text_input("Field Name")
            source_field_description = st.text_input("Source Field Description")
            source_business_name = st.text_input("Source Business Name")
            source_business_description = st.text_area("Source Business Description")
            source_data_type = st.text_input("Source Data Type")
            target_table_name = st.text_input("Target Table Name")
            target_field_name = st.text_input("Target Field Name")
            target_field_description = st.text_input("Target Field Description")
            target_business_name = st.text_input("Target Business Name")
            target_business_description = st.text_area("Target Business Description")
            target_data_type = st.text_input("Target Data Type")
            data_quality_info = st.text_input("Data Quality Info")
            primary_key = st.checkbox("Primary Key")
            mandatory = st.checkbox("Mandatory")
            transformation_rule_id = st.text_input("Transformation Rule ID")
            transformation_description = st.text_area("Transformation Description")
            join_clause = st.text_area("Join Clause")

            submitted = st.form_submit_button("Add Record")

            if submitted:
                new_record = {
                    "malcode": malcode, "schema_name": schema_name, "table_name": table_name, "field_name": field_name,
                    "source_field_description": source_field_description, "source_business_name": source_business_name,
                    "source_business_description": source_business_description, "source_data_type": source_data_type,
                    "target_table_name": target_table_name, "target_field_name": target_field_name,
                    "target_field_description": target_field_description, "target_business_name": target_business_name,
                    "target_business_description": target_business_description, "target_data_type": target_data_type,
                    "data_quality_info": data_quality_info, "primary_key": primary_key, "mandatory": mandatory,
                    "transformation_rule_id": transformation_rule_id, "transformation_description": transformation_description,
                    "join_clause": join_clause
                }

                if is_duplicate(new_record):
                    st.error("Duplicate record found. Please modify the data.")
                else:
                    insert_mapping(new_record)
                    st.success("Record added successfully.")
    else:

        st.write("### Enter Data Below")

        template_data = pd.DataFrame(columns=[
            "malcode", "schema_name", "table_name", "field_name",
            "source_field_description", "source_business_name", "source_business_description", "source_data_type",
            "target_table_name", "target_field_name", "target_field_description", "target_business_name",
            "target_business_description", "target_data_type", "data_quality_info", "primary_key", "mandatory",
            "transformation_rule_id", "transformation_description", "join_clause"
        ])

        edited_data = st.data_editor(template_data, num_rows="dynamic")

        if st.button("Add Records"):
            for _, row in edited_data.iterrows():
                new_record = row.to_dict()
                if is_duplicate(new_record):
                    st.warning(f"Duplicate record skipped: {new_record}")
                else:
                    insert_mapping(new_record)
            st.success("Records processed successfully.")

with tab2:
    st.header("Read Data Mapping")

    data = fetch_data()
    if data:
        st.dataframe(pd.DataFrame(data))
    else:
        st.warning("No records found.")

with tab3:
    st.header("Update Data Mapping")

    data = fetch_data()
    if not data:
        st.warning("No data available to update.")
    else:
        df = pd.DataFrame(data)
        record_id = st.selectbox("Select Record to Update:", df["id"])
        selected_record = df[df["id"] == record_id].iloc[0]

        with st.form("update_form"):
            malcode = st.text_input("Malcode", value=selected_record["malcode"])
            schema_name = st.text_input("Schema Name", value=selected_record["schema_name"])
            table_name = st.text_input("Table Name", value=selected_record["table_name"])
            field_name = st.text_input("Field Name", value=selected_record["field_name"])
            source_field_description = st.text_input("Source Field Description", value=selected_record["source_field_description"])
            source_business_name = st.text_input("Source Business Name", value=selected_record["source_business_name"])
            source_business_description = st.text_area("Source Business Description", value=selected_record["source_business_description"])
            source_data_type = st.text_input("Source Data Type", value=selected_record["source_data_type"])
            target_table_name = st.text_input("Target Table Name", value=selected_record["target_table_name"])
            target_field_name = st.text_input("Target Field Name", value=selected_record["target_field_name"])
            target_field_description = st.text_input("Target Field Description", value=selected_record["target_field_description"])
            target_business_name = st.text_input("Target Business Name", value=selected_record["target_business_name"])
            target_business_description = st.text_area("Target Business Description", value=selected_record["target_business_description"])
            target_data_type = st.text_input("Target Data Type", value=selected_record["target_data_type"])
            data_quality_info = st.text_input("Data Quality Info", value=selected_record["data_quality_info"])
            primary_key = st.checkbox("Primary Key", value=selected_record["primary_key"])
            mandatory = st.checkbox("Mandatory", value=selected_record["mandatory"])
            transformation_rule_id = st.text_input("Transformation Rule ID", value=selected_record["transformation_rule_id"])
            transformation_description = st.text_area("Transformation Description", value=selected_record["transformation_description"])
            join_clause = st.text_area("Join Clause", value=selected_record["join_clause"])

            submitted = st.form_submit_button("Update Record")

            if submitted:
                updated_record = {
                    "malcode": malcode, "schema_name": schema_name, "table_name": table_name, "field_name": field_name,
                    "source_field_description": source_field_description, "source_business_name": source_business_name,
                    "source_business_description": source_business_description, "source_data_type": source_data_type,
                    "target_table_name": target_table_name, "target_field_name": target_field_name,
                    "target_field_description": target_field_description, "target_business_name": target_business_name,
                    "target_business_description": target_business_description, "target_data_type": target_data_type,
                    "data_quality_info": data_quality_info, "primary_key": primary_key, "mandatory": mandatory,
                    "transformation_rule_id": transformation_rule_id, "transformation_description": transformation_description,
                    "join_clause": join_clause
                }
                update_mapping(record_id, updated_record)
                st.success("Record updated successfully.")

with tab4:
    st.header("Delete Data Mapping")

    data = fetch_data()
    if not data:
        st.warning("No data available to delete.")
    else:
        df = pd.DataFrame(data)
        record_id = st.selectbox("Select Record to Delete:", df["id"])
        if st.button("Delete Record"):
            delete_mapping(record_id)
            st.success("Record deleted successfully.")
