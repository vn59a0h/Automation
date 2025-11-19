import streamlit as st
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="SAP Ingestion", page_icon="📋", layout="wide")

st.title("📋 SAP Data Ingestion - CSV Manager")

# Define required columns
REQUIRED_COLUMNS = [
    'icdsTableName',
    'BANNER_NAME',
    'dataSensitivity',
    'dlSchemaName',
    'dlTableName',
    'tableLoadType'
]

# Initialize session state
if 'ingestion_data' not in st.session_state:
    st.session_state.ingestion_data = pd.DataFrame(columns=REQUIRED_COLUMNS)

if 'selected_rows' not in st.session_state:
    st.session_state.selected_rows = set()

if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False

if 'show_add_form' not in st.session_state:
    st.session_state.show_add_form = False

if 'edit_row_index' not in st.session_state:
    st.session_state.edit_row_index = None

# Helper function - validate row
def validate_row(row):
    """Check if row has all required fields"""
    errors = []
    for col in REQUIRED_COLUMNS:
        value = row.get(col)
        if pd.isna(value) or value == '' or value is None:
            errors.append(col)
    return errors

st.markdown("---")

st.subheader("📊 Your Ingestion Table")
st.caption("⚠️ All fields are mandatory. SAP table names must be unique.")

# Only show table if not in edit mode
if not st.session_state.edit_mode:
    # Create table with select column
    df_to_edit = st.session_state.ingestion_data.copy()
    df_to_edit.insert(0, '☑️ Select', False)

    # Set checkboxes for selected rows
    if st.session_state.selected_rows and len(st.session_state.selected_rows) > 0:
        for idx in st.session_state.selected_rows:
            if idx < len(df_to_edit):
                df_to_edit.at[idx, '☑️ Select'] = True

    # Disable all columns except checkbox
    disabled_columns = [col for col in df_to_edit.columns if col != '☑️ Select']

    # Editable dataframe
    edited_df = st.data_editor(
        df_to_edit,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=disabled_columns,
        key="main_editor",
        column_config={
            "☑️ Select": st.column_config.CheckboxColumn(
                "☑️ Select",
                help="Select rows to edit/delete",
                width=80,
            ),
            "icdsTableName": st.column_config.TextColumn(
                "🏢 SAP Table Name *",
                help="e.g., IKPF (REQUIRED - UNIQUE)",
                width=120,
            ),
            "BANNER_NAME": st.column_config.SelectboxColumn(
                "🏷️ Banners *",
                options=['MDD', 'MAK', 'MSB', 'MDD,MAK', 'MDD,MSB', 'MAK,MSB', 'MDD,MAK,MSB'],
                width=130,
            ),
            "dataSensitivity": st.column_config.SelectboxColumn(
                "🔒 Sensitivity *",
                options=['se', 'ns', 'hs'],
                width=100,
            ),
            "dlSchemaName": st.column_config.SelectboxColumn(
                "📦 Schema *",
                options=['sa_mdse_dl_secure', 'sa_mdse_dl_table'],
                width=150,
            ),
            "dlTableName": st.column_config.TextColumn(
                "📝 DL Table Name *",
                help="Lowercase table name (REQUIRED)",
                width=140,
            ),
            "tableLoadType": st.column_config.SelectboxColumn(
                "🔄 Load Type *",
                options=['INC', 'FULL'],
                width=100,
            ),
        }
    )

    # Handle row selection
    if edited_df is not None:
        rows_selected = edited_df[edited_df['☑️ Select'] == True].index.tolist()
        st.session_state.selected_rows = set(rows_selected)
        st.session_state.ingestion_data = edited_df.drop(columns=['☑️ Select']).copy()

st.markdown("---")

# Action buttons
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    if st.button("➕ Add Row", use_container_width=True, key="add_row_btn"):
        st.session_state.show_add_form = True
        st.rerun()

with col2:
    if len(st.session_state.selected_rows) > 0:
        if st.button("✏️ Edit Row", use_container_width=True, key="edit_btn"):
            st.session_state.edit_row_index = list(st.session_state.selected_rows)[0]
            st.session_state.edit_mode = True
            st.rerun()
    else:
        st.button("✏️ Edit Row", use_container_width=True, disabled=True, key="edit_btn_disabled")

with col3:
    if st.session_state.edit_mode:
        if st.button("💾 Save Row", use_container_width=True, type="primary", key="save_btn"):
            st.session_state.edit_mode = False
            st.session_state.selected_rows = set()
            st.session_state.edit_row_index = None
            st.success("✅ Changes saved!")
            st.rerun()
    else:
        st.button("💾 Save Row", use_container_width=True, type="primary", disabled=True, key="save_btn_disabled")

with col4:
    if len(st.session_state.selected_rows) > 0:
        if st.button("🗑️ Delete Row", use_container_width=True, type="secondary", key="delete_btn"):
            deleted_count = len(st.session_state.selected_rows)
            for idx in sorted(st.session_state.selected_rows, reverse=True):
                st.session_state.ingestion_data = st.session_state.ingestion_data.drop(idx).reset_index(drop=True)
            st.session_state.selected_rows = set()
            st.session_state.edit_mode = False
            st.success(f"✅ Deleted {deleted_count} row(s)")
            st.rerun()
    else:
        st.button("🗑️ Delete Row", use_container_width=True, type="secondary", disabled=True, key="delete_btn_disabled")

with col5:
    if st.button("🗑️ Clear All", use_container_width=True, type="secondary", key="clear_btn"):
        st.session_state.ingestion_data = pd.DataFrame(columns=REQUIRED_COLUMNS)
        st.session_state.selected_rows = set()
        st.session_state.edit_mode = False
        st.rerun()

with col6:
    if len(st.session_state.ingestion_data) > 0:
        has_errors = any(validate_row(row) for _, row in st.session_state.ingestion_data.iterrows())
        
        if not has_errors:
            csv_buffer = BytesIO()
            st.session_state.ingestion_data.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            st.download_button(
                label="⬇️ Download",
                data=csv_buffer,
                file_name="ingestion.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.info("ℹ️ Fix errors")
    else:
        st.info("ℹ️ Add rows")

st.markdown("---")

# Edit Row Modal
if st.session_state.edit_mode and st.session_state.edit_row_index is not None:
    st.subheader("✏️ Edit Row")
    
    row_idx = st.session_state.edit_row_index
    row_data = st.session_state.ingestion_data.iloc[row_idx]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        icds_table = st.text_input("🏢 SAP Table Name", value=row_data['icdsTableName'], key="edit_icds_table")
    
    with col2:
        banners_options = ['MDD', 'MAK', 'MSB', 'MDD,MAK', 'MDD,MSB', 'MAK,MSB', 'MDD,MAK,MSB']
        banner_index = banners_options.index(row_data['BANNER_NAME']) if row_data['BANNER_NAME'] in banners_options else 0
        banners = st.selectbox("🏷️ Banners", options=banners_options, index=banner_index, key="edit_banners")
    
    with col3:
        sensitivity_options = ['se', 'ns', 'hs']
        sensitivity_index = sensitivity_options.index(row_data['dataSensitivity']) if row_data['dataSensitivity'] in sensitivity_options else 0
        sensitivity = st.selectbox("🔒 Sensitivity", options=sensitivity_options, index=sensitivity_index, key="edit_sensitivity")
    
    col4, col5, col6, col7 = st.columns([2, 2, 2, 1])
    
    with col4:
        schema_options = ['sa_mdse_dl_secure', 'sa_mdse_dl_table']
        schema_index = schema_options.index(row_data['dlSchemaName']) if row_data['dlSchemaName'] in schema_options else 0
        schema = st.selectbox("📦 Schema", options=schema_options, index=schema_index, key="edit_schema")
    
    with col5:
        table_name = st.text_input("📝 DL Table Name", value=row_data['dlTableName'], key="edit_table_name")
    
    with col6:
        load_type_options = ['INC', 'FULL']
        load_type_index = load_type_options.index(row_data['tableLoadType']) if row_data['tableLoadType'] in load_type_options else 0
        load_type = st.selectbox("🔄 Load Type", options=load_type_options, index=load_type_index, key="edit_load_type")
    
    with col7:
        col_save, col_cancel = st.columns(2)
        with col_save:
            if st.button("✓ Save", use_container_width=True, key="edit_save_btn"):
                icds_table_upper = icds_table.strip().upper()
                
                # Check for duplicate
                other_rows = st.session_state.ingestion_data.drop(row_idx)
                if len(other_rows) > 0 and icds_table_upper in other_rows['icdsTableName'].values:
                    st.error(f"❌ SAP Table Name '{icds_table_upper}' already exists.")
                elif not icds_table or not table_name:
                    st.error("❌ Please fill SAP Table Name and DL Table Name")
                else:
                    updated_data = st.session_state.ingestion_data.copy()
                    updated_data.at[row_idx, 'icdsTableName'] = icds_table_upper
                    updated_data.at[row_idx, 'BANNER_NAME'] = banners
                    updated_data.at[row_idx, 'dataSensitivity'] = sensitivity
                    updated_data.at[row_idx, 'dlSchemaName'] = schema
                    updated_data.at[row_idx, 'dlTableName'] = table_name.strip().lower()
                    updated_data.at[row_idx, 'tableLoadType'] = load_type
                    
                    st.session_state.ingestion_data = updated_data
                    st.session_state.edit_mode = False
                    st.session_state.selected_rows = set()
                    st.session_state.edit_row_index = None
                    st.success("✅ Row updated!")
                    st.rerun()
        
        with col_cancel:
            if st.button("✕ Cancel", use_container_width=True, key="edit_cancel_btn"):
                st.session_state.edit_mode = False
                st.session_state.selected_rows = set()
                st.session_state.edit_row_index = None
                st.rerun()

st.markdown("---")

# Add Row Form
if st.session_state.show_add_form:
    st.subheader("➕ Add New Row")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        icds_table = st.text_input("🏢 SAP Table Name", key="form_icds_table")
    
    with col2:
        banners = st.selectbox("🏷️ Banners", options=['MDD', 'MAK', 'MSB', 'MDD,MAK', 'MDD,MSB', 'MAK,MSB', 'MDD,MAK,MSB'], key="form_banners")
    
    with col3:
        sensitivity = st.selectbox("🔒 Sensitivity", options=['se', 'ns', 'hs'], key="form_sensitivity")
    
    col4, col5, col6, col7 = st.columns([2, 2, 2, 1])
    
    with col4:
        schema = st.selectbox("📦 Schema", options=['sa_mdse_dl_secure', 'sa_mdse_dl_table'], key="form_schema")
    
    with col5:
        table_name = st.text_input("📝 DL Table Name", key="form_table_name")
    
    with col6:
        load_type = st.selectbox("🔄 Load Type", options=['INC', 'FULL'], key="form_load_type")
    
    with col7:
        col_submit, col_cancel = st.columns(2)
        with col_submit:
            if st.button("✓ Add", use_container_width=True, key="submit_add_btn"):
                if not icds_table or not table_name:
                    st.error("❌ Please fill all fields")
                else:
                    icds_table_upper = icds_table.strip().upper()
                    if icds_table_upper in st.session_state.ingestion_data['icdsTableName'].values:
                        st.error(f"❌ SAP Table Name '{icds_table_upper}' already exists.")
                    else:
                        new_row = pd.DataFrame({
                            'icdsTableName': [icds_table_upper],
                            'BANNER_NAME': [banners],
                            'dataSensitivity': [sensitivity],
                            'dlSchemaName': [schema],
                            'dlTableName': [table_name.strip().lower()],
                            'tableLoadType': [load_type],
                        })
                        
                        st.session_state.ingestion_data = pd.concat(
                            [st.session_state.ingestion_data, new_row],
                            ignore_index=True
                        )
                        
                        st.session_state.show_add_form = False
                        st.success(f"✅ Added: {icds_table_upper}")
                        st.rerun()
        
        with col_cancel:
            if st.button("✕ Cancel", use_container_width=True, key="cancel_add_btn"):
                st.session_state.show_add_form = False
                st.rerun()

st.markdown("---")

# Validation checks
validation_errors = {}
has_errors = False

if len(st.session_state.ingestion_data) > 0:
    # Check duplicates
    duplicates = st.session_state.ingestion_data[st.session_state.ingestion_data.duplicated(subset=['icdsTableName'], keep=False)]
    if len(duplicates) > 0:
        st.error("❌ Duplicate SAP Table Names Found:")
        for dup_name in duplicates['icdsTableName'].unique():
            st.error(f"  '{dup_name}' appears multiple times")
        has_errors = True
    
    # Check missing fields
    for idx, row in st.session_state.ingestion_data.iterrows():
        missing_fields = validate_row(row)
        if missing_fields:
            validation_errors[idx] = missing_fields
            has_errors = True

# Show validation errors
if has_errors and validation_errors:
    st.error("❌ Validation Errors - Missing Required Fields:")
    for row_idx, missing_cols in validation_errors.items():
        st.error(f"  Row {row_idx + 1}: Missing {', '.join(missing_cols)}")

st.markdown("---")

# Metrics
if len(st.session_state.ingestion_data) > 0 and not has_errors:
    valid_data = st.session_state.ingestion_data
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Rows", len(valid_data))
    
    with col2:
        banners_str = valid_data['BANNER_NAME'].dropna().astype(str)
        unique_banners = banners_str.str.split(',').explode().unique() if len(banners_str) > 0 else []
        st.metric("🏷️ Unique Banners", len(unique_banners))
    
    with col3:
        schemas = valid_data['dlSchemaName'].dropna()
        st.metric("📦 Schemas", schemas.nunique())
    
    with col4:
        load_types = valid_data['tableLoadType'].dropna()
        st.metric("🔄 Load Types", load_types.nunique())

st.markdown("---")

# Example
st.subheader("📋 Example Format")
example_data = {
    'icdsTableName': ['IKPF', 'ARTCL', 'PF'],
    'BANNER_NAME': ['MDD', 'MDD,MAK,MSB', 'MAK,MSB'],
    'dataSensitivity': ['se', 'ns', 'hs'],
    'dlSchemaName': ['sa_mdse_dl_secure', 'sa_mdse_dl_table', 'sa_mdse_dl_secure'],
    'dlTableName': ['physl_invt_doc', 'article_master', 'payment_flag'],
    'tableLoadType': ['INC', 'FULL', 'INC'],
}
st.dataframe(pd.DataFrame(example_data), use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown("""
### 📝 How to Use:
1. **➕ Add Row**: Fill form to add new row
2. **✏️ Edit Row**: Select 1 row (check ☑️), click to edit entire row
3. **💾 Save Row**: Save your edits and exit edit mode
4. **🗑️ Delete Row**: Select rows, click to delete
5. **🗑️ Clear All**: Remove all rows
6. **⬇️ Download**: Download completed CSV file

### ✅ Features:
- Table is read-only by default (safe editing)
- Select checkbox to pick row for editing
- Edit entire row at once in modal form
- All validations apply
- Changes saved immediately
""")

