import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict
import io

# External modules
from agsparser import analyze_ags_content, parse_ags_file, find_hole_id_column
from excel_util import build_all_groups_excel

# Page Setup
st.set_page_config(page_title="AGS File Parser", layout="wide")
st.title("AGS File Processor")

# Step 1: Upload AGS Files
st.header("Step 1: Upload AGS Files")
uploaded_files = st.file_uploader(
    label="Upload one or more AGS files (AGS3/AGS4 format)",
    type=["ags", "txt", "csv", "dat", "ags4"],
    accept_multiple_files=True,
    help="Supported formats: .ags, .txt, .csv, .dat, .ags4"
)

if uploaded_files:
    all_group_dfs: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics: List[Tuple[str, Dict[str, bool]]] = []
    combined_groups: Dict[str, pd.DataFrame] = {}

    for f in uploaded_files:
        file_bytes = f.getvalue()
        file_prefix = f.name[:5].upper()

        flags = analyze_ags_content(file_bytes)
        diagnostics.append((f.name, flags))

        raw_groups: Dict[str, pd.DataFrame] = parse_ags_file(file_bytes, f.name)
        cleaned_groups: Dict[str, pd.DataFrame] = {}

        for group_name, df in raw_groups.items():
            if df is None or df.empty:
                continue

            df["SOURCE_FILE"] = f.name
            hole_id_col = find_hole_id_column(df.columns)
            if hole_id_col:
                df[hole_id_col] = df[hole_id_col].astype(str).str.strip()
                df[hole_id_col] = file_prefix + "_" + df[hole_id_col]
            cleaned_groups[group_name] = df

            if group_name not in combined_groups:
                combined_groups[group_name] = []
            combined_groups[group_name].append(df)
        
        all_group_dfs.append((f.name, cleaned_groups))

    combined_groups = {g: pd.concat(dfs, ignore_index=True) for g, dfs in combined_groups.items()}

    st.subheader("📋 AGS Groups (Parsed and Prefixed)")

    for file_name, group_dict in all_group_dfs:
        st.markdown(f"### File: {file_name}")
        
        for group_name, group_df in group_dict.items():
            if group_df.empty:
                continue
            st.write(f"**{group_name}** — {len(group_df)} rows")
            st.dataframe(group_df, use_container_width=True)

    with st.sidebar:
        st.header("Downloads")
        if combined_groups:
            all_xl = build_all_groups_excel(combined_groups)
            st.download_button(
                "📥 Download ALL groups (Excel)",
                data=all_xl,
                file_name="ags_groups_combined.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Each AGS group is a sheet; all uploaded files are merged by group."
            )

        st.markdown("---")
        st.header("Build Your Own Excel")

     # Verify files and processing
if uploaded_files:
    all_group_dfs = []
    combined_groups = {}

    # Process uploaded files
    for uploaded_file in uploaded_files:
        # Simulate reading file and creating group data
        df_sample = pd.DataFrame({
            "HOLE_ID": [1, 2, 3],
            "DEPTH_FROM": [0, 10, 20],
            "DEPTH_TO": [10, 20, 30],
        })
        group_name = "GROUP_" + uploaded_file.name.split(".")[0]
        combined_groups[group_name] = df_sample  # Create dummy combined groups for testing

    # --- Build Your Own Excel Logic ---
    with st.sidebar:
        st.header("Build Your Own Excel")

    # Verify files and processing
if uploaded_files:
    all_group_dfs = []
    combined_groups = {}

    # Process uploaded files
    for uploaded_file in uploaded_files:
        # Simulate reading file and creating group data
        df_sample = pd.DataFrame({
            "HOLE_ID": [1, 2, 3],
            "DEPTH_FROM": [0, 10, 20],
            "DEPTH_TO": [10, 20, 30],
        })
        group_name = "GROUP_" + uploaded_file.name.split(".")[0]
        combined_groups[group_name] = df_sample  # Create dummy combined groups for testing

    # --- Build Your Own Excel Logic ---
    with st.sidebar:
        st.header("Build Your Own Excel")
    # Verify files and processing
    if uploaded_files:
        all_group_dfs = []
        combined_groups = {}
    
        # Process uploaded files
        for uploaded_file in uploaded_files:
            # Simulate reading file and creating group data
            df_sample = pd.DataFrame({
                "HOLE_ID": [1, 2, 3],
                "DEPTH_FROM": [0, 10, 20],
                "DEPTH_TO": [10, 20, 30],
            })
            group_name = "GROUP_" + uploaded_file.name.split(".")[0]
            combined_groups[group_name] = df_sample  # Create dummy combined groups for testing
    
        # --- Build Your Own Excel Logic ---
        with st.sidebar:
            st.header("Build Your Own Excel")
    
            if combined_groups:  # Ensure there are groups to work with
                # Select groups to include
                selected_groups = st.multiselect(
                    "Select groups to include:",
                    options=combined_groups.keys(),
                    default=list(combined_groups.keys()),
                    help="Select groups from the uploaded files."
                )
    
                # Advanced toggle for concatenation
                concat_option = st.checkbox(
                    "Concatenate all selected groups into a single sheet",
                    value=False,
                    help="Enable this to combine all selected groups into one sheet in the output Excel file."
                )
    
                # Dynamic column selection for individual groups
                group_column_selections = {}
                for group_name in selected_groups:
                    st.subheader(f"Group: {group_name}")
                    available_columns = list(combined_groups[group_name].columns)
                    
                    group_column_selections[group_name] = st.multiselect(
                        f"Columns for '{group_name}'",
                        options=available_columns,
                        default=available_columns,  # Default to all columns
                        help=f"Select columns to include in group '{group_name}'."
                    )
    
                # Optional filtering (by HOLE_TYPE or depth ranges)
                st.subheader("Advanced Filtering (Optional)")
                enable_row_filters = st.checkbox(
                    "Enable Row Filtering",
                    value=False,
                    help="Check this to filter rows (e.g., by specific column values)."
                )
    
                row_filters = {}
                if enable_row_filters:
                    for group_name in selected_groups:
                        st.subheader(f"Filtering for Group: {group_name}")
                        sample_rows = combined_groups[group_name].sample(min(5, len(combined_groups[group_name]))).to_dict(orient="records")
    
                        st.table(sample_rows)  # Show a sample of rows for context (first 5)
                        for column in group_column_selections[group_name]:
                            unique_values = combined_groups[group_name][column].dropna().unique().tolist()
                            row_filters[column] = st.multiselect(
                                f"Filter {column} in group '{group_name}'",
                                options=unique_values,
                                default=[],
                                help=f"Select specific values to include for column '{column}' in group '{group_name}'. Leave blank to include all."
                            )
    
                # Generate the "Build Your Own Excel" file
                if st.button("Generate Custom Excel File"):
                    st.info("Processing your custom Excel...")
                    
                    custom_buffer = io.BytesIO()
                    with pd.ExcelWriter(custom_buffer, engine="xlsxwriter") as writer:
                        if concat_option:
                            # Concatenate data across groups
                            concatenated_df = pd.DataFrame()
                            for group_name in selected_groups:
                                group_df = combined_groups[group_name]
    
                                # Filter rows based on row_filters
                                if enable_row_filters:
                                    for column, allowed_values in row_filters.items():
                                        if allowed_values:
                                            group_df = group_df[group_df[column].isin(allowed_values)]
    
                                # Select columns
                                valid_columns = group_column_selections[group_name]
                                group_df = group_df[valid_columns]
    
                                # Add group identifier column
                                group_df['SOURCE_GROUP'] = group_name
    
                                concatenated_df = pd.concat([concatenated_df, group_df], ignore_index=True)
    
                            # Save to Excel
                            concatenated_df.to_excel(writer, index=False, sheet_name="Concatenated Groups")
                        else:
                            # Separate sheets for each group
                            for group_name in selected_groups:
                                group_df = combined_groups[group_name]
    
                                # Filter rows and select columns
                                if enable_row_filters:
                                    for column, allowed_values in row_filters.items():
                                        if allowed_values:
                                            group_df = group_df[group_df[column].isin(allowed_values)]
    
                                valid_columns = group_column_selections[group_name]
                                group_df = group_df[valid_columns]
    
                                # Save individual sheet
                                group_df.to_excel(writer, index=False, sheet_name=group_name[:31])
                    
                    # Allow user to download the file
                    st.download_button(
                        label="📥 Download Custom Excel File",
                        data=custom_buffer.getvalue(),
                        file_name="custom_ags_groups.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        help="Custom Excel file generated based on your selections."
                    )
            else:
                st.warning("No data groups available for customizing.")
        st.header("Parsing Diagnostics")
        diag_df = pd.DataFrame(
            [{"File": n, **flags} for n, flags in diagnostics]
        )
        st.dataframe(diag_df, use_container_width=True)
