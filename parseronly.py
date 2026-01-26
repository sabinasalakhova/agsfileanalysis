# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━���━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict
import io

# External modules
from agsparser import analyze_ags_content, parse_ags_file, find_hole_id_column
from excel_util import build_all_groups_excel

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Page Setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

st.set_page_config(page_title="AGS File Parser", layout="wide")
st.title("AGS File Processor")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 1: Upload AGS Files
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
        
        # Extract the first 5 characters of the file name
        file_prefix = f.name[:5].upper()

        # 1) Diagnostics
        flags = analyze_ags_content(file_bytes)
        diagnostics.append((f.name, flags))

        # 2) Parse into per-group DataFrames
        raw_groups: Dict[str, pd.DataFrame] = parse_ags_file(file_bytes, f.name)

        cleaned_groups: Dict[str, pd.DataFrame] = {}

        for group_name, df in raw_groups.items():
            # Skip empty groups
            if df is None or df.empty:
                continue

            # Add SOURCE_FILE column
            df["SOURCE_FILE"] = f.name

            # Find and prefix HOLE_ID
            hole_id_col = find_hole_id_column(df.columns)
            if hole_id_col:
                # Ensure HOLE_ID is a string and prefix it
                df[hole_id_col] = df[hole_id_col].astype(str).str.strip()
                df[hole_id_col] = file_prefix + "_" + df[hole_id_col]

            # Store the cleaned group
            cleaned_groups[group_name] = df

            # Combine groups by group name for "all groups" download
            if group_name not in combined_groups:
                combined_groups[group_name] = []
            combined_groups[group_name].append(df)

        # Collect this file’s cleaned groups
        all_group_dfs.append((f.name, cleaned_groups))

    # Combine groups across files
    combined_groups = {g: pd.concat(dfs, ignore_index=True) for g, dfs in combined_groups.items()}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Display Groups and Enable Downloads
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.subheader("📋 AGS Groups (Parsed and Prefixed)")

    for file_name, group_dict in all_group_dfs:
        st.markdown(f"### File: {file_name}")
        
        for group_name, group_df in group_dict.items():
            if group_df.empty:
                continue
            st.write(f"**{group_name}** — {len(group_df)} rows")
            st.dataframe(group_df, use_container_width=True)

    # Add download options in the sidebar
    with st.sidebar:
        st.header("Downloads")
        
        # Option 1: Download all groups as combined Excel
        if combined_groups:
            # Build Excel for all groups
            all_xl = build_all_groups_excel(combined_groups)
            st.download_button(
                "📥 Download ALL groups (Excel)",
                data=all_xl,
                file_name="ags_groups_combined.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Each AGS group is a sheet; all uploaded files are merged by group."
            )

        # Option 2: Build custom Excel
        st.markdown("---")
        st.header("Build Your Own Excel")

        # Allow user to select groups and provide convenience in choosing columns
        selected_groups = st.multiselect(
            "Select groups to include:",
            options=combined_groups.keys(),
            default=list(combined_groups.keys())
        )

        all_columns = sorted(
            {col for g in selected_groups for col in combined_groups[g].columns}
        )
        selected_columns = st.multiselect(
            "Select columns to include (leave blank for ALL columns):",
            options=all_columns,
            default=[],  # Default to empty (meaning "all columns")
            help="Leave blank to include all columns from each selected group."
        )

        # Default behavior - include all columns if none are specified
        columns_to_include = selected_columns if selected_columns else all_columns

        if selected_groups and columns_to_include:
            # Generate the "Build Your Own Excel" file
            custom_buffer = io.BytesIO()
            with pd.ExcelWriter(custom_buffer, engine="xlsxwriter") as writer:
                for group_name in selected_groups:
                    group_df = combined_groups[group_name][columns_to_include].copy()
                    group_df.to_excel(writer, index=False, sheet_name=group_name[:31])

            st.download_button(
                "📥 Download Selected Groups/Columns (Excel)",
                data=custom_buffer.getvalue(),
                file_name="custom_ags_groups.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Excel file with selected groups and columns."
            )

        st.markdown("---")
        st.header("Parsing Diagnostics")
        diag_df = pd.DataFrame(
            [{"File": n, **flags} for n, flags in diagnostics]
        )
        st.dataframe(diag_df, use_container_width=True)
