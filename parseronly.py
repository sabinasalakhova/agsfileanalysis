
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict
import io
import plotly.express as px
import re

# External modules
from agsparser import analyze_ags_content, _split_quoted_csv, parse_ags_file
from cleaners import deduplicate_cell, drop_singleton_rows, expand_rows, combine_groups, coalesce_columns, to_numeric_safe, normalize_columns
from triaxial import generate_triaxial_table, generate_triaxial_with_lithology, calculate_s_t_values, remove_duplicate_tests

from excel_util import  add_st_charts_to_excel, build_all_groups_excel, remove_duplicate_tests

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Page Setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.set_page_config(page_title="Triaxial Lab Test AGS Processor", layout="wide")
st.title(" AGS File Processor")

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 3: Clean AGS DATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if uploaded_files:
    all_group_dfs: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics: List[Tuple[str, Dict[str, bool]]] = []

    for f in uploaded_files:
        file_bytes = f.getvalue()
        
        # 1) Diagnostics
        flags = analyze_ags_content(file_bytes)
        diagnostics.append((f.name, flags))

        # 2) Parse into per-group DataFrames
        file_bytes = f.getvalue()
        raw_groups: Dict[str, pd.DataFrame] = parse_ags_file(file_bytes, f.name)
        cleaned_groups: Dict[str, pd.DataFrame] = {}

        for group_name, df in raw_groups.items():
            # skip empty groups
            if df is None or df.empty:
                continue

            # 3) Normalize column names
            df = normalize_columns(df)

            # 7)  depth columns
            
            to_numeric_safe(df, ["DEPTH_FROM", "DEPTH_TO"])

            # 8) Tag origin file
            df["SOURCE_FILE"] = f.name

            # store cleaned group
            cleaned_groups[group_name] = df

        # collect this file’s cleaned groups
        all_group_dfs.append((f.name, cleaned_groups))

    # 9) Combine across files
    combined_groups = combine_groups(all_group_dfs)

    # Now `combined_groups` contains one cleaned DataFrame per AGS group,
    
    # merged across all uploaded files. can proceed to triaxial/lithology logic…
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 4: Show quick diagnostics results
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with st.expander("File diagnostics (AGS type & key groups)", expanded=False):
        diag_df = pd.DataFrame(
            [{"File": n, **flags} for (n, flags) in diagnostics]
        )
        st.dataframe(diag_df, width='stretch')

   # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 5:  Sidebar: downloads and plotting options
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with st.sidebar:
        st.header("Downloads & Plot Options")

        if combined_groups:
            all_xl = build_all_groups_excel(combined_groups)
            st.download_button(
                "📥 Download ALL groups (one Excel workbook)",
                data=all_xl,
                file_name="ags_groups_combined.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Each AGS group is a separate sheet; all uploaded files are merged."
            )

        st.markdown("---")
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 6:  Show group tables (with per-group Excel download)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.subheader("📋 AGS Groups (merged across all uploaded files)")
    
    rename_map = {
        "?ETH": "WETH",
        "?ETH_TOP": "WETH_TOP",
        "?ETH_BASE": "WETH_BASE",
        "?ETH_GRAD": "WETH_GRAD",
        "?LEGD": "LEGD",
        "?HORN": "HORN",
    }
    tabs = st.tabs(sorted(combined_groups.keys()))
    for tab, gname in zip(tabs, sorted(combined_groups.keys())):
        with tab:
            gdf = combined_groups[gname]
            st.write(f"**{gname}** — {len(gdf)} rows")
            st.dataframe(gdf, width='stretch', height=350)

            # Per-group download (Excel)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                # apply column heading fixes
                gdf_out = drop_singleton_rows(gdf).rename(columns=rename_map)
            
                # apply sheet name fixes
                safe_sheet = rename_map.get(gname, gname)
                gdf_out.to_excel(writer, index=False, sheet_name=safe_sheet[:31])
            st.download_button(
                label=f"Download {gname} (Excel)",
                data=buffer.getvalue(),
                file_name=f"{gname}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{gname}",
            )
            
        st.markdown("---")
        st.header("Build Your Own Excel")

     # Verify files and processing

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
        st.dataframe(diag_df, width='stretch')
        st.markdown("---")
        st.header("Triaxial Summary & s–t Plots")
             # 1) Build raw triaxial summary
        tri_df = generate_triaxial_table(combined_groups)
        if tri_df.empty:
            st.info("No triaxial data (TRIX/TRET + TRIG/TREG) detected in the uploaded files.")
            
            # ─── 2) Normalize IDs & depths ─────────────────────────────────────
        tri_df["HOLE_ID"]    = tri_df["HOLE_ID"].astype(str).str.upper().str.strip()
        tri_df["SPEC_DEPTH"] = pd.to_numeric(tri_df["SPEC_DEPTH"], errors="coerce")
    
        st_df = calculate_s_t_values(tri_df) 
    
                    # ─── 6) Display summary ─────────────────────────────────────────────
        st.write(f"**Triaxial summary (with s, t & lithology)** — {len(tri_df)} rows")
        st.dataframe(tri_df, width='stretch', height=350)
        
                # ─── 7) (optional) Excel download with charts ──────────────────────
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            tri_df_with_st.to_excel(writer, index=False, sheet_name="Triaxial_Summary")
            st_df.to_excel(writer, index=False, sheet_name="s_t_Values")
            add_st_charts_to_excel(writer, st_df, sheet_name="s_t_Values")
        
        st.download_button(
             "📥 Download Triaxial + s–t (Excel, with charts)",
            data=buffer.getvalue(),
            file_name="triaxial_summary_s_t.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
