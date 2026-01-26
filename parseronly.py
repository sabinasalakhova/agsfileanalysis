# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict
import io
import plotly.express as px
import re
import xlsxwriter
# External modules
from agsparser import analyze_ags_content, _split_quoted_csv, parse_ags_file, find_hole_id_column
from cleaners import deduplicate_cell, drop_singleton_rows, expand_rows, combine_groups, coalesce_columns, to_numeric_safe, normalize_columns
from triaxial import generate_triaxial_table, generate_triaxial_with_lithology, calculate_s_t_values, remove_duplicate_tests
from excel_util import add_st_charts_to_excel, build_all_groups_excel, remove_duplicate_tests

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Page Setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.set_page_config(page_title="Triaxial Lab Test AGS Processor", layout="wide")
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Process Uploaded Files
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if uploaded_files:
    all_group_dfs: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics: List[Tuple[str, Dict[str, bool]]] = []
    failed_files = []
    
    progress_bar = st.progress(0)
    for i, f in enumerate(uploaded_files):
        try:
            file_bytes = f.getvalue()
            # Extract safe file prefix
            file_prefix = re.sub(r'[^A-Z0-9]', '', f.name.split('.')[0].upper())[:5]
            
            # 1) Diagnostics
            flags = analyze_ags_content(file_bytes)
            diagnostics.append((f.name, flags))
            
            # 2) Parse into per-group DataFrames
            raw_groups: Dict[str, pd.DataFrame] = parse_ags_file(file_bytes, f.name)
            cleaned_groups: Dict[str, pd.DataFrame] = {}
            for group_name, df in raw_groups.items():
                # skip empty groups
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
                # 3) Normalize column names
                df = normalize_columns(df)
                # 7) depth columns
                to_numeric_safe(df, ["DEPTH_FROM", "DEPTH_TO"])
                # store cleaned group
                cleaned_groups[group_name] = df
            # collect this file’s cleaned groups
            all_group_dfs.append((f.name, cleaned_groups))
        except Exception as e:
            failed_files.append((f.name, str(e)))
        progress_bar.progress((i + 1) / len(uploaded_files))
    
    if failed_files:
        st.error("Some files failed to process:")
        st.dataframe(pd.DataFrame(failed_files, columns=["File", "Error"]))
    width
    # 9) Combine across files
    combined_groups = combine_groups(all_group_dfs)
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 4: Show quick diagnostics results
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with st.expander("File diagnostics (AGS type & key groups)", expanded=False):
        diag_df = pd.DataFrame([{"File": n, **flags} for (n, flags) in diagnostics])
        st.dataframe(diag_df, width='stretch')
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 5: Sidebar: downloads and plotting options
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with st.sidebar:
        st.header("Export Options")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Full merged workbook"):
                buf = build_all_groups_excel(combined_groups)
                st.download_button(
                    "📥 All groups (Excel)",
                    data=buf,
                    file_name="all_ags_groups.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_all"
                )
        
        with col2:
            if "TRIX" in combined_groups or "TRIG" in combined_groups:
                if st.button("Triaxial + s-t charts"):
                    tri_df = generate_triaxial_table(combined_groups)
                    if not tri_df.empty:
                        tri_df["HOLE_ID"] = tri_df["HOLE_ID"].astype(str).str.upper().str.strip()
                        tri_df["SPEC_DEPTH"] = pd.to_numeric(tri_df["SPEC_DEPTH"], errors="coerce")
                        st_df = calculate_s_t_values(tri_df)
                        
                        buf = io.BytesIO()
                        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                            tri_df.to_excel(w, "Summary", index=False)
                            st_df.to_excel(w, "s_t_Values", index=False)
                            add_st_charts_to_excel(w, st_df, "s_t_Values")
                        buf.seek(0)
                        st.download_button(
                            "📥 Triaxial (Excel, with charts)",
                            data=buf,
                            file_name="triaxial_summary_s_t.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dl_tri"
                        )
                    else:
                        st.info("No triaxial tests found.")
        
        st.divider()
        
        st.header("Build Your Own Excel")
        
        if combined_groups:
            # Select groups to include
            selected_groups = st.multiselect(
                "Select groups to include:",
                options=sorted(combined_groups.keys()),
                default=sorted(combined_groups.keys()),
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
            
            row_filters = {}  # group_name → {column: [values]}
            if enable_row_filters:
                for group_name in selected_groups:
                    st.subheader(f"Filter: {group_name}")
                    df = combined_groups[group_name]
                    row_filters[group_name] = {}
                    sample_rows = df.sample(min(5, len(df))).to_dict(orient="records")
                    st.table(sample_rows)  # Show a sample of rows for context
                    for col in group_column_selections[group_name]:
                        if col not in df.columns:
                            continue
                        vals = sorted(df[col].dropna().unique())
                        if len(vals) > 30:
                            st.caption(f"{col} — too many unique values ({len(vals)})")
                            continue
                        selected = st.multiselect(f"{col}", vals, key=f"flt_{group_name}_{col}")
                        if selected:
                            row_filters[group_name][col] = selected
            
            # Generate the "Build Your Own Excel" file
            if st.button("Generate Custom Excel File"):
                st.info("Processing your custom Excel...")
                
                custom_buffer = io.BytesIO()
                try:
                    with pd.ExcelWriter(custom_buffer, engine="xlsxwriter") as writer:
                        if concat_option:
                            # Concatenate data across groups
                            concatenated_df = pd.DataFrame()
                            for group_name in selected_groups:
                                group_df = combined_groups[group_name].copy()
                                
                                # Filter rows based on row_filters
                                if enable_row_filters and group_name in row_filters:
                                    for column, allowed_values in row_filters[group_name].items():
                                        if allowed_values and column in group_df.columns:
                                            group_df = group_df[group_df[column].isin(allowed_values)]
                                
                                # Select columns
                                valid_columns = group_column_selections.get(group_name, group_df.columns)
                                group_df = group_df[[c for c in valid_columns if c in group_df.columns]]
                                
                                # Add group identifier column
                                group_df['SOURCE_GROUP'] = group_name
                                
                                concatenated_df = pd.concat([concatenated_df, group_df], ignore_index=True)
                            
                            # Save to Excel
                            if not concatenated_df.empty:
                                concatenated_df.to_excel(writer, index=False, sheet_name="Concatenated_Groups")
                            else:
                                pd.DataFrame({"Note": ["No data after filtering"]}).to_excel(writer, index=False, sheet_name="Empty")
                        else:
                            # Separate sheets for each group
                            for group_name in selected_groups:
                                group_df = combined_groups[group_name].copy()
                                
                                # Filter rows
                                if enable_row_filters and group_name in row_filters:
                                    for column, allowed_values in row_filters[group_name].items():
                                        if allowed_values and column in group_df.columns:
                                            group_df = group_df[group_df[column].isin(allowed_values)]
                                
                                # Select columns
                                valid_columns = group_column_selections.get(group_name, group_df.columns)
                                group_df = group_df[[c for c in valid_columns if c in group_df.columns]]
                                
                                # Save individual sheet
                                safe_sheet = re.sub(r'[\[\]*?:/\\]', '_', group_name)[:31]
                                if not group_df.empty:
                                    group_df.to_excel(writer, index=False, sheet_name=safe_sheet)
                    
                    custom_buffer.seek(0)
                    st.download_button(
                        label="📥 Download Custom Excel File",
                        data=custom_buffer,
                        file_name="custom_ags_groups.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        help="Custom Excel file generated based on your selections.",
                        key="custom_dl_ready"
                    )
                except Exception as exc:
                    st.error(f"Excel generation failed: {exc}")
        else:
            st.warning("No data groups available for customizing.")
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 6: Show group tables (with per-group Excel download)
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
                safe_sheet = re.sub(r'[\[\]*?:/\\]', '_', rename_map.get(gname, gname))[:31]
                gdf_out.to_excel(writer, index=False, sheet_name=safe_sheet)
            buffer.seek(0)
            
            st.download_button(
                label=f"Download {gname} (Excel)",
                data=buffer,
                file_name=f"{gname}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{gname}",
            )
    
    st.divider()
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Triaxial Section (Conditional)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if "TRIX" in combined_groups or "TRIG" in combined_groups:
        with st.container():
            st.header("Triaxial Summary & s–t Plots")
            tri_df = generate_triaxial_table(combined_groups)
            if tri_df.empty:
                st.info("No triaxial data (TRIX/TRET + TRIG/TREG) detected in the uploaded files.")
            else:
                # ─── 2) Normalize IDs & depths ─────────────────────────────────────
                tri_df["HOLE_ID"] = tri_df["HOLE_ID"].astype(str).str.upper().str.strip()
                tri_df["SPEC_DEPTH"] = pd.to_numeric(tri_df["SPEC_DEPTH"], errors="coerce")
                
                st_df = calculate_s_t_values(tri_df)
                
                # ─── 6) Display summary ─────────────────────────────────────────────
                st.write(f"**Triaxial summary (with s, t & lithology)** — {len(tri_df)} rows")
                st.dataframe(tri_df, width='stretch', height=350)
