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

        selected_groups = st.multiselect(
            "Select groups to include:",
            options=combined_groups.keys(),
            default=list(combined_groups.keys())
        )

        selected_columns = {}
        for group in selected_groups:
            group_columns = combined_groups[group].columns
            selected_columns[group] = st.multiselect(
                f"Columns for group '{group}':",
                options=group_columns,
                default=[],  # Default to empty, meaning "all columns"
                key=f"columns_{group}",
                help=f"Leave empty to include all columns for group '{group}'."
            )

        concat_option = st.checkbox(
            "Concatenate all selected groups into one sheet",
            value=False,
            help="Enable this to merge all selected groups into a single sheet in the Excel file."
        )

        if selected_groups:
            custom_buffer = io.BytesIO()
            with pd.ExcelWriter(custom_buffer, engine="xlsxwriter") as writer:
                if concat_option:
                    concatenated_df = pd.DataFrame()
                    for group_name in selected_groups:
                        group_df = combined_groups[group_name]
                        valid_columns = selected_columns[group_name] if selected_columns[group_name] else group_df.columns
                        concatenated_df = pd.concat([concatenated_df, group_df[valid_columns]])
                    concatenated_df.to_excel(writer, index=False, sheet_name="Concatenated")
                else:
                    for group_name in selected_groups:
                        group_df = combined_groups[group_name]
                        valid_columns = selected_columns[group_name] if selected_columns[group_name] else group_df.columns
                        group_df[valid_columns].to_excel(writer, index=False, sheet_name=group_name[:31])

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
