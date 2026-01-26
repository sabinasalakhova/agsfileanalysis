# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict
import io

# External modules
from agsparser import analyze_ags_content, parse_ags_file, find_hole_id_column

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

        # Collect this file’s cleaned groups
        all_group_dfs.append((f.name, cleaned_groups))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Combine and Display Results
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.subheader("📋 AGS Groups (Parsed and Prefixed)")

    for file_name, group_dict in all_group_dfs:
        st.markdown(f"### File: {file_name}")
        
        for group_name, group_df in group_dict.items():
            if group_df.empty:
                continue
            st.write(f"**{group_name}** — {len(group_df)} rows")
            st.dataframe(group_df, use_container_width=True)

    with st.sidebar:
        st.header("Parsing Diagnostics")
        diag_df = pd.DataFrame(
            [{"File": n, **flags} for n, flags in diagnostics]
        )
        st.dataframe(diag_df, use_container_width=True)

        st.markdown("---")
