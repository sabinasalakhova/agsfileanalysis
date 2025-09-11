# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import io
import re
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st
from scipy.stats import linregress

# External modules
from .agsparser import analyze_ags_content, _split_quoted_csv, parse_ags_file
from .cleaners import (
    deduplicate_cell, drop_singleton_rows, expand_rows,
    combine_groups, coalesce_columns, to_numeric_safe
)
from .triaxial import (
    generate_triaxial_table, generate_triaxial_with_lithology,
    calculate_s_t_values, remove_duplicate_tests
)
from .excel_util import build_triaxial_excel, add_st_charts_to_excel
from .charts import estimate_strength_params  # optional if you split it

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Page Setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.set_page_config(page_title="Triaxial Lab Test AGS Processor", layout="wide")
st.title("Triaxial Lab Test AGS File Processor")

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
# Step 2: Upload GIU Lithology Table
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.markdown("---")
st.header("Step 2: Upload GIU Lithology Table")
giu_file = st.file_uploader(
    label="Upload GIU lithology table (CSV/XLSX/XLS)",
    type=["csv", "xlsx", "xls"],
    accept_multiple_files=False,
    key="giu_uploader",
    help="Required columns: HOLE_ID or LOCA_ID, DEPTH_FROM, DEPTH_TO, LITH"
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 3: Process and Display Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if uploaded_files:
    st.success(f"✅ {len(uploaded_files)} AGS file(s) uploaded successfully.")
    parsed_groups = {}
    for file in uploaded_files:
        content = file.read().decode("utf-8", errors="ignore")
        parsed = parse_ags_file(content)
        parsed_groups.update(parsed)

    # Add GIU if uploaded
    if giu_file:
        giu_df = pd.read_csv(giu_file) if giu_file.name.endswith(".csv") else pd.read_excel(giu_file)
        parsed_groups["GIU"] = giu_df

    # Build triaxial summary
    triaxial_df = generate_triaxial_with_lithology(parsed_groups)
    triaxial_df = calculate_s_t_values(triaxial_df)
    triaxial_df = remove_duplicate_tests(triaxial_df)

    # Display metrics
    phi, cohesion = estimate_strength_params(triaxial_df)
    st.metric("Friction Angle (φ′)", f"{phi}°")
    st.metric("Cohesion (c′)", f"{cohesion} kPa")

    # Show table
    st.dataframe(triaxial_df)

    # Download button
    excel_bytes = build_triaxial_excel(parsed_groups)
    st.download_button(
        label="📥 Download Triaxial Excel Report",
        data=excel_bytes,
        file_name="triaxial_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.warning("⚠️ No AGS files uploaded yet.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 4: Plot s-t with Best-Fit Line
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

st.markdown("---")
st.header("s-t Plot with Strength Parameters")

if not triaxial_df.empty and "s" in triaxial_df.columns and "t" in triaxial_df.columns:
    df_plot = triaxial_df.dropna(subset=["s", "t"]).copy()

    # Estimate strength parameters
    phi, cohesion = estimate_strength_params(df_plot)

    # Fit line
    slope, intercept, _, _, _ = linregress(df_plot["s"], df_plot["t"])
    df_plot["t_fit"] = df_plot["s"] * slope + intercept

    # Plot
    fig = px.scatter(
        df_plot,
        x="s",
        y="t",
        color="LITHOLOGY",
        hover_data=["HOLE_ID", "SPEC_DEPTH"],
        title=f"t–s Plot (φ′ ≈ {phi}°, c′ ≈ {cohesion} kPa)"
    )
    fig.add_scatter(x=df_plot["s"], y=df_plot["t_fit"], mode="lines", name="Best-Fit Line")

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("ℹ️ No valid s–t data available for plotting.")

