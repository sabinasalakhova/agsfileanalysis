import io
from typing import List, Tuple, Dict

import pandas as pd
import plotly.express as px
import streamlit as st

from agsparser import analyze_ags_content, parse_ags_file
from cleaners import (
    normalize_columns, drop_singleton_rows, expand_rows,
    deduplicate_cell, combine_groups, coalesce_columns, to_numeric_safe
)
from triaxial import (
    generate_triaxial_with_lithology,
    calculate_s_t_values, remove_duplicate_tests
)
from excel_util import add_st_charts_to_excel

# Streamlit page config
st.set_page_config(page_title="Triaxial Lab Test AGS Processor", layout="wide")
st.title("Triaxial Lab Test AGS File Processor")

# Step 1: Upload AGS files
st.header("Step 1: Upload AGS Files")
ags_uploads = st.file_uploader(
    "Select one or more AGS files (AGS3/AGS4 formats)",
    type=["ags", "txt", "csv", "dat", "ags4"],
    accept_multiple_files=True
)

# Step 2: Upload GIU lithology table
st.markdown("---")
st.header("Step 2: Upload GIU Lithology Table")
giu_upload = st.file_uploader(
    "Select GIU lithology file (CSV/XLS/XLSX)",
    type=["csv", "xls", "xlsx"]
)

if not ags_uploads or giu_upload is None:
    st.info("Please upload both AGS files and a GIU lithology table to proceed.")
    st.stop()

# Parse and clean AGS files
all_groups: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
diagnostics = []

for af in ags_uploads:
    content = af.read()
    flags = analyze_ags_content(content)
    diagnostics.append((af.name, flags))

    groups = parse_ags_file(content)
    for name, df in groups.items():
        if df is None or df.empty:
            continue

        df = normalize_columns(df)
        df = drop_singleton_rows(df)
        df = expand_rows(df)
        df = df.applymap(deduplicate_cell)
        coalesce_columns(df, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
        coalesce_columns(df, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
        to_numeric_safe(df, ["DEPTH_FROM", "DEPTH_TO"])
        df["SOURCE_FILE"] = af.name
        groups[name] = df

    all_groups.append((af.name, groups))

# Show diagnostics
with st.expander("📋 AGS File Diagnostics"):
    diag_df = pd.DataFrame([
        {"File": fn, **flags} for fn, flags in diagnostics
    ])
    st.dataframe(diag_df, use_container_width=True)

# Combine all groups
combined = combine_groups(all_groups)

# Read and prepare GIU lithology intervals
if giu_upload.name.lower().endswith(".csv"):
    giu = pd.read_csv(giu_upload)
else:
    giu = pd.read_excel(giu_upload)

# Rename LOCA_ID if present
if "LOCA_ID" in giu.columns and "HOLE_ID" not in giu.columns:
    giu = giu.rename(columns={"LOCA_ID": "HOLE_ID"})

required_cols = {"HOLE_ID", "DEPTH_FROM", "DEPTH_TO", "LITH"}
if not required_cols.issubset(set(giu.columns)):
    st.error(f"GIU file must contain columns: {required_cols}")
    st.stop()

giu = normalize_columns(giu)
coalesce_columns(giu, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
coalesce_columns(giu, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])

# Map triaxial + lithology, compute s/t, dedupe
combined["GIU"] = giu
triaxial_df = generate_triaxial_with_lithology(combined)
triaxial_df = calculate_s_t_values(triaxial_df)
triaxial_df = remove_duplicate_tests(triaxial_df)

# Display final table
st.markdown("---")
st.header("Triaxial Summary with Lithology")
st.dataframe(triaxial_df, use_container_width=True)

# Prepare Excel download: one sheet per LITH, with S–T chart
output = io.BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    for lith_val, df_lith in triaxial_df.groupby("LITH"):
        sheet_name = str(lith_val)[:31]
        df_lith.to_excel(writer, sheet_name=sheet_name, index=False)

        # build s–t DataFrame for chart
        st_df = df_lith[["s", "t"]].dropna()
        add_st_charts_to_excel(writer, st_df, sheet_name=sheet_name)

# Download button
st.download_button(
    label="📥 Download Excel Report (by Lithology)",
    data=output.getvalue(),
    file_name="triaxial_with_lithology.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# Interactive s–t plot
st.markdown("---")
st.header("Interactive s–t Plot")

# Filters
col1, col2 = st.columns(2)
with col1:
    hole_opts = sorted(triaxial_df["HOLE_ID"].dropna().unique())
    selected_holes = st.multiselect("Filter by HOLE_ID", hole_opts, default=hole_opts)
with col2:
    lith_opts = sorted(triaxial_df["LITH"].dropna().unique())
    selected_lith = st.multiselect("Filter by LITH", lith_opts, default=lith_opts)

fdf = triaxial_df[
    triaxial_df["HOLE_ID"].isin(selected_holes) &
    triaxial_df["LITH"].isin(selected_lith)
]

if fdf.empty:
    st.warning("No data matches the selected filters.")
else:
    fig = px.scatter(
        fdf, x="s", y="t",
        color="LITH",
        symbol="SOURCE_FILE",
        hover_data=["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF"],
        title="s–t Scatter by Lithology",
        labels={"s": "s (kPa)", "t": "t (kPa)"},
        template="simple_white"
    )
    st.plotly_chart(fig, use_container_width=True)
