import io
from typing import List, Tuple, Dict

import pandas as pd
import plotly.express as px
import streamlit as st

from agsparser import analyze_ags_content, parse_ags_file
from cleaners import (
    normalize_columns,
    drop_singleton_rows,
    expand_rows,
    deduplicate_cell,
    combine_groups,
    coalesce_columns,
    to_numeric_safe,
)
from triaxial import (
    generate_triaxial_with_lithology,
    calculate_s_t_values,
    remove_duplicate_tests,
)
from excel_util import add_st_charts_to_excel

def main():
    st.set_page_config(page_title="Triaxial Lab Test AGS Processor", layout="wide")
    st.title("Triaxial Lab Test AGS File Processor")

    # Step 1: Upload AGS files
    st.header("Step 1: Upload AGS Files")
    ags_uploads = st.file_uploader(
        "Select one or more AGS files (AGS3/AGS4 formats)",
        type=["ags", "txt", "csv", "dat", "ags4"],
        accept_multiple_files=True,
    )

    # Step 2: Upload GIU lithology table
    st.markdown("---")
    st.header("Step 2: Upload GIU Lithology Table")
    giu_upload = st.file_uploader(
        "Select GIU lithology file (CSV/XLS/XLSX)",
        type=["csv", "xls", "xlsx"],
    )

    if not ags_uploads or giu_upload is None:
        st.info("Please upload both AGS files and a GIU lithology table to proceed.")
        return

    # Parse & clean AGS files
    all_groups: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics: List[Tuple[str, Dict[str, bool]]] = []

    for ags_file in ags_uploads:
        raw = ags_file.read()
        flags = analyze_ags_content(raw)
        diagnostics.append((ags_file.name, flags))

        groups = parse_ags_file(raw)
        cleaned: Dict[str, pd.DataFrame] = {}

        for gname, df in groups.items():
            if df is None or df.empty:
                continue

            # Normalize column names
            df = normalize_columns(df)

            # Rename LOCA_ID → HOLE_ID if present
            if "LOCA_ID" in df.columns and "HOLE_ID" not in df.columns:
                df = df.rename(columns={"LOCA_ID": "HOLE_ID"})

            # Drop singleton rows and expand multi‐row records
            df = drop_singleton_rows(df)
            df = expand_rows(df)

            # Deduplicate any repeated values in cells
            df = df.applymap(deduplicate_cell)

            # Coalesce depth fields
            coalesce_columns(df, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
            coalesce_columns(df, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
            to_numeric_safe(df, ["DEPTH_FROM", "DEPTH_TO"])

            # Tag source file
            df["SOURCE_FILE"] = ags_file.name

            cleaned[gname] = df

        all_groups.append((ags_file.name, cleaned))

    # Show diagnostics
    with st.expander("📋 AGS File Diagnostics"):
        diag_df = pd.DataFrame([{"File": fn, **flags} for fn, flags in diagnostics])
        st.dataframe(diag_df, use_container_width=True)

    # Combine all AGS groups across files
    combined_groups = combine_groups(all_groups)

    # Read & clean GIU table
    if giu_upload.name.lower().endswith(".csv"):
        giu = pd.read_csv(giu_upload)
    else:
        giu = pd.read_excel(giu_upload)

    giu = normalize_columns(giu)
    # Rename LOCA_ID → HOLE_ID if needed
    if "LOCA_ID" in giu.columns and "HOLE_ID" not in giu.columns:
        giu = giu.rename(columns={"LOCA_ID": "HOLE_ID"})

    required_cols = {"HOLE_ID", "DEPTH_FROM", "DEPTH_TO", "LITH"}
    if not required_cols.issubset(set(giu.columns)):
        st.error(f"GIU file must contain columns: {required_cols}")
        return

    coalesce_columns(giu, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
    coalesce_columns(giu, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
    to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])

    # Inject GIU into combined groups dict
    combined_groups["GIU"] = giu

    # Build triaxial summary with lithology
    triaxial_df = generate_triaxial_with_lithology(combined_groups)
    triaxial_df = calculate_s_t_values(triaxial_df)
    triaxial_df = remove_duplicate_tests(triaxial_df)

    st.markdown("---")
    st.header("Triaxial Summary with Lithology")
    st.dataframe(triaxial_df, use_container_width=True)

    # === Excel Download by LITH ===
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for lith_val, df_lith in triaxial_df.groupby("LITH"):
            sheet = str(lith_val)[:31]
            df_lith.to_excel(writer, sheet_name=sheet, index=False)
            st_df = df_lith[["s", "t"]].dropna()
            add_st_charts_to_excel(writer, st_df, sheet_name=sheet)

    st.download_button(
        "📥 Download Excel by Lithology",
        data=output.getvalue(),
        file_name="triaxial_with_lithology.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # === Interactive s–t Plot ===
    st.markdown("---")
    st.header("Interactive s–t Plot")

    holes = sorted(triaxial_df["HOLE_ID"].dropna().unique())
    liths = sorted(triaxial_df["LITH"].dropna().unique())

    col1, col2 = st.columns(2)
    with col1:
        pick_holes = st.multiselect("Filter HOLE_ID", holes, default=holes)
    with col2:
        pick_lith  = st.multiselect("Filter LITH", liths, default=liths)

    fdf = triaxial_df.query("HOLE_ID in @pick_holes and LITH in @pick_lith")
    if fdf.empty:
        st.warning("No data matches selected filters.")
    else:
        fig = px.scatter(
            fdf,
            x="s", y="t",
            color="LITH",
            symbol="SOURCE_FILE",
            hover_data=["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF"],
            title="s–t Scatter by Lithology",
            labels={"s": "s (kPa)", "t": "t (kPa)"},
            template="simple_white",
        )
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
