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

    # Step 1: AGS upload
    ags_uploads = st.file_uploader(
        "Step 1: Upload AGS files (AGS3/AGS4)", 
        type=["ags", "txt", "csv", "dat", "ags4"], 
        accept_multiple_files=True
    )

    # Step 2: GIU lithology
    st.markdown("---")
    giu_upload = st.file_uploader(
        "Step 2: Upload GIU lithology (CSV/XLS/XLSX)",
        type=["csv", "xls", "xlsx"]
    )

    if not ags_uploads or giu_upload is None:
        st.info("Please upload both AGS files and a GIU lithology table.")
        return

    # Parse & clean AGS
    all_groups: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics = []
    for af in ags_uploads:
        raw = af.read()
        flags = analyze_ags_content(raw)
        diagnostics.append((af.name, flags))

        groups = parse_ags_file(raw)
        cleaned = {}
        for gname, df in groups.items():
            if df is None or df.empty:
                continue

            df = normalize_columns(df)
            if "LOCA_ID" in df.columns and "HOLE_ID" not in df.columns:
                df = df.rename(columns={"LOCA_ID": "HOLE_ID"})

            df = drop_singleton_rows(df)
            df = expand_rows(df)
            df = df.applymap(deduplicate_cell)
            coalesce_columns(df, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
            coalesce_columns(df, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
            to_numeric_safe(df, ["DEPTH_FROM", "DEPTH_TO"])
            df["SOURCE_FILE"] = af.name
            cleaned[gname] = df

        all_groups.append((af.name, cleaned))

    # Diagnostics
    with st.expander("📋 AGS Diagnostics"):
        diag_df = pd.DataFrame([{"File": n, **f} for n, f in diagnostics])
        st.dataframe(diag_df, use_container_width=True)

    # Combine AGS groups
    combined = combine_groups(all_groups)

    # Read & clean GIU
    if giu_upload.name.lower().endswith(".csv"):
        giu = pd.read_csv(giu_upload)
    else:
        giu = pd.read_excel(giu_upload)

    giu = normalize_columns(giu)
    if "LOCA_ID" in giu.columns and "HOLE_ID" not in giu.columns:
        giu = giu.rename(columns={"LOCA_ID": "HOLE_ID"})

    req = {"HOLE_ID", "DEPTH_FROM", "DEPTH_TO", "LITH"}
    if not req.issubset(giu.columns):
        st.error(f"GIU must contain {req}")
        return

    coalesce_columns(giu, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
    coalesce_columns(giu, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
    to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])
    combined["GIU"] = giu

    # Build triaxial + lithology, compute s/t, dedupe
    tri = generate_triaxial_with_lithology(combined)
    tri = calculate_s_t_values(tri)
    tri = remove_duplicate_tests(tri)

    # RENAME LITHOLOGY → LITH for grouping/filtering
    if "LITHOLOGY" in tri.columns:
        tri = tri.rename(columns={"LITHOLOGY": "LITH"})

    st.markdown("---")
    st.header("Triaxial Summary with Lithology")
    st.dataframe(tri, use_container_width=True)

    # Excel: one sheet per LITH, each with an s–t chart
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for lith_val, df_lith in tri.groupby("LITH"):
            sheet = str(lith_val)[:31]
            df_lith.to_excel(writer, sheet_name=sheet, index=False)
            st_df = df_lith[["s", "t"]].dropna()
            add_st_charts_to_excel(writer, st_df, sheet_name=sheet)

    st.download_button(
        "📥 Download Excel by Lithology",
        data=buf.getvalue(),
        file_name="triaxial_by_lithology.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Interactive s–t plot
    st.markdown("---")
    st.header("Interactive s–t Plot")

    holes = sorted(tri["HOLE_ID"].dropna().unique())
    liths = sorted(tri["LITH"].dropna().unique())

    c1, c2 = st.columns(2)
    with c1:
        pick_h = st.multiselect("Filter HOLE_ID", holes, default=holes)
    with c2:
        pick_l = st.multiselect("Filter LITH", liths, default=liths)

    fdf = tri.query("HOLE_ID in @pick_h and LITH in @pick_l")
    if fdf.empty:
        st.warning("No points match filters.")
    else:
        fig = px.scatter(
            fdf, x="s", y="t",
            color="LITH", symbol="SOURCE_FILE",
            hover_data=["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF"],
            title="s–t Scatter by Lithology",
            labels={"s": "s (kPa)", "t": "t (kPa)"},
            template="simple_white"
        )
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
