import io
from typing import List

import pandas as pd
import streamlit as st
from agsparser import parse_ags_file
from cleaners import (
    normalize_columns, drop_singleton_rows, expand_rows,
    combine_groups, to_numeric_safe
)
from triaxial import (
    generate_triaxial_table, generate_triaxial_with_lithology,
    calculate_s_t_values, remove_duplicate_tests
)
from excel_util import add_st_charts_to_excel

st.set_page_config(page_title="Triaxial + Lithology Explorer", layout="wide")


def main():
    st.title("Triaxial Test Summary with Lithology Mapping")
    st.markdown(
        """
        Upload one or more AGS files and a GIU file containing lithology intervals.
        The GIU file must include: **HOLE_ID** (or **LOCA_ID**), **DEPTH_FROM**, **DEPTH_TO**, **LITH**.
        """
    )

    # 1. File upload
    ags_files: List[bytes] = st.file_uploader(
        "Upload AGS files", type=["ags", "csv"], accept_multiple_files=True
    )
    giu_file = st.file_uploader(
        "Upload GIU file (xlsx or csv)", type=["xlsx", "xls", "csv"]
    )

    if not ags_files or giu_file is None:
        st.info("Please upload both AGS and GIU files to proceed.")
        return

    # 2. Parse AGS files into groups
    parsed = []
    for up in ags_files:
        content = up.read()
        groups = parse_ags_file(content)
        parsed.append((up.name, groups))

    # 3. Clean and combine across files
    # Normalize, drop singletons, expand multi‐row records
    cleaned = []
    for fname, groups in parsed:
        cleaned_groups = {}
        for name, df in groups.items():
            df2 = normalize_columns(df)
            df2 = drop_singleton_rows(df2)
            df2 = expand_rows(df2)
            df2["SOURCE_FILE"] = fname
            cleaned_groups[name] = df2
        cleaned.append((fname, cleaned_groups))

    combined = combine_groups(cleaned)

    # 4. Read and normalize GIU lithology intervals
    if giu_file.name.lower().endswith(".csv"):
        giu = pd.read_csv(giu_file)
    else:
        giu = pd.read_excel(giu_file)

    # Rename LOCA_ID if present
    if "LOCA_ID" in giu.columns and "HOLE_ID" not in giu.columns:
        giu = giu.rename(columns={"LOCA_ID": "HOLE_ID"})

    required = {"HOLE_ID", "DEPTH_FROM", "DEPTH_TO", "LITH"}
    if not required.issubset(set(giu.columns)):
        st.error(f"GIU file must contain columns: {required}")
        return

    giu = normalize_columns(giu)
    to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])

    # 5. Generate triaxial summary + map lithology
    # Insert GIU into groups so generate_triaxial_with_lithology can use it
    combined["GIU"] = giu
    tri_df = generate_triaxial_with_lithology(combined)
    tri_df = calculate_s_t_values(tri_df)
    tri_df = remove_duplicate_tests(tri_df)

    st.subheader("Combined Triaxial Table with LITH")
    st.dataframe(tri_df)

    # 6. Build Excel output: one sheet per LITH value, with S–T plot
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        for lith_val, df_lith in tri_df.groupby("LITH"):
            sheet = str(lith_val)[:31]  # Excel sheet name limit
            df_lith.to_excel(writer, sheet_name=sheet, index=False)

            # Add S–T scatter to this sheet
            st_df = df_lith[["s", "t"]].dropna()
            add_st_charts_to_excel(writer, st_df, sheet_name=sheet)

        writer.save()

    st.download_button(
        label="Download Excel Report",
        data=buffer.getvalue(),
        file_name="triaxial_lithology_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if __name__ == "__main__":
    main()
