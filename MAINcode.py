
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict, Optional
import io
import plotly.express as px
import re

# External modules
from agsparser import analyze_ags_content, _split_quoted_csv, parse_ags_file
from cleaners import deduplicate_cell, drop_singleton_rows, expand_rows, combine_groups, coalesce_columns, to_numeric_safe, normalize_columns
from triaxial import generate_triaxial_table, generate_triaxial_with_lithology, calculate_s_t_values, remove_duplicate_tests
from map_concat import combine_ags_data, build_continuous_intervals, map_group_to_intervals, simplify_weathering_grade
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
giu_base = st.text_input("Enter GIU base prefix (e.g., GIU123):", value="GIU") 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Step 3: Clean AGS DATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if uploaded_files:
    all_group_dfs: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics: List[Tuple[str, Dict[str, bool]]] = []
    
    # User inputs GIU base prefix once (outside the loop)
    giu_base = st.text_input(
        "Enter GIU base prefix (e.g., GIU123):",
        value="GIU",
        help="This prefix will be added to HOLE_ID for each file (e.g., GIU123_1_BH01)"
    )

    for file_idx, f in enumerate(uploaded_files):
        file_bytes = f.getvalue()
        
        # 1) Diagnostics
        flags = analyze_ags_content(file_bytes)
        diagnostics.append((f.name, flags))
        
        # 2) Parse into per-group DataFrames
        raw_groups: Dict[str, pd.DataFrame] = parse_ags_file(file_bytes, f.name)
        cleaned_groups: Dict[str, pd.DataFrame] = {}
        
        # Per-file GIU number (unique per file)
        giu_no = f"{giu_base}_{file_idx + 1}" if giu_base else f"FILE_{file_idx + 1}"
        
        for group_name, df in raw_groups.items():
            if df is None or df.empty:
                continue
            
            # Cleaning steps (same as before)
            df = normalize_columns(df)
            df = drop_singleton_rows(df)
            df = df.map(deduplicate_cell)
            coalesce_columns(df, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
            coalesce_columns(df, ["DEPTH_TO", "END_DEPTH"], "DEPTH_TO")
            to_numeric_safe(df, ["DEPTH_FROM", "DEPTH_TO"])
            df["SOURCE_FILE"] = f.name
            
            # ── IMPORTANT: Add GIU prefixing here ───────────────────────────────
            df["GIU_NO"] = giu_no 
            hole_id_col = find_hole_id_column(df.columns)
            if hole_id_col:
                df[hole_id_col] = df[hole_id_col].astype(str).str.strip()
                df["GIU_HOLE_ID"] = giu_no + "_" + df[hole_id_col]
            
            cleaned_groups[group_name] = df
        
        # Collect this file’s cleaned groups
        all_group_dfs.append((f.name, cleaned_groups))

    # 9) Combine across files (now includes GIU_NO and GIU_HOLE_ID in every group)
    combined_groups = combine_groups(all_group_dfs)

    # Now `combined_groups` contains one cleaned DataFrame per AGS group,
    # merged across all uploaded files. You can proceed to triaxial/lithology logic…

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Step 4: Show quick diagnostics results, user should understand not to mix ags3 and ags4
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
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # giu file cleaning
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    giu_df = None
    if giu_file is not None:
        name = giu_file.name.lower()
        if name.endswith(".csv"):
            giu_df = pd.read_csv(giu_file)
        else:
            giu_df = pd.read_excel(giu_file)

        # NOW giu_df is a DataFrame. Clean it:
        giu_df = normalize_columns(giu_df)

        if "LOCA_ID" in giu_df.columns and "HOLE_ID" not in giu_df.columns:
            giu_df = giu_df.rename(columns={"LOCA_ID": "HOLE_ID"})

        giu_df = drop_singleton_rows(giu_df)
        giu_df = expand_rows(giu_df)
        giu_df = giu_df.map(deduplicate_cell)
        coalesce_columns(giu_df, ["DEPTH_FROM" ,"START_DEPTH"], "DEPTH_FROM")
        coalesce_columns(giu_df, ["DEPTH_TO" ,"END_DEPTH"],     "DEPTH_TO")
        to_numeric_safe(giu_df, ["DEPTH_FROM" ,"DEPTH_TO"])

        st.write("Cleaned GIU intervals:")
        st.dataframe(giu_df, width='stretch')


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # --- Triaxial summary & plots
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.markdown("---")
    st.header("Triaxial Summary & s–t Plots")

    # 1) Build raw triaxial summary
    tri_df = generate_triaxial_table(combined_groups)

    if tri_df.empty:
        st.info("No triaxial data (TRIX/TRET + TRIG/TREG) detected in the uploaded files.")
    else:
        if giu_df is None:
            st.error("Please upload and clean the GIU table first.")
            st.stop()

        # ─── 2) Normalize IDs & depths ─────────────────────────────────────
        tri_df["HOLE_ID"]    = tri_df["HOLE_ID"].astype(str).str.upper().str.strip()
        tri_df["SPEC_DEPTH"] = pd.to_numeric(tri_df["SPEC_DEPTH"], errors="coerce")


        giu_df["HOLE_ID"]     = (giu_df["HOLE_ID"]
                                 .astype(str)
                                 .str.upper()
                                 .str.strip())
        giu_df["DEPTH_FROM"]  = pd.to_numeric(giu_df["DEPTH_FROM"], errors="coerce")
        giu_df["DEPTH_TO"]    = pd.to_numeric(giu_df["DEPTH_TO"],   errors="coerce")

        # ─── 3) Map lithology from GIU into tri_df ─────────────────────────
        def map_litho(row):
            hole, depth = row["HOLE_ID"], row["SPEC_DEPTH"]
            if pd.isna(hole) or pd.isna(depth):
                return None

            mask = (
                    giu_df["HOLE_ID"].str.upper().str.strip().str.endswith(hole)
                    & (giu_df["DEPTH_FROM"] <= depth)
                    & (giu_df["DEPTH_TO"]   >= depth)
            )
            sub = giu_df.loc[mask]
            return sub.iloc[0]["LITH"] if not sub.empty else None

        tri_df["LITH"] = tri_df.apply(map_litho, axis=1)
        st.write(f"🔍 Mapped LITH for {tri_df['LITH'].notna().sum()} / {len(tri_df)} records")

        # ─── 4) Compute s & t ───────────────────────────────────────────────

        st_df = calculate_s_t_values(tri_df)

        # ─── 5) Merge s,t (and LITH) into final summary ────────────────────
        merge_keys   = [c for c in ["HOLE_ID" ,"SPEC_DEPTH" ,"CELL" ,"PWPF" ,"DEVF"] if c in tri_df.columns]
        st_cols      = [c for c in ["s" ,"t" ,"s_total" ,"s_effective" ,"s_source" ,"TEST_TYPE" ,"SOURCE_FILE"]
                        if c in st_df.columns]

        tri_df_with_st = (
            tri_df
            .merge(st_df[merge_keys + st_cols], on=merge_keys, how="left")
            .pipe(remove_duplicate_tests)
        )

                # ─── 6) Display summary ─────────────────────────────────────────────
        st.write(f"**Triaxial summary (with s, t & lithology)** — {len(tri_df_with_st)} rows")
        st.dataframe(tri_df_with_st, use_container_width=True, height=350)  # DEPTH_SOURCE now visible

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

    # ──────────────────────────────────────────────────────────────
    # New Section: Combine into Continuous Geological Intervals
    # ──────────────────────────────────────────────────────────────
    st.markdown("---")
    st.header("Combine into Continuous Intervals")

    if combined_groups:
        available_groups = sorted(combined_groups.keys())
        selected_groups = st.multiselect(
            "Select groups to include in continuous log:",
            options=available_groups,
            default=[g for g in ['GEOL', 'WETH', 'CORE', 'DETL'] if g in available_groups]
        )

        if st.button("Generate Continuous Intervals"):
            with st.spinner("Building intervals and mapping attributes..."):
                try:
                    result_df = combine_ags_data(
                        combined_groups,
                        giu_df=giu_df,  # Pass GIU for LITH mapping
                        selected_groups=selected_groups,
                        legacy_fill=True  # Enable legacy mode
                    )

                    if result_df.empty:
                        st.warning("No depth intervals could be created from the selected groups.")
                    else:
                        st.success(f"Created {len(result_df)} intervals across boreholes.")
                        st.dataframe(
                            result_df.style.format(precision=2, na_rep="-"),
                            use_container_width=True,
                            height=500
                        )

                        # Download buttons
                        col1, col2 = st.columns(2)
                        with col1:
                            csv_buf = io.StringIO()
                            result_df.to_csv(csv_buf, index=False)
                            st.download_button(
                                "📥 Download CSV",
                                csv_buf.getvalue(),
                                "continuous_intervals.csv",
                                "text/csv"
                            )

                        with col2:
                            excel_buf = io.BytesIO()
                            with pd.ExcelWriter(excel_buf, engine='xlsxwriter') as writer:
                                result_df.to_excel(writer, index=False, sheet_name="Intervals")
                            st.download_button(
                                "📥 Download Excel",
                                excel_buf.getvalue(),
                                "continuous_intervals.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                except Exception as e:
                    st.error(f"Error during interval combination: {str(e)}")
                    st.info("Common causes: missing DEPTH_FROM/DEPTH_TO columns, no numeric depths, or empty selected groups.")
    else:
        st.info("Upload and process AGS files first.")
