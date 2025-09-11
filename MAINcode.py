

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict
import io
import plotly.express as px

# External modules
from agsparser import analyze_ags_content, _split_quoted_csv, parse_ags_file
from cleaners import deduplicate_cell, drop_singleton_rows, expand_rows, combine_groups, coalesce_columns, to_numeric_safe, normalize_columns
from triaxial import generate_triaxial_table, generate_triaxial_with_lithology, calculate_s_t_values, remove_duplicate_tests

from excel_util import  add_st_charts_to_excel, build_all_groups_excel, remove_duplicate_tests


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
giu = st.file_uploader(
    label="Upload GIU lithology table (CSV/XLSX/XLS)",
    type=["csv", "xlsx", "xls"],
    accept_multiple_files=False,
    key="giu_uploader",
    help="Required columns: HOLE_ID or LOCA_ID, DEPTH_FROM, DEPTH_TO, LITH"
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
        raw_groups: Dict[str, pd.DataFrame] = parse_ags_file(file_bytes)
        cleaned_groups: Dict[str, pd.DataFrame] = {}

        for group_name, df in raw_groups.items():
            # skip empty groups
            if df is None or df.empty:
                continue

            # 3) Normalize column names
            df = normalize_columns(df)

            # 4) Drop rows where only one cell is populated
            df = drop_singleton_rows(df)

            # 5) Expand any multi-interval rows into one record per interval
            df = expand_rows(df)

            # 6) Clean up duplicate values within each cell
            df = df.applymap(deduplicate_cell)

            # 7) Unify depth columns
            coalesce_columns(df, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
            coalesce_columns(df, ["DEPTH_TO",   "END_DEPTH"],   "DEPTH_TO")
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
    # merged across all uploaded files. You can proceed to triaxial/lithology logic…

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#Step 4: Show quick diagnostics results, user should understand not to mix ags3 and ags4
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with st.expander("File diagnostics (AGS type & key groups)", expanded=False):
        diag_df = pd.DataFrame(
            [{"File": n, **flags} for (n, flags) in diagnostics]
        )
        st.dataframe(diag_df, use_container_width=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#Step 5:  Sidebar: downloads and plotting options
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
#Step 6:  Show group tables (with per-group Excel download)  
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.subheader("📋 AGS Groups (merged across all uploaded files)")

    tabs = st.tabs(sorted(combined_groups.keys()))
    for tab, gname in zip(tabs, sorted(combined_groups.keys())):
        with tab:
            gdf = combined_groups[gname]
            st.write(f"**{gname}** — {len(gdf)} rows")
            st.dataframe(gdf, use_container_width=True, height=350)

            # Per-group download (Excel)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                drop_singleton_rows(gdf).to_excel(writer, index=False, sheet_name=gname[:31])
            st.download_button(
                label=f"Download {gname} (Excel)",
                data=buffer.getvalue(),
                file_name=f"{gname}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{gname}",
            )

 # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   # --- Triaxial summary & plots
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    st.markdown("---")
    st.header(" Triaxial Summary & s–t Plots")

    
    tri_df = generate_triaxial_table(combined_groups)
    
    if tri_df.empty:
        st.info("No triaxial data (TRIX/TRET + TRIG/TREG) detected in the uploaded files.")
   
    else:
               
        tri_df["HOLE_ID"]    = tri_df["HOLE_ID"].astype(str).str.upper().str.strip()
        tri_df["SPEC_DEPTH"] = pd.to_numeric(tri_df["SPEC_DEPTH"], errors="coerce")
    
        # Mapper: finds the first GIU interval whose HOLE_ID endswith the AGS hole
        def map_litho(row):
            h = row["HOLE_ID"]
            d = row["SPEC_DEPTH"]
            if pd.isna(h) or pd.isna(d):
                return None
    
            mask = (
                giu["HOLE_ID"].str.upper().str.strip().str.endswith(h)
                & (giu["DEPTH_FROM"] <= d)
                & (giu["DEPTH_TO"]   >= d)
            )
            m = giu_file.loc[mask]
            return m.iloc[0]["LITH"] if not m.empty else None
    
        # Apply mapping
        tri_df["LITH"] = tri_df.apply(map_litho, axis=1)
        assigned = tri_df["LITH"].notna().sum()
        st.write(f"🔍 mapped lithology for {assigned} of {len(tri_df)} records")
    
        # ─── (A) s–t computations ───────────────────────────────────────────
        mode  = "Effective" if stress_mode.startswith("Effective") else "Total"
        st_df = calculate_s_t_values(tri_df)
    
        # ─── (B) Merge s,t into the summary grid ────────────────────────────
        merge_keys   = [c for c in ["HOLE_ID","SPEC_DEPTH","CELL","PWPF","DEVF"] if c in tri_df.columns]
        cols_from_st = [c for c in ["HOLE_ID","SPEC_DEPTH","CELL","PWPF","DEVF","s_total","s_effective","s","t","TEST_TYPE","SOURCE_FILE"] if c in st_df.columns]
    
        tri_df_with_st = pd.merge(
            tri_df.assign(LITH=tri_df["LITH"]),     # include LITH 
            st_df[cols_from_st],
            on=merge_keys,
            how="left"
        )
        tri_df_with_st = remove_duplicate_tests(tri_df_with_st)
        # Normalize first
        tri_df_with_st["HOLE_ID"]    = tri_df_with_st["HOLE_ID"].astype(str).str.upper().str.strip()
        tri_df_with_st["SPEC_DEPTH"] = pd.to_numeric(tri_df_with_st["SPEC_DEPTH"], errors="coerce")
        
        # Define the same mapper, pointing at `giu`
        def map_litho_after(row):
            hole  = row["HOLE_ID"]
            depth = row["SPEC_DEPTH"]
            if not hole or pd.isna(depth):
                return None
        
            mask = (
                giu["HOLE_ID"].str.endswith(hole)
                & (giu["DEPTH_FROM"] <= depth)
                & (giu["DEPTH_TO"]   >= depth)
            )
            sub = giu.loc[mask]
            return sub.iloc[0]["LITH"] if not sub.empty else None
        
        # Apply to the merged table
        tri_df_with_st["LITH"] = tri_df_with_st.apply(map_litho_after, axis=1)
        st.write(f"🔍 mapped lithology for {tri_df_with_st['LITH'].notna().sum()} records")
        st.write(f"**Triaxial summary (with s & t)** — {len(tri_df_with_st)} rows")
        st.dataframe(tri_df_with_st, use_container_width=True, height=350)
    

                # s–t computations & plot
        st.markdown("#### s–t computed values")
        mode = "Effective" if stress_mode.startswith("Effective") else "Total"
        st_df = calculate_s_t_values(tri_df)
        
                # Download triaxial table (with s–t) + Excel Charts
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            # 1) Save the with-s,t summary (more useful than raw-only)
            tri_df_with_st.to_excel(writer, index=False, sheet_name="Triaxial_Summary")
            # 2) Save the computed s–t values (contains s_total, s_effective, s, t)
            st_df.to_excel(writer, index=False, sheet_name="s_t_Values")
            # 3) Add Excel charts (s′–t and s–t) on a 'Charts' sheet
            add_st_charts_to_excel(writer, st_df, sheet_name="s_t_Values")

        
        st.download_button(
            "📥 Download Triaxial Summary + s–t (Excel, with charts)",
            data=buffer.getvalue(),
            file_name="triaxial_summary_s_t.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        fdf = st_df.copy()
        
        # Plot
        hover_cols = [c for c in ["HOLE_ID", "TEST_TYPE", "SPEC_DEPTH", "CELL", "PWPF", "DEVF", "s_total", "s_effective", "SOURCE_FILE"] if c in fdf.columns]
        fig = px.scatter(
            fdf,
            x="s",
            y="t",
            color=fdf[color_by] if color_by in fdf.columns else None,
            facet_col=facet_col if facet_col in fdf.columns else None,
            symbol="TEST_TYPE" if "TEST_TYPE" in fdf.columns else None,
            hover_data=hover_cols,
            title=f"s–t Plot ({mode} stress)",
            labels={"s": "s (kPa)", "t": "t = q/2 (kPa)"},
            template="simple_white"
        )
        if show_labels and "HOLE_ID" in fdf.columns:
            fig.update_traces(text=fdf["HOLE_ID"], textposition="top center", mode="markers+text")

        fig.update_layout(legend_title_text=color_by if color_by in fdf.columns else "Legend")
        st.plotly_chart(fig, use_container_width=True, theme="streamlit")

else:
    st.info("Upload one or more AGS files to begin. You can select additional files anytime; the app merges all groups and updates tables, downloads, and plots.")
