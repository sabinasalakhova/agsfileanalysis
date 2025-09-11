

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


import pandas as pd
import streamlit as st


# External modules
from agsparser import analyze_ags_content, _split_quoted_csv, parse_ags_file
from cleaners import deduplicate_cell, drop_singleton_rows, expand_rows, combine_groups, coalesce_columns, to_numeric_safe
from triaxial import generate_triaxial_table, generate_triaxial_with_lithology, calculate_s_t_values, remove_duplicate_tests

from excel_util import build_triaxial_excel, add_st_charts_to_excel
from charts import estimate_strength_params  

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
# Step 3: Process 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if uploaded_files:
    # Parse all uploaded files
    all_group_dfs: List[Tuple[str, Dict[str, pd.DataFrame]]] = []
    diagnostics = []

    for f in uploaded_files:
        file_bytes = f.getvalue()
        flags = analyze_ags_content(file_bytes)
        diagnostics.append((f.name, flags))
        gdict = parse_ags_file(file_bytes)
        # Attach source file tag
        for g in gdict.values():
            if g is not None:
                g["SOURCE_FILE"] = f.name
        all_group_dfs.append((f.name, gdict))

    # Show quick diagnostics
    with st.expander("File diagnostics (AGS type & key groups)", expanded=False):
        diag_df = pd.DataFrame(
            [{"File": n, **flags} for (n, flags) in diagnostics]
        )
        st.dataframe(diag_df, use_container_width=True)

    # Combine groups across files
    combined_groups = combine_groups(all_group_dfs)

    # Sidebar: downloads and plotting options
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
    # Show group tables (with per-group Excel download)
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

 
   # --- Triaxial summary & plots
    st.markdown("---")
    st.header(" Triaxial Summary & s–t Plots")
    tri_df = generate_triaxial_table(combined_groups)
    
    if tri_df.empty:
        st.info("No triaxial data (TRIX/TRET + TRIG/TREG) detected in the uploaded files.")
    else:
        # (A) s–t computations (do this BEFORE displaying the summary)
        mode = "Effective" if stress_mode.startswith("Effective") else "Total"
        st_df = compute_s_t(tri_df, mode=mode)
    
        # (B) Merge s,t into the Triaxial summary grid (avoid accidental many-to-many merges)
        merge_keys = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "PWPF", "DEVF"] if c in tri_df.columns]
        cols_from_st = [c for c in ["HOLE_ID","SPEC_DEPTH","CELL","PWPF","DEVF","s_total","s_effective","s","t","TEST_TYPE","SOURCE_FILE"] if c in st_df.columns]
        tri_df_with_st = pd.merge(tri_df, st_df[cols_from_st], on=merge_keys, how="left")
        tri_df_with_st = remove_duplicate_tests(tri_df_with_st)
    
        st.write(f"**Triaxial summary (with s & t)** — {len(tri_df_with_st)} rows")
        st.dataframe(tri_df_with_st, use_container_width=True, height=350)
    

                # s–t computations & plot
        st.markdown("#### s–t computed values")
        mode = "Effective" if stress_mode.startswith("Effective") else "Total"
        st_df = compute_s_t(tri_df, mode=mode)
        
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


            
