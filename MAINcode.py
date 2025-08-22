

import io
import re
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# --------------------------------------------------------------------------------------
# Page config & title
# --------------------------------------------------------------------------------------
st.set_page_config(page_title="Triaxial Lab Test AGS Processor", layout="wide")
st.title("Triaxial Lab Test AGS File Processor (.ags ➜ Tables + Excel + s–t Graphs)")

# --------------------------------------------------------------------------------------
# File upload (multi-file)
# --------------------------------------------------------------------------------------
uploaded_files = st.file_uploader(
    "Upload one or more AGS files (AGS3/AGS4)", type=["ags", "txt", "csv", "dat", "ags4"], accept_multiple_files=True
)

# --------------------------------------------------------------------------------------
# Helpers: AGS detection & parsing (handles AGS3 and AGS4)
# --------------------------------------------------------------------------------------
def analyze_ags_content(file_bytes: bytes) -> Dict[str, str]:
    """
    Light fingerprinting of AGS3 vs AGS4 and presence of key groups.
    Non-fatal: returns flags only.
    """
    results = {"AGS3": "No", "AGS4": "No", 'Contains "LOCA"': "No", "Contains **HOLE": "No"}
    try:
        content = file_bytes.decode("latin-1", errors="ignore")
        lines = content.splitlines()
        for line in lines:
            s = line.strip()
            if s.startswith('"GROUP"') or s.startswith("GROUP"):
                results["AGS4"] = "Yes"
                if '"GROUP","LOCA"' in s or "GROUP,LOCA" in s:
                    results['Contains "LOCA"'] = "Yes"
                break
            if s.startswith('"**') or s.startswith("**"):
                results["AGS3"] = "Yes"
                if "**HOLE" in s:
                    results["Contains **HOLE"] = "Yes"
                break
    except Exception:
        pass
    return results


def _split_quoted_csv(line: str) -> List[str]:
    """
    Robust split for AGS-style quoted CSV (handles quotes and commas).
    """
    # Trim whitespace
    s = line.strip()
    # Normalize quotes
    if s.startswith('"') and s.endswith('"') and '","' in s:
        parts = [p.replace('""', '"') for p in s.split('","')]
        parts[0] = parts[0].lstrip('"')
        parts[-1] = parts[-1].rstrip('"')
        return parts
    # Fallback split
    return [p.strip().strip('"') for p in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', s)]


def parse_ags_file(file_bytes: bytes) -> Dict[str, pd.DataFrame]:
    """
    Unified parser that supports:
    - AGS4: lines starting with GROUP, HEADING, UNIT, TYPE and DATA or direct data rows
    - AGS3: lines starting with **GROUP, *HEADING and data rows
    - <CONT> continuation rows for both (may appear as "<CONT>" or &lt;CONT&gt;)
    Returns a dict: {group_name: DataFrame}
    """
    text = file_bytes.decode("latin-1", errors="ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    group_data: Dict[str, List[Dict[str, str]]] = {}
    group_headings: Dict[str, List[str]] = {}
    current_group = None
    headings: List[str] = []

    def ensure_group(gname: str):
        if gname not in group_data:
            group_data[gname] = []

    for raw in lines:
        # Replace HTML-escaped continuation
        if raw.startswith('"&lt;CONT&gt;"') or raw.startswith("&lt;CONT&gt;"):
            token = "<CONT>"
        else:
            token = None

        parts = _split_quoted_csv(raw)

        # AGS4 path: leading descriptor like GROUP, HEADING, UNIT, TYPE, DATA or <CONT>
        if parts and (parts[0].upper() in {"GROUP", "HEADING", "UNIT", "TYPE", "DATA"} or token == "<CONT>"):
            desc = parts[0].upper() if token is None else "<CONT>"
            if desc == "GROUP":
                current_group = parts[1]
                ensure_group(current_group)
                headings = []
            elif desc == "HEADING":
                # second..N are headings
                headings = parts[1:]
                group_headings[current_group] = headings
            elif desc == "DATA":
                if current_group and headings:
                    row = dict(zip(headings, parts[1:len(headings)+1]))
                    group_data[current_group].append(row)
            elif desc == "<CONT>":
                # Append continuation values to previous data row
                if current_group and headings and group_data[current_group]:
                    for idx, val in enumerate(parts[1:]):
                        if idx < len(headings) and val:
                            field = headings[idx]
                            prev = group_data[current_group][-1].get(field, "")
                            prev = prev if prev else ""
                            if str(val) not in [p.strip() for p in prev.split(" | ") if p]:
                                group_data[current_group][-1][field] = (prev + " | " if prev else "") + val
            else:
                # UNIT/TYPE ignored for parsing (metadata)
                pass
            continue

        # AGS3 path: **GROUP, *HEADING, DATA, <CONT>
        if parts and (parts[0].startswith("**") or parts[0].startswith("*") or parts[0] == "<CONT>"):
            first = parts[0]
            if first.startswith("**"):
                current_group = first[2:]
                ensure_group(current_group)
                headings = []
            elif first.startswith("*"):
                # headings are the parts; strip leading *
                headings = [p.lstrip("*") for p in parts]
                group_headings[current_group] = headings
            elif first == "<CONT>":
                if current_group and headings and group_data[current_group]:
                    for idx, val in enumerate(parts[1:]):
                        if idx < len(headings) and val:
                            field = headings[idx]
                            prev = group_data[current_group][-1].get(field, "")
                            prev = prev if prev else ""
                            if str(val) not in [p.strip() for p in prev.split(" | ") if p]:
                                group_data[current_group][-1][field] = (prev + " | " if prev else "") + val
            continue

        # DATA row fallback (both styles) when descriptors are absent
        if current_group and headings:
            if len(parts) >= len(headings):
                row = dict(zip(headings, parts[:len(headings)]))
                group_data[current_group].append(row)

    # Convert each group to a DataFrame
    group_dfs = {g: pd.DataFrame(rows) for g, rows in group_data.items()}

    # Normalization: common AGS spelling differences and keys
    for g, df in group_dfs.items():
        if df.empty:
            continue
        # normalize column names
        renamed = {}
        for c in df.columns:
            cc = c
            if cc.upper() == "SPEC_DPTH" or cc.upper() == "SPEC_DEPTH":
                cc = "SPEC_DEPTH"
            if cc.upper() == "LOCA_ID" or cc.upper() == "HOLE_ID":
                cc = "HOLE_ID"
            renamed[c] = cc
        df = df.rename(columns=renamed)
        group_dfs[g] = df

    return group_dfs

#remove empty rows
def drop_singleton_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that have <=1 non-empty/non-null values across all columns.
    This prevents rows with only HOLE_ID populated from slipping in.
    """
    if df.empty:
        return df
    # Treat empty strings and whitespace as NaN
    clean = df.replace(r"^\s*$", np.nan, regex=True)
    nn = clean.notna().sum(axis=1)
    return df.loc[nn > 1].reset_index(drop=True)

#if one cell has multiple same values, leave one
def deduplicate_cell(cell):
    if pd.isna(cell):
        return cell
    parts = [p.strip() for p in str(cell).split(" | ")]
    unique_parts = []
    for p in parts:
        if p and p not in unique_parts:
            unique_parts.append(p)
    return " | ".join(unique_parts)

#if one cell has multiple different values >>> expand
def expand_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    If any cell contains " | " concatenated values, expand into multiple rows.
    """
    expanded_rows = []
    for _, row in df.iterrows():
        split_values = {col: (str(row[col]).split(" | ") if pd.notna(row[col]) else [""]) for col in df.columns}
        max_len = max(len(v) for v in split_values.values()) if split_values else 1
        for i in range(max_len):
            new_row = {col: (split_values[col][i] if i < len(split_values[col]) else "") for col in df.columns}
            expanded_rows.append(new_row)
    return pd.DataFrame(expanded_rows)


# --------------------------------------------------------------------------------------
# Merge multi-file groups & show diagnostics
# --------------------------------------------------------------------------------------
def combine_groups(all_group_dfs: List[Tuple[str, Dict[str, pd.DataFrame]]]) -> Dict[str, pd.DataFrame]:
    """
    Combine groups across files. Adds SOURCE_FILE column.
    Returns {group_name: combined_df}
    """
    combined: Dict[str, List[pd.DataFrame]] = {}
    for fname, gdict in all_group_dfs:
        for gname, df in gdict.items():
            if df is None or df.empty:
                continue
            temp = df.copy()
            temp["SOURCE_FILE"] = fname
            combined.setdefault(gname, []).append(temp)
    return {g: drop_singleton_rows(pd.concat(dfs, ignore_index=True)) for g, dfs in combined.items()}


# --------------------------------------------------------------------------------------
# Triaxial summary & s–t calculations
# --------------------------------------------------------------------------------------
def coalesce_columns(df: pd.DataFrame, candidates: List[str], new_name: str):
    """
    Create/rename a single column 'new_name' from the first existing candidate.
    """
    for c in candidates:
        if c in df.columns:
            df[new_name] = df[c]
            return
    # ensure column exists
    if new_name not in df.columns:
        df[new_name] = np.nan


def to_numeric_safe(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def generate_triaxial_table(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a single triaxial summary table from available AGS groups:
    - SAMP / CLSS (optional)
    - TRIG (total stress general) or TREG (effective stress general)
    - TRIX (AGS3 results) or TRET (AGS4 results)
    """
    # Get groups by priority
    samp = groups.get("SAMP", pd.DataFrame()).copy()
    clss = groups.get("CLSS", pd.DataFrame()).copy()
    trig = groups.get("TRIG", pd.DataFrame()).copy()  # total stress general
    treg = groups.get("TREG", pd.DataFrame()).copy()  # effective stress general
    trix = groups.get("TRIX", pd.DataFrame()).copy()  # AGS3 results
    tret = groups.get("TRET", pd.DataFrame()).copy()  # AGS4 results

    # Normalize key columns for joins
    for df in [samp, clss, trig, treg, trix, tret]:
        if df.empty:
            continue
        if "HOLE_ID" not in df.columns:
            df["HOLE_ID"] = np.nan
        # Normalize SPEC_DEPTH spelling
        rename_map = {c: "SPEC_DEPTH" for c in df.columns if c.upper() in {"SPEC_DPTH", "SPEC_DEPTH"}}
        df.rename(columns=rename_map, inplace=True)
    
        # Ensure HOLE_ID is string
        if "HOLE_ID" in df.columns:
            df["HOLE_ID"] = df["HOLE_ID"].astype(str)


    # Merge keys
    merge_keys = ["HOLE_ID"]
    if not samp.empty and "SPEC_DEPTH" in samp.columns:
        merge_keys.append("SPEC_DEPTH")

    merged = samp.copy() if not samp.empty else pd.DataFrame(columns=merge_keys).copy()

    # add CLSS (outer)
    if not clss.empty:
        merged = pd.merge(merged, clss, on=merge_keys, how="outer", suffixes=("", "_CLSS"))

    # add TRIG/TREG type info
    ty_cols = []
    if not trig.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TRIG_TYPE"] if c in trig.columns]
        trig_f = trig[keep].copy()
        merged = pd.merge(merged, trig_f, on=[c for c in keep if c in merge_keys], how="outer")
        ty_cols.append("TRIG_TYPE")
    if not treg.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TREG_TYPE"] if c in treg.columns]
        treg_f = treg[keep].copy()
        merged = pd.merge(merged, treg_f, on=[c for c in keep if c in merge_keys], how="outer")
        ty_cols.append("TREG_TYPE")

    # add TRIX/TRET result data (outer)
    tri_res = pd.DataFrame()
    if not trix.empty:
        tri_res = trix.copy()
    if not tret.empty:
        tri_res = tri_res.append(tret.copy(), ignore_index=True) if not tri_res.empty else tret.copy()

    # Coalesce expected result columns -> unified names
    if not tri_res.empty:
        coalesce_columns(tri_res, ["SPEC_DEPTH", "SPEC_DPTH"], "SPEC_DEPTH")
        coalesce_columns(tri_res, ["HOLE_ID", "LOCA_ID"], "HOLE_ID")
        coalesce_columns(tri_res, ["TRIX_CELL", "TRET_CELL"], "CELL")     # σ3 total cell pressure during shear
        coalesce_columns(tri_res, ["TRIX_DEVF", "TRET_DEVF"], "DEVF")     # deviator at failure (q)
        coalesce_columns(tri_res, ["TRIX_PWPF", "TRET_PWPF"], "PWPF")     # porewater u at failure
        tri_keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF", "PWPF", "SOURCE_FILE"] if c in tri_res.columns]
        tri_res = tri_res[tri_keep].copy()
        merged = pd.merge(merged, tri_res, on=[c for c in ["HOLE_ID", "SPEC_DEPTH"] if c in merged.columns], how="outer")

    # Final column subset (add useful identifiers if present)
    cols_pref = [
        "HOLE_ID", "SAMP_ID", "SAMP_REF", "SAMP_TOP",
        "SPEC_REF", "SPEC_DEPTH", "SAMP_DESC", "SPEC_DESC", "GEOL_STAT",
        "TRIG_TYPE", "TREG_TYPE",  # test types
        "CELL", "DEVF", "PWPF", "SOURCE_FILE"
    ]
    final_cols = [c for c in cols_pref if c in merged.columns]
    final_df = merged[final_cols].copy() if final_cols else merged.copy()

    # Deduplicate cell text and expand rows if any " | "
    final_df = final_df.applymap(deduplicate_cell)
    expanded_df = expand_rows(final_df)

    # Drop rows that are effectively empty (<=1 non-null)
    expanded_df = drop_singleton_rows(expanded_df)

    # Numeric cast for core fields
    to_numeric_safe(expanded_df, ["SPEC_DEPTH", "CELL", "DEVF", "PWPF"])

    return expanded_df


def compute_s_t(tri_df: pd.DataFrame, mode: str = "Effective") -> pd.DataFrame:
    """
    Compute (s, t) for each triaxial test row.
    - t = q/2 = DEVF/2
    - s_total = σ3 + q/2 = CELL + DEVF/2
    - s_eff = σ3' + q/2 = (CELL - PWPF) + DEVF/2
    If PWPF is missing in Effective mode, s_eff falls back to NaN; we still compute t.
    """
    df = tri_df.copy()
    # Coalesce a single 'TEST_TYPE' helper
    if "TEST_TYPE" not in df.columns:
        if "TREG_TYPE" in df.columns and "TRIG_TYPE" in df.columns:
            df["TEST_TYPE"] = df["TREG_TYPE"].fillna(df["TRIG_TYPE"])
        elif "TREG_TYPE" in df.columns:
            df["TEST_TYPE"] = df["TREG_TYPE"]
        elif "TRIG_TYPE" in df.columns:
            df["TEST_TYPE"] = df["TRIG_TYPE"]
        else:
            df["TEST_TYPE"] = np.nan

    to_numeric_safe(df, ["CELL", "DEVF", "PWPF"])
    df["t"] = df["DEVF"] / 2.0
    df["s_total"] = df["CELL"] + df["t"]
    df["s_effective"] = (df["CELL"] - df["PWPF"]) + df["t"] if "PWPF" in df.columns else np.nan

    if mode.lower().startswith("eff"):
        df["s"] = df["s_effective"]
    else:
        df["s"] = df["s_total"]

    # Keep key columns for plotting
    keep = ["HOLE_ID", "SPEC_DEPTH", "TEST_TYPE", "CELL", "PWPF", "DEVF", "s_total", "s_effective", "s", "t", "SOURCE_FILE"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].copy()


def build_all_groups_excel(groups: Dict[str, pd.DataFrame]) -> bytes:
    """
    Create an Excel workbook where each group is one sheet.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as xw:
        for gname, gdf in sorted(groups.items()):
            if gdf is None or gdf.empty:
                continue
            # Excel sheet name limit and avoid duplicates
            sheet_name = gname[:31]
            # Clean rows (no singleton)
            out = drop_singleton_rows(gdf)
            out.to_excel(xw, index=False, sheet_name=sheet_name)
    return buffer.getvalue()

def add_st_charts_to_excel(writer: pd.ExcelWriter, st_df: pd.DataFrame, sheet_name: str = "s_t_Values"):
    """
    Adds two charts to the workbook:
      - s'–t (effective): x = s_effective, y = t
      - s–t (total)    : x = s_total,     y = t
    Places them on a new sheet 'Charts'.
    """
    if st_df is None or st_df.empty:
        return

    workbook  = writer.book
    ws_vals   = writer.sheets.get(sheet_name)
    if ws_vals is None:
        return

    # Row/col counts in the written sheet
    nrows = len(st_df)
    if nrows == 0:
        return

    # Column indices
    idx = {c: i for i, c in enumerate(st_df.columns)}
    if "t" not in idx or ("s_effective" not in idx and "s_total" not in idx):
        return  # nothing to plot

    # Data starts at row=1 (row 0 is header)
    r0, r1 = 1, nrows

    # Create Charts worksheet
    ws_charts = workbook.add_worksheet("Charts")

    def add_scatter(title: str, xcol: str, ycol: str, anchor: str):
        if xcol not in idx or ycol not in idx:
            return
        cx, cy = idx[xcol], idx[ycol]

        chart = workbook.add_chart({'type': 'scatter', 'subtype': 'marker_only'})
        chart.set_title({'name': title})
        chart.set_x_axis({'name': 's (kPa)'})
        chart.set_y_axis({'name': "t = q/2 (kPa)"})
        chart.set_legend({'none': True})

        # A1 notation ranges for x/y
        sheet = sheet_name
        # Excel is col letters; build ranges using XlsxWriter utility
        # We'll use row/col notation instead (zero-based, inclusive)
        chart.add_series({
            'name':       title,
            'categories': [sheet, r0, cx, r1, cx],  # x-values
            'values':     [sheet, r0, cy, r1, cy],  # y-values
            'marker':     {'type': 'circle', 'size': 4},
        })
        chart.set_size({'width': 640, 'height': 420})
        ws_charts.insert_chart(anchor, chart)

    # s'–t (effective)
    add_scatter("s′–t (Effective stress)", "s_effective", "t", "B2")
    # s–t (total)
    add_scatter("s–t (Total stress)",      "s_total",     "t", "B25")



# --------------------------------------------------------------------------------------
# Main app logic
# --------------------------------------------------------------------------------------
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

        st.markdown("---")
        st.subheader("s–t plot settings")
        stress_mode = st.radio("Stress path:", ["Effective (s'–t)", "Total (s–t)"], index=0)
        color_by = st.selectbox("Color points by:", ["TEST_TYPE", "HOLE_ID", "SOURCE_FILE"], index=0)
        facet_col = st.selectbox("Facet by (optional):", ["None", "TEST_TYPE", "SOURCE_FILE"], index=0)
        facet_col = None if facet_col == "None" else facet_col
        show_labels = st.checkbox("Show HOLE_ID labels", value=False)

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


        # Filters
        c1, c2, c3 = st.columns(3)
        with c1:
            holes = sorted([h for h in st_df["HOLE_ID"].dropna().unique()])
            pick_holes = st.multiselect("Filter HOLE_ID", holes, default=holes[: min(10, len(holes))])
        with c2:
            types = sorted([t for t in st_df["TEST_TYPE"].dropna().unique()])
            pick_types = st.multiselect("Filter TEST_TYPE", types, default=types)
        with c3:
            srcs = sorted([s for s in st_df["SOURCE_FILE"].dropna().unique()]) if "SOURCE_FILE" in st_df.columns else []
            pick_srcs = st.multiselect("Filter by SOURCE_FILE", srcs, default=srcs)

        fdf = st_df.copy()
        if pick_holes:
            fdf = fdf[fdf["HOLE_ID"].isin(pick_holes)]
        if pick_types:
            fdf = fdf[fdf["TEST_TYPE"].isin(pick_types)]
        if pick_srcs and "SOURCE_FILE" in fdf.columns:
            fdf = fdf[fdf["SOURCE_FILE"].isin(pick_srcs)]

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
