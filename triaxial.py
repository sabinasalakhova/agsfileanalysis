from .cleaners import drop_singleton_rows, expand_rows,coalesce_columns, to_numeric_safe, deduplicate_cell
from typing import Dict
import pandas as pd
import numpy as np

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
    trix = groups.get("TRIX", pd.DataFrame()).copy()  # AGS3 triaxial test results
    tret = groups.get("TRET", pd.DataFrame()).copy()  # AGS4 effective stress results

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

    # Add CLSS (outer)
    if not clss.empty:
        merged = pd.merge(merged, clss, on=merge_keys, how="outer", suffixes=("", "_CLSS"))

    # Add TRIG/TREG type info
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

    # Add TRIX/TRET result data (outer)
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
