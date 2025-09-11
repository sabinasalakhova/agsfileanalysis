
from typing import Dict
import pandas as pd


def generate_triaxial_table(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Assumes input groups are already cleaned and standardized.
    Builds a merged triaxial summary table from AGS groups.
    """

    # Get groups
    samp = groups.get("SAMP", pd.DataFrame()).copy()
    clss = groups.get("CLSS", pd.DataFrame()).copy()
    trig = groups.get("TRIG", pd.DataFrame()).copy()
    treg = groups.get("TREG", pd.DataFrame()).copy()
    trix = groups.get("TRIX", pd.DataFrame()).copy()
    tret = groups.get("TRET", pd.DataFrame()).copy()

    # Merge keys
    merge_keys = ["HOLE_ID"]
    if not samp.empty and "SPEC_DEPTH" in samp.columns:
        merge_keys.append("SPEC_DEPTH")

    merged = samp.copy() if not samp.empty else pd.DataFrame(columns=merge_keys).copy()

    # Add CLSS
    if not clss.empty:
        merged = pd.merge(merged, clss, on=merge_keys, how="outer", suffixes=("", "_CLSS"))

    # Add TRIG/TREG type info
    if not trig.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TRIG_TYPE"] if c in trig.columns]
        trig_f = trig[keep].copy()
        merged = pd.merge(merged, trig_f, on=[c for c in keep if c in merge_keys], how="outer")
    if not treg.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TREG_TYPE"] if c in treg.columns]
        treg_f = treg[keep].copy()
        merged = pd.merge(merged, treg_f, on=[c for c in keep if c in merge_keys], how="outer")

    # Combine TRIX and TRET
    tri_res = pd.concat([trix, tret], ignore_index=True) if not trix.empty or not tret.empty else pd.DataFrame()
    if not tri_res.empty:
        tri_keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF", "PWPF", "SOURCE_FILE"] if c in tri_res.columns]
        tri_res = tri_res[tri_keep].copy()
        merged = pd.merge(merged, tri_res, on=[c for c in ["HOLE_ID", "SPEC_DEPTH"] if c in merged.columns], how="outer")

    # Final column subset
    cols_pref = [
        "HOLE_ID", "SAMP_ID", "SAMP_REF", "SAMP_TOP",
        "SPEC_REF", "SPEC_DEPTH", "DEPTH_FROM", "DEPTH_TO",
        "SAMP_DESC", "SPEC_DESC", "GEOL_STAT",
        "TRIG_TYPE", "TREG_TYPE",
        "CELL", "DEVF", "PWPF", "SOURCE_FILE"
    ]
    final_cols = [c for c in cols_pref if c in merged.columns]
    final_df = merged[final_cols].copy() if final_cols else merged.copy()

    return final_df

