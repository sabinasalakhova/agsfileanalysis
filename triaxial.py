from typing import Dict, Tuple
import numpy as np
import pandas as pd
from scipy.stats import linregress

from .cleaners import coalesce_columns, to_numeric_safe, drop_singleton_rows

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core Table Builder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_triaxial_table(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Assumes input groups are already cleaned and standardized.
    Builds a merged triaxial summary table from AGS groups.
    """
    samp = groups.get("SAMP", pd.DataFrame()).copy()
    clss = groups.get("CLSS", pd.DataFrame()).copy()
    trig = groups.get("TRIG", pd.DataFrame()).copy()
    treg = groups.get("TREG", pd.DataFrame()).copy()
    trix = groups.get("TRIX", pd.DataFrame()).copy()
    tret = groups.get("TRET", pd.DataFrame()).copy()

    merge_keys = ["HOLE_ID"]
    if not samp.empty and "SPEC_DEPTH" in samp.columns:
        merge_keys.append("SPEC_DEPTH")

    merged = samp.copy() if not samp.empty else pd.DataFrame(columns=merge_keys).copy()

    if not clss.empty:
        merged = pd.merge(merged, clss, on=merge_keys, how="outer", suffixes=("", "_CLSS"))

    if not trig.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TRIG_TYPE"] if c in trig.columns]
        trig_f = trig[keep].copy()
        merged = pd.merge(merged, trig_f, on=[c for c in keep if c in merge_keys], how="outer")

    if not treg.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TREG_TYPE"] if c in treg.columns]
        treg_f = treg[keep].copy()
        merged = pd.merge(merged, treg_f, on=[c for c in keep if c in merge_keys], how="outer")

    tri_res = pd.concat([trix, tret], ignore_index=True) if not trix.empty or not tret.empty else pd.DataFrame()
    if not tri_res.empty:
        tri_keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF", "PWPF", "SOURCE_FILE"] if c in tri_res.columns]
        tri_res = tri_res[tri_keep].copy()
        merged = pd.merge(merged, tri_res, on=[c for c in ["HOLE_ID", "SPEC_DEPTH"] if c in merged.columns], how="outer")

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Lithology Mapping
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_triaxial_with_lithology(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Builds triaxial summary table and maps lithology from GIU group based on depth ranges.
    Optimized for large GIU files by grouping intervals by HOLE_ID.
    """
    triaxial_df = generate_triaxial_table(groups)
    giu = groups.get("GIU", pd.DataFrame()).copy()

    if giu.empty or "HOLE_ID" not in giu.columns:
        triaxial_df["LITHOLOGY"] = None
        return triaxial_df

    coalesce_columns(giu, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
    coalesce_columns(giu, ["DEPTH_TO", "END_DEPTH"], "DEPTH_TO")
    coalesce_columns(giu, ["GEOL_DESC", "GEOL_GEOL", "GEOL_GEO2"], "LITHOLOGY")
    to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])

    giu_by_hole = {
        hole: df.dropna(subset=["DEPTH_FROM", "DEPTH_TO"])
        for hole, df in giu.groupby("HOLE_ID")
    }

    def map_litho(row):
        hole = row.get("HOLE_ID")
        depth = row.get("SPEC_DEPTH")
        if pd.isna(hole) or pd.isna(depth):
            return None
        giu_rows = giu_by_hole.get(hole)
        if giu_rows is None:
            return None
        match = giu_rows[
            (giu_rows["DEPTH_FROM"] <= depth) &
            (giu_rows["DEPTH_TO"] >= depth)
        ]
        return match["LITHOLOGY"].iloc[0] if not match.empty else None

    triaxial_df["LITHOLOGY"] = triaxial_df.apply(map_litho, axis=1)
    return triaxial_df

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# s–t Value Calculation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def calculate_s_t_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates s and t values for triaxial test data.
    Works with both AGS3 (TRIX) and AGS4 (TRET) formats.
    """
    coalesce_columns(df, ["TRIX_CELL", "TRET_CELL"], "CELL")
    coalesce_columns(df, ["TRIX_DEVF", "TRET_DEVF"], "DEVF")
    coalesce_columns(df, ["TRIX_PWPF", "TRET_PWPF"], "PWPF")
    to_numeric_safe(df, ["CELL", "DEVF"])

    df["SIGMA_1"] = df["CELL"] + df["DEVF"]
    df["s"] = (df["SIGMA_1"] + df["CELL"]) / 2
    df["t"] = df["DEVF"] / 2

    return df

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Deduplication
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def remove_duplicate_tests(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate triaxial test rows based on key identifiers.
    """
    if df.empty:
        return df

    key_cols = [
        'HOLE_ID', 'SPEC_DEPTH', 'CELL', 'DEVF', 'PWPF',
        'TEST_TYPE', 'SOURCE_FILE'
    ]
    available_cols = [col for col in key_cols if col in df.columns]

    if len(available_cols) >= 3:
        temp_df = df[available_cols].copy()
        for col in temp_df.columns:
            if pd.api.types.is_float_dtype(temp_df[col]):
                temp_df[col] = temp_df[col].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")
            else:
                temp_df[col] = temp_df[col].astype(str)

        temp_df['combined'] = temp_df.apply(lambda row: '|'.join(row.values), axis=1)
        mask = ~temp_df.duplicated(subset=['combined'], keep='first')
        df = df[mask].reset_index(drop=True)

    return df
