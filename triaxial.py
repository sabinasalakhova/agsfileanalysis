from typing import Dict, Tuple
import numpy as np
import pandas as pd


from cleaners import coalesce_columns, to_numeric_safe, drop_singleton_rows, deduplicate_cell, expand_rows

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core Table Builder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_triaxial_table(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a single triaxial summary table from available AGS groups with robust merges.
    Uses safe merges that only join on keys present in both frames, normalises key dtypes,
    and falls back to concat when a sensible join is not possible.
    """
    samp = groups.get("SAMP", pd.DataFrame()).copy()
    clss = groups.get("CLSS", pd.DataFrame()).copy()
    trig = groups.get("TRIG", pd.DataFrame()).copy()
    treg = groups.get("TREG", pd.DataFrame()).copy()
    trix = groups.get("TRIX", pd.DataFrame()).copy()
    tret = groups.get("TRET", pd.DataFrame()).copy()

    # Normalize SPEC_DEPTH spellings and ensure HOLE_ID present
    for df in (samp, clss, trig, treg, trix, tret):
        if df.empty:
            continue
        rename_map = {c: "SPEC_DEPTH" for c in df.columns if c.upper() in {"SPEC_DPTH", "SPEC_DEPTH"}}
        if rename_map:
            df.rename(columns=rename_map, inplace=True)
        if "HOLE_ID" not in df.columns:
            df["HOLE_ID"] = np.nan

    merge_keys = ["HOLE_ID"]
    if not samp.empty and "SPEC_DEPTH" in samp.columns:
        merge_keys.append("SPEC_DEPTH")

    merged = samp.copy() if not samp.empty else pd.DataFrame(columns=merge_keys).copy()

    def _safe_merge(left: pd.DataFrame, right: pd.DataFrame, preferred_keys: List[str], suffixes=("", "")) -> pd.DataFrame:
        """
        Merge left and right safely:
        - use only keys present in both frames
        - normalise HOLE_ID to uppercase stripped strings, numeric keys to numeric
        - if keys contain non-scalar values or no common keys, fallback to concat
        """
        common = [k for k in preferred_keys if k in left.columns and k in right.columns]
        if not common:
            return pd.concat([left, right], axis=0, ignore_index=True, sort=False)

        # Normalize types for keys
        for k in common:
            if k.upper() in {"HOLE_ID", "LOCA_ID"}:
                left[k] = left[k].astype(str).str.upper().str.strip().replace({"nan": None})
                right[k] = right[k].astype(str).str.upper().str.strip().replace({"nan": None})
            else:
                left[k] = pd.to_numeric(left[k], errors="coerce")
                right[k] = pd.to_numeric(right[k], errors="coerce")

        # Reject non-scalar key values (lists, arrays) — fallback to concat
        def _has_non_scalar(df, keys):
            for key in keys:
                if df[key].apply(lambda v: isinstance(v, (list, tuple, set, pd.Series, np.ndarray))).any():
                    return True
            return False

        if _has_non_scalar(left, common) or _has_non_scalar(right, common):
            return pd.concat([left, right], axis=0, ignore_index=True, sort=False)

        return pd.merge(left, right, on=common, how="outer", suffixes=suffixes)

    # Merge CLSS (safe)
    if not clss.empty:
        merged = _safe_merge(merged, clss, merge_keys, suffixes=("", "_CLSS"))

    # Merge TRIG (safe, only keep relevant cols)
    if not trig.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TRIG_TYPE"] if c in trig.columns]
        trig_f = trig[keep].copy()
        merged = _safe_merge(merged, trig_f, merge_keys)

    # Merge TREG (safe, only keep relevant cols)
    if not treg.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TREG_TYPE"] if c in treg.columns]
        treg_f = treg[keep].copy()
        merged = _safe_merge(merged, treg_f, merge_keys)

    # combine TRIX and TRET results
    tri_res = pd.DataFrame()
    if not trix.empty:
        tri_res = trix.copy()
    if not tret.empty:
        tri_res = pd.concat([tri_res, tret.copy()], ignore_index=True) if not tri_res.empty else tret.copy()

    if not tri_res.empty:
        coalesce_columns(tri_res, ["SPEC_DEPTH", "SPEC_DPTH"], "SPEC_DEPTH")
        coalesce_columns(tri_res, ["HOLE_ID", "LOCA_ID"], "HOLE_ID")
        coalesce_columns(tri_res, ["TRIX_CELL", "TRET_CELL"], "CELL")
        coalesce_columns(tri_res, ["TRIX_DEVF", "TRET_DEVF"], "DEVF")
        coalesce_columns(tri_res, ["TRIX_PWPF", "TRET_PWPF"], "PWPF")
        tri_keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF", "PWPF", "SOURCE_FILE"] if c in tri_res.columns]
        tri_res = tri_res[tri_keep].copy()
        merged = _safe_merge(merged, tri_res, ["HOLE_ID", "SPEC_DEPTH"])

    cols_pref = [
        "HOLE_ID", "SAMP_ID", "SAMP_REF", "SAMP_TOP",
        "SPEC_REF", "SPEC_DEPTH", "SAMP_DESC", "SPEC_DESC", "GEOL_STAT",
        "TRIG_TYPE", "TREG_TYPE",
        "CELL", "DEVF", "PWPF", "SOURCE_FILE"
    ]
    final_cols = [c for c in cols_pref if c in merged.columns]
    final_df = merged[final_cols].copy() if final_cols else merged.copy()

    final_df = final_df.applymap(deduplicate_cell)
    expanded_df = expand_rows(final_df)
    expanded_df = drop_singleton_rows(expanded_df)
    to_numeric_safe(expanded_df, ["SPEC_DEPTH", "CELL", "DEVF", "PWPF"])

    return expanded_df
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
    Compute s and t for triaxial tests, preferring effective stresses when PWPF is present.
    Returns a new DataFrame with computed columns and a source flag for s:
      - SIGMA_1, SIGMA_3 (total)
      - SIGMA_1_EFF, SIGMA_3_EFF (effective; NaN if PWPF missing)
      - s_total, s_effective
      - s (chosen value) and s_source ("effective" or "total")
      - t (shear = DEVF/2)
      - valid (boolean)
    The function coalesces common AGS names into canonical columns, converts to numeric,
    and returns only columns useful for merging/plotting.
    """
    # operate on a copy to avoid mutating caller's DataFrame
    df = df.copy()

    # Coalesce expected alternate column names into canonical names.
    # Replace these helpers with your project utilities if available.
    def _coalesce(src_df, candidates, target):
        for c in candidates:
            if c in src_df.columns:
                src_df[target] = src_df.get(target).combine_first(src_df[c]) if target in src_df.columns else src_df[c]
        return src_df

    _coalesce(df, ["TRIX_CELL", "TRET_CELL", "CELL"], "CELL")
    _coalesce(df, ["TRIX_DEVF", "TRET_DEVF", "DEVF"], "DEVF")
    _coalesce(df, ["TRIX_PWPF", "TRET_PWPF", "PWPF"], "PWPF")

    # Safe numeric conversion
    for col in ["CELL", "DEVF", "PWPF"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute total principal stresses
    # SIGMA_1 = DEVF + CELL ; SIGMA_3 = CELL
    df["SIGMA_1"] = df.get("DEVF", 0.0) + df.get("CELL", 0.0)
    df["SIGMA_3"] = df.get("CELL", 0.0)

    # Compute effective stresses where PWPF is available (will be NaN if PWPF is NaN)
    df["SIGMA_1_EFF"] = df["SIGMA_1"] - df.get("PWPF")
    df["SIGMA_3_EFF"] = df["SIGMA_3"] - df.get("PWPF")

    # s and t (total and effective)
    df["s_total"]     = 0.5 * (df["SIGMA_1"] + df["SIGMA_3"])
    df["s_effective"] = 0.5 * (df["SIGMA_1_EFF"] + df["SIGMA_3_EFF"])
    df["t"]           = 0.5 * (df["SIGMA_1"] - df["SIGMA_3"])  # equal to DEVF/2 when values present

    # Choose s: use effective if both SIGMA_1_EFF and SIGMA_3_EFF are finite; else fall back to total
    has_eff = np.isfinite(df[["SIGMA_1_EFF", "SIGMA_3_EFF"]]).all(axis=1)
    df["s"] = df["s_effective"].where(has_eff, df["s_total"])
    df["s_source"] = np.where(has_eff, "effective", "total")

    # Validity flag for downstream filtering
    df["valid"] = np.isfinite(df[["s", "t"]]).all(axis=1)

    # Prepare output columns (include merge keys present in input)
    merge_cols = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "PWPF", "DEVF"] if c in df.columns]
    out_cols = merge_cols + [
        "SIGMA_1", "SIGMA_3", "SIGMA_1_EFF", "SIGMA_3_EFF",
        "s_total", "s_effective", "s", "s_source", "t", "valid"
    ]

    # Preserve TEST_TYPE, SOURCE_FILE, LITH if present
    for extra in ("TEST_TYPE", "SOURCE_FILE", "LITH"):
        if extra in df.columns:
            out_cols.append(extra)

    return df.loc[:, [c for c in out_cols if c in df.columns]]

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
