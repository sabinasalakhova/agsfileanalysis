from typing import Dict, Tuple
import numpy as np
import pandas as pd

from cleaners import coalesce_columns, to_numeric_safe, drop_singleton_rows, deduplicate_cell, expand_rows

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core Table Builder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_triaxial_table(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a single triaxial summary table from available AGS groups:
    - SAMP / CLSS (optional)
    - TRIG (total stress general) or TREG (effective stress general)
    - TRIX (AGS3 results) or TRET (AGS4 results)
    Enhanced with fallbacks for HOLE_ID and depths.
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
        if not df.empty:
            df.columns = [col.upper().strip() for col in df.columns]
            to_numeric_safe(df, ["SAMP_TOP", "SAMP_BASE", "SPEC_DEPTH", "DEPTH_FROM", "DEPTH_TO"])

    # Step 1: Start with test results (TRIX/TRET priority)
    trix_tret = pd.concat([trix, tret], ignore_index=True) if not trix.empty or not tret.empty else pd.DataFrame()
    if trix_tret.empty:
        return pd.DataFrame()  # No triaxial data

    # Step 2: Join general test info (TRIG/TREG)
    trig_treg = pd.concat([trig, treg], ignore_index=True)
    if not trig_treg.empty:
        common_keys = [c for c in ['HOLE_ID', 'SAMP_REF', 'SPEC_REF'] if c in trix_tret.columns and c in trig_treg.columns]
        trix_tret = pd.merge(trix_tret, trig_treg, on=common_keys, how='left', suffixes=('', '_gen'))

    # Step 3: Join sample/classification (SAMP/CLSS)
    samp_clss = pd.concat([samp, clss], ignore_index=True)
    if not samp_clss.empty:
        common_keys = [c for c in ['HOLE_ID', 'SAMP_REF', 'SPEC_REF'] if c in trix_tret.columns and c in samp_clss.columns]
        trix_tret = pd.merge(trix_tret, samp_clss, on=common_keys, how='left', suffixes=('', '_samp'))

    # Step 4: Fallback for HOLE_ID (if still NaN, propagate from SAMP/LOCA if available)
    if 'HOLE_ID' not in trix_tret.columns or trix_tret['HOLE_ID'].isna().all():
        if 'LOCA' in groups and 'HOLE_ID' in groups['LOCA'].columns:
            # Join from LOCA if available (rare, but fallback)
            loca = groups.get("LOCA", pd.DataFrame())
            common_keys = [c for c in ['HOLE_ID', 'SAMP_REF'] if c in trix_tret.columns and c in loca.columns]
            trix_tret = pd.merge(trix_tret, loca[['HOLE_ID']], on=common_keys, how='left')

    # Step 5: Fallback & unify depths (add DEPTH_SOURCE for transparency)
    coalesce_columns(trix_tret, ["DEPTH_FROM", "SAMP_TOP"], "DEPTH_FROM")
    coalesce_columns(trix_tret, ["DEPTH_TO", "SAMP_BASE"], "DEPTH_TO")
    if 'SPEC_DEPTH' in trix_tret.columns:
        # If no interval, fallback to point depth
        trix_tret['DEPTH_FROM'] = trix_tret['DEPTH_FROM'].fillna(trix_tret['SPEC_DEPTH'])
        trix_tret['DEPTH_TO'] = trix_tret['DEPTH_TO'].fillna(trix_tret['SPEC_DEPTH'])
    
    trix_tret['DEPTH_SOURCE'] = np.where(
        trix_tret['DEPTH_FROM'].notna() & trix_tret['DEPTH_TO'].notna() & (trix_tret['DEPTH_FROM'] != trix_tret['DEPTH_TO']),
        'Interval (SAMP/DEPTH)',
        'Point (SPEC_DEPTH)'
    )

    # Final cleanup
    to_numeric_safe(trix_tret, ["DEPTH_FROM", "DEPTH_TO", "SPEC_DEPTH"])
    trix_tret = drop_singleton_rows(trix_tret)

    return trix_tret

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Deduplication (unchanged)
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
