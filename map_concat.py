


import pandas as pd
import numpy as np
from typing import List, Optional, Dict


def build_continuous_intervals(
    df: pd.DataFrame,
    hole_col: str = 'GIU_HOLE_ID',
    depth_from_col: str = 'DEPTH_FROM',
    depth_to_col: str = 'DEPTH_TO'
) -> pd.DataFrame:
    """
    Create a non-overlapping, continuous depth interval table per borehole
    using ALL unique depth boundaries from the input data.

    Returns
    -------
    pd.DataFrame
        Columns: [hole_col, 'DEPTH_FROM', 'DEPTH_TO', 'THICKNESS_M']
    """
    required = [hole_col, depth_from_col, depth_to_col]
    if not all(c in df.columns for c in required):
        raise ValueError(f"Required columns missing: {', '.join(required)}")

    # Collect all unique depth points per hole
    depth_points = (
        df[required]
        .melt(id_vars=[hole_col], value_name='depth')
        .dropna(subset=['depth'])
        .groupby(hole_col)['depth']
        .apply(lambda x: sorted(x.unique()))
        .reset_index(name='depth_points')
    )

    intervals = []
    for _, row in depth_points.iterrows():
        hole = row[hole_col]
        points = row['depth_points']
        if len(points) < 2:
            continue

        df_int = pd.DataFrame({
            hole_col: hole,
            'DEPTH_FROM': points[:-1],
            'DEPTH_TO': points[1:],
        })
        df_int['THICKNESS_M'] = df_int['DEPTH_TO'] - df_int['DEPTH_FROM']
        intervals.append(df_int)

    if not intervals:
        return pd.DataFrame(columns=[hole_col, 'DEPTH_FROM', 'DEPTH_TO', 'THICKNESS_M'])

    return pd.concat(intervals, ignore_index=True)


def map_group_to_intervals(
    intervals: pd.DataFrame,
    source_df: pd.DataFrame,
    hole_col: str,
    value_col: str,
    source_from: str = 'DEPTH_FROM',
    source_to: str = 'DEPTH_TO',
    fill_method: str = 'ffill',
    legacy_mode: bool = False
) -> pd.DataFrame:
    """
    Map values from a source group onto continuous intervals using overlap logic.
    Supports forward-fill and optional legacy .between() mode for robustness.
    """
    if source_df.empty or value_col not in source_df.columns:
        return intervals.copy()

    required = [hole_col, source_from, source_to, value_col]
    if not all(c in source_df.columns for c in required[:3]):
        return intervals.copy()  # skip if source lacks depth info

    if legacy_mode:
        # Legacy-style: per-hole loop with .between()
        result = intervals.copy()
        result[value_col] = np.nan
        for hole in result[hole_col].unique():
            hole_int = result[result[hole_col] == hole]
            hole_src = source_df[source_df[hole_col] == hole]
            for _, src_row in hole_src.iterrows():
                mask = (
                    (hole_int['DEPTH_FROM'] >= src_row[source_from]) &
                    (hole_int['DEPTH_FROM'] < src_row[source_to])
                )
                result.loc[hole_int[mask].index, value_col] = src_row[value_col]
        if fill_method == 'ffill':
            result[value_col] = result.groupby(hole_col)[value_col].ffill()
        return result

    else:
        # Original vectorized merge_asof
        source = source_df[required].copy()
        source = source.rename(columns={source_from: 'src_from', source_to: 'src_to'})

        merged = pd.merge_asof(
            intervals.sort_values(['DEPTH_FROM']),
            source.sort_values(['src_from']),
            left_on='DEPTH_FROM',
            right_on='src_from',
            by=hole_col,
            direction='forward',
            suffixes=('', '_source')
        )

        overlap_mask = (
            (merged['DEPTH_FROM'] < merged['src_to']) &
            (merged['DEPTH_TO'] > merged['src_from'])
        )
        merged.loc[overlap_mask, value_col] = merged.loc[overlap_mask, value_col + '_source']

        if fill_method == 'ffill':
            merged[value_col] = merged.groupby(hole_col)[value_col].ffill()

        drop_cols = [c for c in merged.columns if c.endswith('_source') or c in ['src_from', 'src_to']]
        return merged.drop(columns=drop_cols)


def simplify_weathering_grade(series: pd.Series) -> pd.Series:
    """
    Convert detailed weathering grades (e.g. III/IV) to simplified Roman numeral.
    """
    grade_map = {
        r'^I(/II)?$': 'I',
        r'^II(/III)?$': 'II',
        r'^III(/IV)?$': 'III',
        r'^(IV(/V)?|III/IV|IV/III)': 'IV',
        r'^(V(/VI)?|IV/V|V/IV)': 'V',
        r'^VI(/V)?$': 'VI',
    }

    simplified = series.astype(str).str.strip()
    for pattern, value in grade_map.items():
        simplified = simplified.str.replace(pattern, value, regex=True)

    return simplified


def combine_ags_data(
    combined_groups: Dict[str, pd.DataFrame],
    giu_df: Optional[pd.DataFrame] = None,
    selected_groups: Optional[List[str]] = None,
    hole_col: str = 'GIU_HOLE_ID',
    default_groups: List[str] = ['CORE', 'DETL', 'FRAC', 'GEOL', 'WETH'],
    legacy_fill: bool = True  # Use legacy .between() for reliability
) -> pd.DataFrame:
    if selected_groups is None:
        selected_groups = default_groups

    # ── 1. Prepare depth records with fallback unification ──────────────────────
    depth_records = []

    for g in selected_groups:
        if g not in combined_groups or combined_groups[g].empty:
            continue

        src = combined_groups[g].copy()

        # Fallback depth columns (point → interval)
        coalesce_columns(src, ["DEPTH_FROM", "SAMP_TOP", "SAMPLE_TOP", "START_DEPTH"], "DEPTH_FROM")
        coalesce_columns(src, ["DEPTH_TO",   "SAMP_BASE", "SAMPLE_BASE", "END_DEPTH"],   "DEPTH_TO")

        # If only point depth exists → make it interval (DEPTH_FROM = DEPTH_TO)
        point_cols = ["SPEC_DEPTH", "SAMPLE_DEPTH", "TEST_DEPTH"]
        for pc in point_cols:
            if pc in src.columns and 'DEPTH_FROM' not in src.columns:
                src['DEPTH_FROM'] = src[pc]
                src['DEPTH_TO']   = src[pc]

        # Ensure numeric
        to_numeric_safe(src, ["DEPTH_FROM", "DEPTH_TO"])

        # Only keep rows that have some depth info
        has_depth = src["DEPTH_FROM"].notna() | src["DEPTH_TO"].notna()
        if has_depth.any():
            temp = src[has_depth][[hole_col, "DEPTH_FROM", "DEPTH_TO"]].copy()
            temp["SOURCE_GROUP"] = g  # helpful for debugging
            depth_records.append(temp)

    if not depth_records:
        raise ValueError("No depth information found in selected groups.")

    depth_df = pd.concat(depth_records, ignore_index=True)

    # ── 2. Build continuous intervals ──────────────────────────────────────────
    master_intervals = build_continuous_intervals(
        depth_df,
        hole_col=hole_col
    )

    # ── 3. Map attributes from each group ──────────────────────────────────────
    value_col_map = {
        'CORE': 'CORE_RQD',
        'DETL': 'DETL_DESC',
        'FRAC': 'FRAC_FI',
        'GEOL': 'GEOL_DESC',
        'WETH': 'WETH_GRAD'
    }

    for group_name in selected_groups:
        if group_name not in combined_groups or combined_groups[group_name].empty:
            continue

        src_df = combined_groups[group_name]
        if 'DEPTH_FROM' not in src_df.columns or 'DEPTH_TO' not in src_df.columns:
            continue

        if group_name in value_col_map:
            val_col = value_col_map[group_name]
            if val_col in src_df.columns:
                master_intervals = map_group_to_intervals(
                    master_intervals,
                    src_df,
                    hole_col=hole_col,
                    value_col=val_col,
                    legacy_mode=legacy_fill  # Use legacy for reliability
                )

    # ── 4. Map LITH from GIU (if provided) ─────────────────────────────────────
    if giu_df is not None and not giu_df.empty and 'LITH' in giu_df.columns:
        giu = giu_df.copy()
        giu.columns = giu.columns.str.strip().str.upper()
        coalesce_columns(giu, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
        coalesce_columns(giu, ["DEPTH_TO", "END_DEPTH"], "DEPTH_TO")
        to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])

        master_intervals = map_group_to_intervals(
            master_intervals,
            giu,
            hole_col=hole_col,
            value_col='LITH',
            source_from='DEPTH_FROM',
            source_to='DEPTH_TO',
            legacy_mode=legacy_fill
        )

    # ── 5. Weathering simplification ───────────────────────────────────────────
    if 'WETH_GRAD' in master_intervals.columns:
        master_intervals['WETH'] = simplify_weathering_grade(master_intervals['WETH_GRAD'])

    # ── 6. Final cleanup ───────────────────────────────────────────────────────
    master_intervals = master_intervals.sort_values([hole_col, 'DEPTH_FROM']).reset_index(drop=True)

    return master_intervals
