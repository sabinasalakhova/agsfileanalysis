# interval_utils.py
"""
Utilities for building continuous depth intervals and mapping attributes
from various AGS groups onto those intervals.
"""

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
    fill_method: str = 'ffill'
) -> pd.DataFrame:
    """
    Map values from a source group onto continuous intervals using overlap logic.
    Supports forward-fill within each borehole if requested.
    """
    if source_df.empty or value_col not in source_df.columns:
        return intervals.copy()

    required = [hole_col, source_from, source_to, value_col]
    if not all(c in source_df.columns for c in required[:3]):
        return intervals.copy()  # silently skip if source lacks depth info

    source = source_df[required].copy()
    source = source.rename(columns={source_from: 'src_from', source_to: 'src_to'})

    # Sort both tables
    intervals_sorted = intervals.sort_values(['DEPTH_FROM'])
    source_sorted = source.sort_values(['src_from'])

    merged = pd.merge_asof(
        intervals_sorted,
        source_sorted,
        left_on='DEPTH_FROM',
        right_on='src_from',
        by=hole_col,
        direction='forward',
        suffixes=('', '_source')
    )

    # Keep value only where real overlap exists
    overlap_mask = (
        (merged['DEPTH_FROM'] < merged['src_to']) &
        (merged['DEPTH_TO'] > merged['src_from'])
    )
    merged.loc[overlap_mask, value_col] = merged.loc[overlap_mask, value_col + '_source']

    # Optional forward-fill within each hole
    if fill_method == 'ffill':
        merged[value_col] = merged.groupby(hole_col)[value_col].ffill()

    # Clean up temporary columns
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
    selected_groups: Optional[List[str]] = None,
    hole_col: str = 'GIU_HOLE_ID',
    default_groups: List[str] = ['CORE', 'DETL', 'FRAC', 'GEOL', 'WETH']
) -> pd.DataFrame:
    """
    Combine selected AGS groups into a single continuous interval-based log.

    Parameters
    ----------
    combined_groups : Dict[str, pd.DataFrame]
        Output from your combine_groups() function
    selected_groups : list of str, optional
        Groups to include (default: CORE, DETL, FRAC, GEOL, WETH)
    hole_col : str
        Borehole identifier column
    """
    if selected_groups is None:
        selected_groups = default_groups

    # Collect all depth records from selected groups
    depth_records = []
    for g in selected_groups:
        if g in combined_groups and not combined_groups[g].empty:
            src = combined_groups[g]
            if 'DEPTH_FROM' in src.columns and 'DEPTH_TO' in src.columns:
                temp = src[[hole_col, 'DEPTH_FROM', 'DEPTH_TO']].copy()
                depth_records.append(temp)

    if not depth_records:
        return pd.DataFrame()

    depth_df = pd.concat(depth_records, ignore_index=True)

    # Build continuous intervals
    master_intervals = build_continuous_intervals(
        depth_df,
        hole_col=hole_col
    )

    # Define mapping: group → column to extract
    value_col_map = {
        'CORE': 'CORE_RQD',     # ← change to your actual column name
        'DETL': 'DETL_DESC',
        'FRAC': 'FRAC_FI',
        'GEOL': 'GEOL_DESC',    # or 'GEOL_LEG' ?
        'WETH': 'WETH_GRAD'
        # Add more as needed, e.g.:
        # 'SAMP': 'SAMP_TYPE',
        # 'TRIX': 'DEVF'
    }

    # Map values from each selected group
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
                    value_col=val_col
                )

    # Weathering simplification (if present)
    if 'WETH_GRAD' in master_intervals.columns:
        master_intervals['WETH'] = simplify_weathering_grade(master_intervals['WETH_GRAD'])

    # Final sort & reset index
    return master_intervals.sort_values([hole_col, 'DEPTH_FROM']).reset_index(drop=True)
    return master_intervals.sort_values([hole_col, 'DEPTH_FROM']).reset_index(drop=True)
