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
    if not all(c in df.columns for c in [hole_col, depth_from_col, depth_to_col]):
        raise ValueError(f"Required columns missing: {hole_col}, {depth_from_col}, {depth_to_col}")

    # Collect all unique depth points per hole
    depth_points = (
        df[[hole_col, depth_from_col, depth_to_col]]
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
    Map values from a source group (e.g. GEOL, CORE, WETH) onto continuous intervals
    using interval overlap (any overlap → assign value).
    
    Supports ffill/bfill or first-match logic.
    """
    if source_df.empty:
        return intervals.copy()

    # Prepare source with interval boundaries
    source = source_df[[hole_col, source_from, source_to, value_col]].copy()
    source = source.rename(columns={source_from: 'src_from', source_to: 'src_to'})

    # Merge_asof style: find intervals that overlap with source records
    merged = pd.merge_asof(
        intervals.sort_values(['DEPTH_FROM']),
        source.sort_values(['src_from']),
        left_on='DEPTH_FROM',
        right_on='src_from',
        by=hole_col,
        direction='forward',
        suffixes=('', '_source')
    )

    # Keep only rows where intervals overlap source
    overlap_mask = (
        (merged['DEPTH_FROM'] < merged['src_to']) &
        (merged['DEPTH_TO'] > merged['src_from'])
    )
    merged.loc[overlap_mask, value_col] = merged.loc[overlap_mask, value_col + '_source']

    # Optional: forward-fill within hole if needed
    if fill_method == 'ffill':
        merged[value_col] = merged.groupby(hole_col)[value_col].ffill()

    return merged.drop(columns=[c for c in merged.columns if '_source' in c or c in ['src_from', 'src_to']])


def simplify_weathering_grade(series: pd.Series) -> pd.Series:
    """
    Convert detailed weathering grades to simplified Roman numeral.
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
    uploaded_excel_files: List,
    selected_groups: Optional[List[str]] = None,
    hole_col: str = 'GIU_HOLE_ID',
    default_groups: List[str] = ['CORE', 'DETL', 'FRAC', 'GEOL', 'WETH']
) -> pd.DataFrame:
    """
    Modern, vectorized version of combining AGS Excel sheets into one continuous
    interval-based geological log per borehole.
    
    Parameters
    ----------
    uploaded_excel_files : list of file-like objects
        Excel files (each with sheets like HOLE, CORE, GEOL, etc.)
    selected_groups : list of str, optional
        Which groups/sheets to process (default: CORE, DETL, FRAC, GEOL, WETH)
    hole_col : str
        Column name for borehole identifier (usually 'GIU_HOLE_ID')
    
    Returns
    -------
    pd.DataFrame
        Continuous intervals with mapped attributes from all groups
    """
    if selected_groups is None:
        selected_groups = default_groups

    # Full group list we care about
    all_groups = ['HOLE'] + selected_groups

    master_intervals = None
    all_source_dfs = {}

    for file_obj in uploaded_excel_files:
        file_name = getattr(file_obj, 'name', 'unknown.xlsx')

        # Read all requested sheets
        group_dict = {}
        for g in all_groups:
            try:
                group_dict[g] = pd.read_excel(file_obj, sheet_name=g)
            except ValueError:
                group_dict[g] = pd.DataFrame()

        if 'HOLE' not in group_dict or group_dict['HOLE'].empty:
            continue

        # Normalize column names early
        for g in group_dict:
            if not group_dict[g].empty:
                group_dict[g].columns = group_dict[g].columns.str.strip().str.upper()

        # Collect all depth intervals across all groups
        depth_df = pd.DataFrame()
        for g in selected_groups:
            if g not in group_dict or group_dict[g].empty:
                continue
            src = group_dict[g]
            if 'DEPTH_FROM' in src.columns and 'DEPTH_TO' in src.columns:
                depth_df = pd.concat([depth_df, src[['DEPTH_FROM', 'DEPTH_TO']]], ignore_index=True)

        if depth_df.empty:
            continue

        # Build continuous intervals for this file
        intervals = build_continuous_intervals(
            depth_df.assign(**{hole_col: group_dict['HOLE'][hole_col].iloc[0]}),
            hole_col=hole_col
        )

        # Merge attributes from each source group
        for group_name in selected_groups:
            if group_name not in group_dict or group_dict[group_name].empty:
                continue

            src_df = group_dict[group_name]
            if 'DEPTH_FROM' not in src_df.columns or 'DEPTH_TO' not in src_df.columns:
                continue

            # Define which column to pull from each group
            value_col_map = {
                'CORE': 'CORE_RQD',     # example – adjust to your actual columns
                'DETL': 'DETL_DESC',
                'FRAC': 'FRAC_FI',
                'GEOL': 'GEOL_DESC',    # or 'GEOL_LEG' ?
                'WETH': 'WETH_GRAD'
            }

            if group_name in value_col_map:
                val_col = value_col_map[group_name]
                if val_col in src_df.columns:
                    intervals = map_group_to_intervals(
                        intervals,
                        src_df,
                        hole_col=hole_col,
                        value_col=val_col
                    )

        # Accumulate
        if master_intervals is None:
            master_intervals = intervals
        else:
            master_intervals = pd.concat([master_intervals, intervals], ignore_index=True)

    if master_intervals is None or master_intervals.empty:
        return pd.DataFrame()

    # Final cleanup & weathering simplification
    if 'WETH_GRAD' in master_intervals.columns:
        master_intervals['WETH'] = simplify_weathering_grade(master_intervals['WETH_GRAD'])

    # Add any other final columns you want (e.g. FI, Details, GEOL, TCR, RQD...)
    # They should already be mapped if you added them to value_col_map

    return master_intervals.sort_values([hole_col, 'DEPTH_FROM']).reset_index(drop=True)
