
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.upper().strip() for col in df.columns]
    return df


def drop_singleton_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Treat empty strings and whitespace as NaN
    pd.set_option('future.no_silent_downcasting', True)  # Add this line once at the beginning of the script
    clean = df.replace(r"^\s*$", np.nan, regex=True).infer_objects(copy=False)
    nn = clean.notna().sum(axis=1)
    return df.loc[nn > 1].reset_index(drop=True)

def deduplicate_cell(cell):
    if pd.isna(cell):
        return cell
    parts = [p.strip() for p in str(cell).split(" | ")]
    unique_parts = []
    for p in parts:
        if p and p not in unique_parts:
            unique_parts.append(p)
    return " | ".join(unique_parts)


def expand_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand rows where any cell contains ' | ' separated values,
    but skip expansion if all split values across columns are identical.
    """
    expanded_rows = []

    for _, row in df.iterrows():
        split_values = {
            col: (str(row[col]).split(" | ") if pd.notna(row[col]) else [""])
            for col in df.columns
        }

        # Check if all columns have the same repeated value
        all_same = all(
            len(set(values)) == 1 for values in split_values.values()
        )

        if all_same and all(len(values) > 1 for values in split_values.values()):
            # If all values are the same and repeated, keep as single row
            new_row = {col: split_values[col][0] for col in df.columns}
            expanded_rows.append(new_row)
        else:
            # Expand into multiple rows
            max_len = max(len(v) for v in split_values.values()) if split_values else 1
            for i in range(max_len):
                new_row = {
                    col: (split_values[col][i] if i < len(split_values[col]) else "")
                    for col in df.columns
                }
                expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows) 

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
