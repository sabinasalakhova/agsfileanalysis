from typing import Dict
import pandas as pd 
import io
import re
from cleaners import drop_singleton_rows

def build_all_groups_excel(groups: Dict[str, pd.DataFrame]) -> bytes:
    """
    Create an Excel workbook where each group is one sheet.
    Sanitizes sheet names to prevent Excel errors.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as xw:
        for gname, gdf in sorted(groups.items()):
            if gdf is None or gdf.empty:
                continue
        
            # apply column heading fixes
            gdf = gdf.rename(columns=rename_map)
        
            # apply sheet name fixes
            gname = rename_map.get(gname, gname)
        
            # sanitize + truncate as before
            safe_name = re.sub(r"[\[\]:*?/\\]", "_", gname)
            sheet_name = safe_name[:31]
            
            # Clean rows (no singleton)
            out = drop_singleton_rows(gdf)
            out.to_excel(xw, index=False, sheet_name=sheet_name)
    return buffer.getvalue()



def add_st_charts_to_excel(writer: pd.ExcelWriter, st_df: pd.DataFrame, sheet_name: str = "s_t_Values"):
    """
    Adds two scatter charts to a new sheet "Charts" based on the table written to sheet_name.
    Assumes st_df has been written to writer with sheet_name already.
    """
    if st_df is None or st_df.empty:
        return

    # workbook / sheets from the ExcelWriter (xlsxwriter)
    workbook = writer.book
    sheets = writer.sheets

    # must have the values sheet already written
    if sheet_name not in sheets:
        return

    # Row/col counts in the written sheet
    nrows = len(st_df)
    if nrows == 0:
        return

    # Column index map (zero-based) based on DataFrame columns (matches Excel write order)
    idx = {c: i for i, c in enumerate(st_df.columns)}

    # must have t and at least one of s_effective/s_total
    if "t" not in idx or ("s_effective" not in idx and "s_total" not in idx):
        return

    # Data rows in the sheet: header is row 0, data starts at row 1 and ends at row 1 + nrows - 1
    r0 = 1
    r1 = r0 + nrows - 1

    # Create Charts worksheet (avoid name clash)
    charts_name = "Charts"
    # if Charts exists, remove or use new name
    if charts_name in sheets:
        ws_charts = sheets[charts_name]
    else:
        ws_charts = workbook.add_worksheet(charts_name)
        # Not adding to writer.sheets mapping here; it's OK for charts insertion

    def add_scatter(title: str, xcol: str, ycol: str, anchor: str):
        if xcol not in idx or ycol not in idx:
            return

        cx, cy = idx[xcol], idx[ycol]

        chart = workbook.add_chart({'type': 'scatter', 'subtype': 'marker_only'})
        chart.set_title({'name': title})
        chart.set_x_axis({'name': 's (kPa)'})
        chart.set_y_axis({'name': 't = q/2 (kPa)'})
        chart.set_legend({'none': True})

        # Add series using sheet_name and zero-based (row, col) ranges
        chart.add_series({
            'name':       title,
            'categories': [sheet_name, r0, cx, r1, cx],  # x-values
            'values':     [sheet_name, r0, cy, r1, cy],  # y-values
            'marker':     {'type': 'circle', 'size': 4},
        })
        chart.set_size({'width': 640, 'height': 420})
        ws_charts.insert_chart(anchor, chart)

    # s'–t (effective)
    add_scatter("s′–t (Effective stress)", "s_effective", "t", "B2")
    # s–t (total)
    add_scatter("s–t (Total stress)", "s_total", "t", "B25")
    
def remove_duplicate_tests(df: pd.DataFrame) -> pd.DataFrame:
 
    if df.empty:
        return df
    
    # Define key columns that should be unique for each test
    key_cols = [
        'HOLE_ID', 'SPEC_DEPTH', 'CELL', 'DEVF', 'PWPF', 
        'TEST_TYPE', 'SOURCE_FILE'
    ]
    
    # Use only columns that actually exist in the DataFrame
    available_cols = [col for col in key_cols if col in df.columns]
    
    # If we have at least 3 identifying columns, use them for deduplication
    if len(available_cols) >= 3:
        # Create a copy of the relevant columns
        temp_df = df[available_cols].copy()
        
        # Convert float columns to rounded strings for consistent comparison
        for col in temp_df.columns:
            if pd.api.types.is_float_dtype(temp_df[col]):
                # Round floats to 2 decimal places and convert to string
                temp_df[col] = temp_df[col].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")
            else:
                # Convert non-float columns to string
                temp_df[col] = temp_df[col].astype(str)
        
        # Create a combined string representation for each row
        temp_df['combined'] = temp_df.apply(lambda row: '|'.join(row.values), axis=1)
        
        # Identify duplicates while keeping the first occurrence
        mask = ~temp_df.duplicated(subset=['combined'], keep='first')
        df = df[mask].reset_index(drop=True)
    
    return df
