from typing import Dict
import pandas as pd 
import io
from cleaners import drop_singleton_rows

def build_all_groups_excel(groups: Dict[str, pd.DataFrame]) -> bytes:
    """
    Create an Excel workbook where each group is one sheet.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as xw:
        for gname, gdf in sorted(groups.items()):
            if gdf is None or gdf.empty:
                continue
            # Excel sheet name limit and avoid duplicates
            sheet_name = gname[:31]
            # Clean rows (no singleton)
            out = drop_singleton_rows(gdf)
            out.to_excel(xw, index=False, sheet_name=sheet_name)
    return buffer.getvalue()



def add_st_charts_to_excel(writer: pd.ExcelWriter, st_df: pd.DataFrame, sheet_name: str = "s_t_Values"):
    """
    Adds two charts to the workbook:
      - s'–t (effective): x = s_effective, y = t
      - s–t (total)    : x = s_total,     y = t
    Places them on a new sheet 'Charts'.
    """
    if st_df is None or st_df.empty:
        return

    workbook  = writer.book
    ws_vals   = writer.sheets.get(sheet_name)
    if ws_vals is None:
        return

    # Row/col counts in the written sheet
    nrows = len(st_df)
    if nrows == 0:
        return

    # Column indices
    idx = {c: i for i, c in enumerate(st_df.columns)}
    if "t" not in idx or ("s_effective" not in idx and "s_total" not in idx):
        return  # nothing to plot

    # Data starts at row=1 (row 0 is header)
    r0, r1 = 1, nrows

    # Create Charts worksheet
    ws_charts = workbook.add_worksheet("Charts")

    def add_scatter(title: str, xcol: str, ycol: str, anchor: str):
        if xcol not in idx or ycol not in idx:
            return
        cx, cy = idx[xcol], idx[ycol]

        chart = workbook.add_chart({'type': 'scatter', 'subtype': 'marker_only'})
        chart.set_title({'name': title})
        chart.set_x_axis({'name': 's (kPa)'})
        chart.set_y_axis({'name': "t = q/2 (kPa)"})
        chart.set_legend({'none': True})

        # A1 notation ranges for x/y
        sheet = sheet_name
        # Excel is col letters; build ranges using XlsxWriter utility
        # We'll use row/col notation instead (zero-based, inclusive)
        chart.add_series({
            'name':       title,
            'categories': [sheet, r0, cx, r1, cx],  # x-values
            'values':     [sheet, r0, cy, r1, cy],  # y-values
            'marker':     {'type': 'circle', 'size': 4},
        })
        chart.set_size({'width': 640, 'height': 420})
        ws_charts.insert_chart(anchor, chart)

    # s'–t (effective)
    add_scatter("s′–t (Effective stress)", "s_effective", "t", "B2")
    # s–t (total)
    add_scatter("s–t (Total stress)",      "s_total",     "t", "B25")
    
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
