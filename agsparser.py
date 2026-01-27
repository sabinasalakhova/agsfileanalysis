from typing import Dict,Optional, List, Tuple
import pandas as pd
import csv
import io
import streamlit as st

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PARSING ENGINE (v2.0)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _split_quoted_csv(line: str) -> List[str]:
    """
    Parses a single CSV line using the standard csv library.
    This handles quotes ("val", "val"), escaped quotes ("val""val"), 
    and empty fields (,,) correctly, unlike simple string splitting.
    """
    if not line or not line.strip():
        return []
    try:
        # skipinitialspace allows "Val", "Val" to be parsed correctly
        reader = csv.reader(io.StringIO(line.strip()), strict=False, skipinitialspace=True)
        return next(reader)
    except Exception:
        return []

def find_hole_id_column(columns: List[str]) -> Optional[str]:
    """Identify HOLE_ID or common variants in a list of columns."""
    # Create a map of uppercase column names to original names
    uc_map = {str(c).upper(): c for c in columns}

    # List of common primary keys for boreholes
    candidates = [
        "HOLE_ID", "HOLEID", "HOLE", "LOCA_ID", "LOCATION_ID"
    ]

    for cand in candidates:
        if cand in uc_map:
            return uc_map[cand]

    return None


def _normalize_token(token: str) -> str:
    if token is None:
        return ""
    # Strip whitespace, quotes, and any stray BOM at the start of a line
    return token.strip().strip('"').lstrip("\ufeff").upper()

def analyze_ags_content(file_bytes: bytes) -> Dict[str, str]:
    """
    Quickly scans the file to determine AGS version and key features.
    """
    results = {
        "AGS3": "No",
        "AGS4": "No",
        'Contains "LOCA"': "No",
        "Contains **HOLE": "No"
    }
    try:
        content = file_bytes.decode("latin-1", errors="ignore")
        # Quick scan of first 50 lines usually suffices for detection
        lines = content.splitlines()[:50]
        for line in lines:
            s = line.strip()
            if not s: continue
            
            # Use raw string check for speed in analyzer
            if 'GROUP' in s and 'LOCA' in s:
                results['Contains "LOCA"'] = "Yes"
            if '**HOLE' in s:
                results["Contains **HOLE"] = "Yes"
                
            # Version checks
            if s.startswith('"GROUP"') or s.startswith("GROUP"):
                results["AGS4"] = "Yes"
            if s.startswith('"**') or s.startswith("**"):
                results["AGS3"] = "Yes"
    except Exception:
        pass
    return results

def parse_ags_file(file_bytes: bytes, file_name: str) -> Dict[str, pd.DataFrame]:
    """
    Main parser. Reads AGS3/4 files, handling continuation lines and 
    split headings robustly.
    """
    # 1. Detect Version
    analysis = analyze_ags_content(file_bytes)
    is_ags3 = analysis.get("AGS3") == "Yes"
    is_ags4 = analysis.get("AGS4") == "Yes"

    # 2. Decode and Clean Lines
    text = file_bytes.decode("latin-1", errors="ignore")
    raw_lines = text.splitlines()
    
    # 3. Initialize State
    group_data: Dict[str, List[Dict[str, str]]] = {}
    group_headings: Dict[str, List[str]] = {}
    
    current_group = None
    headings: List[str] = []
    data_started = False  # Flag: Have we moved from HEADINGS to DATA in this group?
    
    parse_errors = []

    def ensure_group(name: str):
        group_data.setdefault(name, [])

    def append_continuation(parts: List[str]):
        """
        Merges a <CONT> line into the *previous* data row.
        """
        # Safety: Need a group, headings, and at least one previous data row
        if not (current_group and headings and group_data[current_group]):
            return

        # Pad parts to preserve positional indexing for empty fields.
        # AGS4: parts[0] is "DATA"; AGS3: parts[0] is "<CONT>"
        expected_len = len(headings) + 1
        if len(parts) < expected_len:
            parts = parts + [""] * (expected_len - len(parts))

        # AGS4 Rule: <CONT> replaces the "DATA" keyword.
        # "DATA" is index 0. Data starts at index 1.
        # So parts[1] maps to Headings[0]. Shift = -1.
        if is_ags4:
            for i in range(1, expected_len):
                heading_index = i - 1
                _merge_val(heading_index, parts[i])

        # AGS3 Rule: <CONT> replaces the first variable (Index 0).
        # parts[0] is <CONT>. parts[1] is Variable 1.
        # So parts[1] maps to Headings[1]. No Shift.
        else:
            for i in range(1, expected_len):
                heading_index = i
                _merge_val(heading_index, parts[i])

    def _merge_val(idx: int, val: str):
        if idx >= len(headings):
            return
        if val is None:
            return
        # Only merge non-empty values; empty placeholders must NOT shift indices.
        if not str(val).strip():
            return

        val = str(val).strip()
        field = headings[idx]
        # Get existing value for this field in the last row
        last_row = group_data[current_group][-1]
        prev = last_row.get(field, "")
        
        # Append only if unique (avoid duplicating "Val | Val")
        existing_parts = [p.strip() for p in prev.split(" | ") if p]
        if val not in existing_parts:
            last_row[field] = f"{prev} | {val}" if prev else val

    # 4. Main Parsing Loop
    for i, line in enumerate(raw_lines):
        # Skip empty lines or purely whitespace
        if not line.strip():
            continue
            
        parts = _split_quoted_csv(line)
        if not parts:
            continue

        # Normalized Keyword (First token, uppercase)
        keyword = _normalize_token(parts[0])

        # --- A. Handle Continuation Lines (Global Priority) ---
        # Handle normal <CONT> in column 0
        if keyword == "<CONT>":
            append_continuation(parts)
            continue

        # Handle malformed lines where <CONT> is in column 1 (column 0 empty)
        if keyword == "" and len(parts) > 1 and _normalize_token(parts[1]) == "<CONT>":
            # Drop the leading empty field so <CONT> becomes index 0
            parts = parts[1:]
            append_continuation(parts)
            continue
            
        # --- B. Skip Metadata/Units ---
        if keyword in ["<UNITS>", "UNIT", "<UNIT>", "PROJ", "ABBR"]: 
            # Note: PROJ/ABBR here filters out old-style header garbage if present, 
            # but real groups start with "**" or "GROUP".
            pass 
        if keyword in ["<UNITS>", "UNIT", "<UNIT>"]:
            continue

        # --- C. AGS4 Logic ---
        if is_ags4:
            if keyword == "GROUP":
                # Start New Group
                if len(parts) > 1:
                    current_group = parts[1]
                    ensure_group(current_group)
                    headings = []
                    data_started = False
            
            elif keyword == "HEADING":
                # Handle Headings (including Split Headings Rule 13)
                new_headings = parts[1:]
                if not data_started and headings:
                    # If we already have headings but haven't seen DATA yet, this is a continuation
                    headings.extend(new_headings)
                else:
                    headings = new_headings
                if current_group:
                    group_headings[current_group] = headings
            
            elif keyword == "DATA":
                # Data Row
                if current_group and headings:
                    data_started = True
                    # Zip matches parts[1] to headings[0]
                    row_dict = dict(zip(headings, parts[1:]))
                    group_data[current_group].append(row_dict)

        # --- D. AGS3 Logic ---
        elif is_ags3:
            if keyword.startswith("**"):
                # Start New Group
                current_group = keyword[2:]  # Remove "**"
                ensure_group(current_group)
                headings = []
                data_started = False
            
            elif keyword.startswith("*"):
                # Handle Headings
                new_headings = [p.lstrip("*") for p in parts if p.strip()]
                
                # Rule 13: Split Headings
                if not data_started and headings:
                    headings.extend(new_headings)
                else:
                    headings = new_headings
                
                if current_group:
                    group_headings[current_group] = headings
            
            elif current_group and headings:
                # Data Row (No keyword in AGS3, just values)
                data_started = True
                # Zip matches parts[0] to headings[0]
                row_dict = dict(zip(headings, parts[:len(headings)]))
                group_data[current_group].append(row_dict)

    # 5. Display Warnings
    if parse_errors:
        st.warning(f"⚠️ Warning: {len(parse_errors)} lines failed to parse.")

        # 6. Final DataFrame Construction
    group_dfs = {}

    for group_name, rows in group_data.items():
        df = pd.DataFrame(rows)
        if not df.empty:
            # Normalize odd column headings early
            rename_map = {
                "?ETH": "WETH",
                "?ETH_TOP": "WETH_TOP",
                "?ETH_BASE": "WETH_BASE",
                "?ETH_GRAD": "WETH_GRAD",
                "?LEGD": "LEGD",
                "?HORN": "HORN",
            }
            df = df.rename(columns=rename_map)

            # Add source file column using the provided file name
            df["SOURCE_FILE"] = file_name

        group_dfs[group_name] = df
    return group_dfs
