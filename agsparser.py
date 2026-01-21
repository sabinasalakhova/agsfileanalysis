from typing import Dict, List, Tuple
import re
import pandas as pd
import csv
import io
import streamlit as st

# --------------------------------------------------------------------------------------
### split quotes in the AGS
# --------------------------------------------------------------------------------------
def _split_quoted_csv(line: str) -> List[str]:
    """
    Robustly splits a CSV line using the csv module.
    Handles quotes, empty fields, and escaped characters correctly.
    """
    s = line.strip()
    if not s:
        return []

    try:
        # strict=False allows for some leniency
        # skipinitialspace=True handles cases like: "Val", "Val"
        reader = csv.reader(io.StringIO(s), strict=False, skipinitialspace=True)
        return next(reader)
    except Exception:
        return []

# --------------------------------------------------------------------------------------
### ags version analyzer
# --------------------------------------------------------------------------------------
def analyze_ags_content(file_bytes: bytes) -> Dict[str, str]:
    results = {
        "AGS3": "No",
        "AGS4": "No",
        'Contains "LOCA"': "No",
        "Contains **HOLE": "No"
    }
    try:
        content = file_bytes.decode("latin-1", errors="ignore")
        lines = content.splitlines()
        for line in lines:
            s = line.strip()
            if not s: 
                continue
            if line.startswith("<UNIT>") or line.startswith("UNIT") or line.startswith("<UNITS>"): 
                continue
            
            # Simple check for version headers
            if s.startswith('"GROUP"') or s.startswith("GROUP"):
                results["AGS4"] = "Yes"
                if '"GROUP","LOCA"' in s or "GROUP,LOCA" in s:
                    results['Contains "LOCA"'] = "Yes"
                break
            if s.startswith('"**') or s.startswith("**"):
                results["AGS3"] = "Yes"
                if "**HOLE" in s:
                    results["Contains **HOLE"] = "Yes"
                break
    except Exception:
        pass
    return results


# --------------------------------------------------------------------------------------
# based on the version parses the ags file
# --------------------------------------------------------------------------------------
def parse_ags_file(file_bytes: bytes) -> Dict[str, pd.DataFrame]:
    analysis = analyze_ags_content(file_bytes)
    is_ags3 = analysis.get("AGS3") == "Yes"
    is_ags4 = analysis.get("AGS4") == "Yes"

    text = file_bytes.decode("latin-1", errors="ignore")
    
    # Filter lines
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith(("<UNIT>", "UNIT", "<UNITS>"))
    ]

    group_data: Dict[str, List[Dict[str, str]]] = {}
    group_headings: Dict[str, List[str]] = {}
    current_group = None
    headings: List[str] = []
    
    parse_errors = []

    def ensure_group(name: str):
        group_data.setdefault(name, [])

    def append_continuation(parts: List[str]):
        """
        Appends data from a <CONT> line to the last row of the current group.
        """
        if not (current_group and headings and group_data[current_group]):
            return

        # ---------------------------------------------------------
        # AGS4 Logic: <CONT> replaces "DATA".
        # parts[0] is <CONT>. parts[1] is the first Data Value.
        # Headings[0] maps to parts[1].
        # Shift Index: i -> i-1
        # ---------------------------------------------------------
        if is_ags4:
             for i in range(1, len(parts)):
                 heading_index = i - 1
                 val = parts[i].strip()
                 
                 if heading_index < len(headings) and val:
                     field = headings[heading_index]
                     prev = group_data[current_group][-1].get(field, "")
                     existing_values = [p.strip() for p in prev.split(" | ") if p]
                     if val not in existing_values:
                         group_data[current_group][-1][field] = f"{prev} | {val}" if prev else val

        # ---------------------------------------------------------
        # AGS3 Logic: <CONT> replaces the Key Field (Variable 0).
        # parts[0] is <CONT> (Variable 0). parts[1] is Variable 1.
        # Headings[1] maps to parts[1].
        # Direct Mapping: i -> i
        # ---------------------------------------------------------
        else: 
            # Start from 1 because index 0 is <CONT> (which replaces Key)
            for i in range(1, len(parts)):
                heading_index = i
                val = parts[i].strip()

                if heading_index < len(headings) and val:
                    field = headings[heading_index]
                    prev = group_data[current_group][-1].get(field, "")
                    existing_values = [p.strip() for p in prev.split(" | ") if p]
                    if val not in existing_values:
                        group_data[current_group][-1][field] = f"{prev} | {val}" if prev else val

    for i, line in enumerate(lines):
        parts = _split_quoted_csv(line)
        
        if not parts:
            if len(line) > 5:
                parse_errors.append(f"Line {i+1}: Failed to parse")
            continue
            
        # Clean keyword (handle quotes/spaces that might remain or encoded XML)
        raw_keyword = parts[0].upper().strip()
        
        # 1. Handle Continuation Lines
        # Check for standard <CONT> and HTML encoded version just in case
        if raw_keyword in ["<CONT>", "&LT;CONT&GT;"]:
            append_continuation(parts)
            continue
            
        # 2. Skip remaining Units/Metadata
        if raw_keyword in ["<UNITS>", "UNIT", "<UNIT>"]:
            continue

        # 3. AGS4 Logic
        if is_ags4:
            if raw_keyword == "GROUP":
                if len(parts) > 1:
                    current_group = parts[1]
                    ensure_group(current_group)
                    headings = []
            elif raw_keyword == "HEADING":
                headings = parts[1:]
                if current_group:
                    group_headings[current_group] = headings
            elif raw_keyword == "DATA":
                if current_group and headings:
                    # parts[1] -> headings[0]
                    row_dict = dict(zip(headings, parts[1:]))
                    group_data[current_group].append(row_dict)

        # 4. AGS3 Logic
        if is_ags3:
            if raw_keyword.startswith("**"):
                current_group = raw_keyword[2:]
                ensure_group(current_group)
                headings = []
            elif raw_keyword.startswith("*"):
                headings = [p.lstrip("*") for p in parts]
                if current_group:
                    group_headings[current_group] = headings
            elif current_group and headings:
                # parts[0] -> headings[0]
                row_dict = dict(zip(headings, parts[:len(headings)]))
                group_data[current_group].append(row_dict)

    if parse_errors:
        st.warning(f"⚠️ Warning: {len(parse_errors)} lines failed to parse.")
        with st.expander("Show parse errors"):
            for err in parse_errors:
                st.write(err)

    group_dfs = {}
    for group, rows in group_data.items():
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.rename(columns=lambda c: "SPEC_DEPTH" if c.upper() in {"SPEC_DPTH", "SPEC_DEPTH"}
                           else "HOLE_ID" if c.upper() in {"LOCA_ID", "HOLE_ID"}
                           else c)
        group_dfs[group] = df

    return group_dfs
