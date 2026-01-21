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
    This ensures that empty fields (e.g. "","") are preserved as empty strings,
    maintaining the correct column indexing for continuation lines.
    """
    s = line.strip()
    if not s:
        return []

    try:
        # strict=False allows for some leniency, skipinitialspace handles "Val", "Val"
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
        Handles indexing differences between AGS3 and AGS4.
        """
        if not (current_group and headings and group_data[current_group]):
            return

        # AGS4 Logic: Data lines usually start with "DATA", so indices are shifted.
        # <CONT> replaces "DATA", so parts[1] maps to headings[0].
        if is_ags4:
             data_values = parts[1:] # Shift by 1
             for i, val in enumerate(data_values):
                 val = val.strip()
                 if i < len(headings) and val:
                     field = headings[i]
                     prev = group_data[current_group][-1].get(field, "")
                     existing_values = [p.strip() for p in prev.split(" | ") if p]
                     if val not in existing_values:
                         group_data[current_group][-1][field] = f"{prev} | {val}" if prev else val

        # AGS3 Logic: Data lines have no keyword.
        # <CONT> replaces the Key Field (Index 0).
        # parts[0] is <CONT>. parts[1] is Heading[1]. parts[i] matches Headings[i].
        else: 
            # Start range at 1 to skip <CONT> itself
            for i in range(1, len(parts)):
                if i < len(headings):
                    val = parts[i].strip()
                    if val:
                        field = headings[i]
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
            
        keyword = parts[0].upper().strip()

        # 1. Handle Continuation Lines
        if keyword == "<CONT>":
            append_continuation(parts)
            continue
            
        # 2. Skip remaining Units/Metadata
        if keyword in ["<UNITS>", "UNIT", "<UNIT>"]:
            continue

        # 3. AGS4 Logic
        if is_ags4:
            if keyword == "GROUP":
                if len(parts) > 1:
                    current_group = parts[1]
                    ensure_group(current_group)
                    headings = []
            elif keyword == "HEADING":
                headings = parts[1:]
                if current_group:
                    group_headings[current_group] = headings
            elif keyword == "DATA":
                if current_group and headings:
                    row_dict = dict(zip(headings, parts[1:]))
                    group_data[current_group].append(row_dict)

        # 4. AGS3 Logic
        if is_ags3:
            if keyword.startswith("**"):
                current_group = keyword[2:]
                ensure_group(current_group)
                headings = []
            elif keyword.startswith("*"):
                headings = [p.lstrip("*") for p in parts]
                if current_group:
                    group_headings[current_group] = headings
            elif current_group and headings:
                # Direct mapping for AGS3
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
