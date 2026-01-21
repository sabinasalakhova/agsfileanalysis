from typing import Dict, List,Tuple
import re
import pandas as pd

# --------------------------------------------------------------------------------------
### split quotes in the AGS
# --------------------------------------------------------------------------------------

def _split_quoted_csv(line: str) -> List[str]:
    
    s = line.strip() #remove whitespace
  
    if s.startswith('"') and s.endswith('"') and '","' in s: 
      
        parts = [p.replace('""', '"') for p in s.split('","')]  # split by '","' and replaces any escaped double quotes ("") with a single quote (")
      
        #remove outermost quotes
        parts[0] = parts[0].lstrip('"') 
        parts[-1] = parts[-1].rstrip('"')
      
        return parts
 
    return [p.strip().strip('"') for p in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', s)] #regex in case some errors, splits only on commas outside quotes


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
            parts = _split_quoted_csv(line)
            if line.startswith("<UNIT>") or line.startswith("UNIT") or line.startswith("<UNITS>"):
                continue
            token = "<CONT>" if line.startswith('"<CONT>"') else None
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
    #exclude <UNITS>
    lines = [
    line.strip()
    for line in text.splitlines()
    if line.strip() and not line.strip().startswith(("<UNIT>", "UNIT", "<UNITS>", "<CONT>"))
]

    group_data: Dict[str, List[Dict[str, str]]] = {}
    group_headings: Dict[str, List[str]] = {}
    current_group = None
    headings: List[str] = []

    def ensure_group(name: str):
        group_data.setdefault(name, [])

    def append_continuation(parts: List[str]):
        if current_group and headings and group_data[current_group]:
            for i, val in enumerate(parts[1:]):
                if i < len(headings) and val:
                    field = headings[i]
                    prev = group_data[current_group][-1].get(field, "")
                    if val not in [p.strip() for p in prev.split(" | ") if p]:
                        group_data[current_group][-1][field] = f"{prev} | {val}" if prev else val

    for line in lines:
        parts = _split_quoted_csv(line)
        token = "<CONT>" if line.startswith('"&lt;CONT&gt;"') or line.startswith("&lt;CONT&gt;") else None

        if is_ags4:
            keyword = parts[0].upper()
            if keyword == "GROUP":
                current_group = parts[1]
                ensure_group(current_group)
                headings = []
            elif keyword == "HEADING":
                headings = parts[1:]
                group_headings[current_group] = headings
            elif keyword == "DATA":
                group_data[current_group].append(dict(zip(headings, parts[1:])))

        if is_ags3:
            keyword = parts[0]
            if keyword.startswith("**"):  # Group identifier
                current_group = keyword[2:]
                ensure_group(current_group)
                headings = []
            elif keyword.startswith("*"):  # Heading or metadata
                headings = [p.lstrip("*") for p in parts]
                group_headings[current_group] = headings
            elif current_group and headings:  # Data follows headings
                group_data[current_group].append(dict(zip(headings, parts[:len(headings)])))
    # Convert to DataFrames and normalize column names
    group_dfs = {}
    for group, rows in group_data.items():
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.rename(columns=lambda c: "SPEC_DEPTH" if c.upper() in {"SPEC_DPTH", "SPEC_DEPTH"}
                           else "HOLE_ID" if c.upper() in {"LOCA_ID", "HOLE_ID"}
                           else c)
        group_dfs[group] = df

    return group_dfs
