
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# helper:generates triaxial data table from ags contents
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_triaxial_table(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Assumes input groups are already cleaned and standardized.
    Builds a merged triaxial summary table from AGS groups.
    """

    # Get groups
    samp = groups.get("SAMP", pd.DataFrame()).copy()
    clss = groups.get("CLSS", pd.DataFrame()).copy()
    trig = groups.get("TRIG", pd.DataFrame()).copy()
    treg = groups.get("TREG", pd.DataFrame()).copy()
    trix = groups.get("TRIX", pd.DataFrame()).copy()
    tret = groups.get("TRET", pd.DataFrame()).copy()

    # Merge keys
    merge_keys = ["HOLE_ID"]
    if not samp.empty and "SPEC_DEPTH" in samp.columns:
        merge_keys.append("SPEC_DEPTH")

    merged = samp.copy() if not samp.empty else pd.DataFrame(columns=merge_keys).copy()

    # Add CLSS
    if not clss.empty:
        merged = pd.merge(merged, clss, on=merge_keys, how="outer", suffixes=("", "_CLSS"))

    # Add TRIG/TREG type info
    if not trig.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TRIG_TYPE"] if c in trig.columns]
        trig_f = trig[keep].copy()
        merged = pd.merge(merged, trig_f, on=[c for c in keep if c in merge_keys], how="outer")
    if not treg.empty:
        keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "TREG_TYPE"] if c in treg.columns]
        treg_f = treg[keep].copy()
        merged = pd.merge(merged, treg_f, on=[c for c in keep if c in merge_keys], how="outer")

    # Combine TRIX and TRET
    tri_res = pd.concat([trix, tret], ignore_index=True) if not trix.empty or not tret.empty else pd.DataFrame()
    if not tri_res.empty:
        tri_keep = [c for c in ["HOLE_ID", "SPEC_DEPTH", "CELL", "DEVF", "PWPF", "SOURCE_FILE"] if c in tri_res.columns]
        tri_res = tri_res[tri_keep].copy()
        merged = pd.merge(merged, tri_res, on=[c for c in ["HOLE_ID", "SPEC_DEPTH"] if c in merged.columns], how="outer")

    # Final column subset
    cols_pref = [
        "HOLE_ID", "SAMP_ID", "SAMP_REF", "SAMP_TOP",
        "SPEC_REF", "SPEC_DEPTH", "DEPTH_FROM", "DEPTH_TO",
        "SAMP_DESC", "SPEC_DESC", "GEOL_STAT",
        "TRIG_TYPE", "TREG_TYPE",
        "CELL", "DEVF", "PWPF", "SOURCE_FILE"
    ]
    final_cols = [c for c in cols_pref if c in merged.columns]
    final_df = merged[final_cols].copy() if final_cols else merged.copy()

    return final_df

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# helper: associates values in triaxial table with LITH from giu_file
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_triaxial_with_lithology(groups: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Builds triaxial summary table and maps lithology from GIU group based on depth ranges.
    Optimized for large GIU files by grouping intervals by HOLE_ID.
    Assumes all input groups are cleaned.
    """
    triaxial_df = generate_triaxial_table(groups)
    giu = groups.get("GIU", pd.DataFrame()).copy()

    if giu.empty or "HOLE_ID" not in giu.columns:
        triaxial_df["LITHOLOGY"] = None
        return triaxial_df

    # Normalize GIU depth columns
    coalesce_columns(giu, ["DEPTH_FROM", "START_DEPTH"], "DEPTH_FROM")
    coalesce_columns(giu, ["DEPTH_TO", "END_DEPTH"], "DEPTH_TO")
    coalesce_columns(giu, ["GEOL_DESC", "GEOL_GEOL", "GEOL_GEO2"], "LITHOLOGY")
    to_numeric_safe(giu, ["DEPTH_FROM", "DEPTH_TO"])

    # Group GIU intervals by HOLE_ID
    giu_by_hole = {
        hole: df.dropna(subset=["DEPTH_FROM", "DEPTH_TO"])
        for hole, df in giu.groupby("HOLE_ID")
    }

    # Efficient row-wise mapping
    def map_litho(row):
        hole = row.get("HOLE_ID")
        depth = row.get("SPEC_DEPTH")
        if pd.isna(hole) or pd.isna(depth):
            return None
        giu_rows = giu_by_hole.get(hole)
        if giu_rows is None:
            return None
        match = giu_rows[
            (giu_rows["DEPTH_FROM"] <= depth) &
            (giu_rows["DEPTH_TO"] >= depth)
        ]
        return match["LITHOLOGY"].iloc[0] if not match.empty else None

    triaxial_df["LITHOLOGY"] = triaxial_df.apply(map_litho, axis=1)
    return triaxial_df
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# helper: computes s,t values
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def calculate_s_t_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates s and t values for triaxial test data.
    Works with both AGS3 (TRIX) and AGS4 (TRET) formats.
    Assumes df contains either TRIX or TRET fields.
    """

    # Coalesce AGS3 and AGS4 fields
    coalesce_columns(df, ["TRIX_CELL", "TRET_CELL"], "CELL")     # σ3′
    coalesce_columns(df, ["TRIX_DEVF", "TRET_DEVF"], "DEVF")     # deviator stress
    coalesce_columns(df, ["TRIX_PWPF", "TRET_PWPF"], "PWPF")     # pore pressure

    # Convert to numeric
    to_numeric_safe(df, ["CELL", "DEVF"])

    # Calculate σ1′ = σ3′ + DEVF
    df["SIGMA_1"] = df["CELL"] + df["DEVF"]

    # Calculate s and t
    df["s"] = (df["SIGMA_1"] + df["CELL"]) / 2
    df["t"] = df["DEVF"] / 2

    return df

    return df
