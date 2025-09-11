from scipy.stats import linregress
import numpy as np

def estimate_strength_params(df: pd.DataFrame) -> Tuple[float, float]:
    df = df.dropna(subset=["s", "t"])
    if df.empty:
        return (np.nan, np.nan)

    slope, intercept, _, _, _ = linregress(df["s"], df["t"])
    phi = np.degrees(np.arcsin(slope))
    cohesion = intercept / np.cos(np.radians(phi))
    return round(phi, 2), round(cohesion, 2)

