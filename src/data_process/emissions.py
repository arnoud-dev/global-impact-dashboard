import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "data/raw/emissions/ghg_emissions_by_sector.csv"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/sector_emissions.csv"
DIM_COUNTRY = PROJECT_ROOT / "data/dimensions/dim_country.csv"

def run():
    df = pd.read_csv(RAW_FILE)
    df_country = pd.read_csv(DIM_COUNTRY)

    valid_codes = set(df_country["iso3"].dropna())
    df = df[df["Code"].isin(valid_codes)]

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    sector_cols = df.columns.difference(["Entity", "Code", "Year"])

    for col in sector_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df[sector_cols] = df[sector_cols].fillna(0)
    df = df.sort_values(["Code", "Year"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print("Saved:", OUTPUT_FILE)
    print("Rows:", len(df))
