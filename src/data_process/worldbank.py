from pathlib import Path
import pandas as pd
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data/raw/worldbank"
OUT_DIR = PROJECT_ROOT / "data/processed"
DIM_COUNTRY = PROJECT_ROOT / "data/dimensions/dim_country.csv"

INDICATORS = {
    "co2_emissions.csv": "co2_kt",
    "population_total.csv": "population",
    "methane_emissions.csv": "methane_mt",
    "total_ghg_emissions.csv": "ghg_mt",
}

def read_last_updated(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0] == "Last Updated Date":
                return row[1]
    return None

def process_file(file_path, value_name, valid_codes):
    df = pd.read_csv(file_path, skiprows=4)
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    year_cols = [c for c in df.columns if c.isdigit()]
    df[year_cols] = df[year_cols].apply(pd.to_numeric, errors="coerce")

    df = df[["Country Name", "Country Code"] + year_cols]
    df = df[df["Country Code"].isin(valid_codes)]

    df = df.melt(
        id_vars=["Country Name", "Country Code"],
        var_name="year",
        value_name=value_name,
    )

    df["year"] = df["year"].astype(int)
    df = df.dropna(subset=[value_name])
    df = df.sort_values(["Country Code", "year"])

    return df

def run():
    df_country = pd.read_csv(DIM_COUNTRY)
    valid_codes = set(df_country["iso3"].dropna())

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for filename, value_name in INDICATORS.items():
        path = RAW_DIR / filename
        if not path.exists():
            continue

        updated = read_last_updated(path)
        print(path.name, "updated:", updated)

        df = process_file(path, value_name, valid_codes)
        out_file = OUT_DIR / f"processed_{value_name}.csv"
        df.to_csv(out_file, index=False)
        print("Saved:", out_file)
