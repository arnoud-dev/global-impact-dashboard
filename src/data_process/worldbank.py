from pathlib import Path
import pandas as pd
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data/raw/worldbank"
OUT_DIR = PROJECT_ROOT / "data/processed"
DIM_COUNTRY = PROJECT_ROOT / "data/dimensions/dim_country.csv"

INDICATORS = {
    "co2_emissions.csv": "total_co2_emissions",
    "population_total.csv": "population",
    "methane_emissions.csv": "methane_emissions",
    "total_ghg_emissions.csv": "total_ghg_emissions",
    "energy_use_per_capita.csv": "energy_consumption_total",
    "electricity_consumption_per_capita.csv": "electricity_consumption",
    "renewable_energy_share.csv": "renewable_energy_share",
    "gdp_current_usd.csv": "gdp",
}

def read_last_updated(path: Path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0] == "Last Updated Date":
                return row[1]
    return None

def process_file(file_path: Path, value_name: str, valid_codes: set):
    df = pd.read_csv(file_path, skiprows=4)
    df.columns = df.columns.str.strip()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    year_cols = [c for c in df.columns if c.isdigit()]
    df[year_cols] = df[year_cols].apply(pd.to_numeric, errors="coerce")

    df = df[["Country Name", "Country Code"] + year_cols]
    df = df[df["Country Code"].isin(valid_codes)]

    df_long = df.melt(
        id_vars=["Country Name", "Country Code"],
        var_name="year",
        value_name=value_name
    )
    df_long["year"] = df_long["year"].astype(int)
    df_long = df_long.dropna(subset=[value_name])
    df_long = df_long.sort_values(["Country Code", "year"])
    return df_long

def run():
    df_country = pd.read_csv(DIM_COUNTRY)
    valid_codes = set(df_country["iso3"].dropna())

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    processed_dfs = []

    for filename, value_name in INDICATORS.items():
        path = RAW_DIR / filename
        if not path.exists():
            print(f"Missing file: {path}")
            continue

        last_updated = read_last_updated(path)
        print(f"{filename} last updated: {last_updated}")

        df_long = process_file(path, value_name, valid_codes)
        out_file = OUT_DIR / f"processed_{value_name}.csv"
        df_long.to_csv(out_file, index=False)
        print(f"Saved: {out_file} ({len(df_long)} rows)")

        df_long["last_updated"] = last_updated
        processed_dfs.append(df_long)

    if processed_dfs:
        merged_df = processed_dfs[0]
        for df_next in processed_dfs[1:]:
            merged_df = merged_df.merge(
                df_next,
                on=["Country Name", "Country Code", "year", "last_updated"],
                how="outer"
            )

        merged_out = OUT_DIR / "merged_worldbank_metrics.csv"
        merged_df.to_csv(merged_out, index=False)
        print(f"Merged dataset saved: {merged_out} ({len(merged_df)} rows)")

if __name__ == "__main__":
    run()
