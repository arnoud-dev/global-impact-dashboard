from pathlib import Path
import pandas as pd
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA = PROJECT_ROOT / "data/raw/co2/co2.csv"
CONFIG_FILE = PROJECT_ROOT / "config.txt"
OUTPUT_DIR = PROJECT_ROOT / "data/processed"
OUTPUT_FILE = OUTPUT_DIR / "processed_co2.csv"

last_updated = None
with open(RAW_DATA, newline="") as f:
    reader = csv.reader(f)
    for row in reader:
        if row and row[0] == "Last Updated Date":
            last_updated = row[1]
            break

print("Last updated:", last_updated)

df = pd.read_csv(RAW_DATA, skiprows=4)
df.columns = df.columns.str.strip()
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

countries_line = None
with open(CONFIG_FILE) as f:
    for line in f:
        if line.startswith("countries="):
            countries_line = line.strip().split("=", 1)[1]
            break

countries = countries_line.split(";") if countries_line else []

filtered = df[df["Country Code"].isin(countries)].copy()

year_cols = [c for c in filtered.columns if c.isdigit()]
filtered[year_cols] = filtered[year_cols].apply(pd.to_numeric, errors="coerce")

valid_years = filtered[year_cols].notna().all()
year_cols_clean = valid_years[valid_years].index.tolist()

filtered = filtered[["Country Name", "Country Code"] + year_cols_clean]

world_totals = filtered[year_cols_clean].sum()

world_row = {
    "Country Name": "Selected Countries Total",
    "Country Code": "WORLD",
}

for year in year_cols_clean:
    world_row[year] = world_totals[year]

world_df = pd.DataFrame([world_row])
filtered = pd.concat([world_df, filtered], ignore_index=True)

long_df = filtered.melt(
    id_vars=["Country Name", "Country Code"],
    var_name="Year",
    value_name="CO2"
)

long_df["Year"] = long_df["Year"].astype(int)
long_df = long_df.sort_values(["Country Code", "Year"])


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
long_df.to_csv(OUTPUT_FILE, index=False)

print("Saved:", OUTPUT_FILE)
