from pathlib import Path
import pandas as pd
import csv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA = PROJECT_ROOT / "data/raw/co2/co2.csv"
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

year_cols = [c for c in df.columns if c.isdigit()]

df[year_cols] = df[year_cols].apply(pd.to_numeric, errors="coerce")

df = df[["Country Name", "Country Code"] + year_cols]

long_df = df.melt(
    id_vars=["Country Name", "Country Code"],
    var_name="Year",
    value_name="CO2_kt"
)

long_df["Year"] = long_df["Year"].astype(int)

long_df = long_df.dropna(subset=["CO2_kt"])

long_df = long_df.sort_values(["Country Code", "Year"])

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
long_df.to_csv(OUTPUT_FILE, index=False)

print("Saved:", OUTPUT_FILE)
