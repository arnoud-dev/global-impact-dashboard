import pandas as pd
from pathlib import Path

RAW_FILE = Path("data/raw/emissions/ghg_emissions_by_sector.csv")
OUTPUT_FILE = Path("data/processed/sector_emissions.csv")


def main():
    print("Loading raw emissions dataset...")
    df = pd.read_csv(RAW_FILE)

    print(f"Rows loaded: {len(df)}")

    df = df[~df["Code"].str.startswith("OWID", na=False)]

    print(f"After removing aggregates: {len(df)}")

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    sector_cols = df.columns.difference(["Entity", "Code", "Year"])

    for col in sector_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df[sector_cols] = df[sector_cols].fillna(0)
    df = df.sort_values(["Code", "Year"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    print("Saving processed dataset...")
    df.to_csv(OUTPUT_FILE, index=False)

    print("Processing complete.")
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Final rows: {len(df)}")


if __name__ == "__main__":
    main()
