import json
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_METADATA = PROJECT_ROOT / "data/raw/metadata/worldbank_countries.json"
DIM_COUNTRY = PROJECT_ROOT / "data/dimensions/dim_country.csv"
DIM_REGION = PROJECT_ROOT / "data/dimensions/dim_region.csv"

def run():
    with open(RAW_METADATA, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    countries = []

    for page in payload:
        if isinstance(page, list) and len(page) > 1:
            countries.extend(page[1])

    country_records = []
    region_records = {}

    for entry in countries:
        region = entry.get("region", {})
        region_id = region.get("id")
        region_name = region.get("value")

        if region_name == "Aggregates":
            continue

        capital = entry.get("capitalCity")
        if not capital:
            continue

        if region_id and region_id not in region_records:
            region_records[region_id] = region_name

        country_records.append(
            {
                "iso3": entry.get("id"),
                "iso2": entry.get("iso2Code"),
                "country_name": entry.get("name"),
                "region_id": region_id,
                "capital": capital,
                "longitude": entry.get("longitude"),
                "latitude": entry.get("latitude"),
            }
        )

    df_country = (
        pd.DataFrame(country_records)
        .sort_values("country_name")
        .reset_index(drop=True)
    )

    df_region = (
        pd.DataFrame(
            [{"region_id": k, "region_name": v}
             for k, v in region_records.items()]
        )
        .sort_values("region_name")
        .reset_index(drop=True)
    )

    DIM_COUNTRY.parent.mkdir(parents=True, exist_ok=True)
    df_country.to_csv(DIM_COUNTRY, index=False)
    df_region.to_csv(DIM_REGION, index=False)

    print("Saved countries:", len(df_country))
    print("Saved regions:", len(df_region))
