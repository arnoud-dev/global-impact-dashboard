import json
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_METADATA = PROJECT_ROOT / "data/raw/metadata/worldbank_countries.json"
PROCESSED_METADATA = PROJECT_ROOT / "data/dimensions/dim_country.csv"


def process_country_metadata(file_path):
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    countries = payload[0][1]

    records = []
    for entry in countries:
        records.append(
            {
                "id": entry.get("id"),
                "iso2Code": entry.get("iso2Code"),
                "name": entry.get("name"),
                "region.value": entry.get("region", {}).get("value"),
            }
        )

    df = pd.DataFrame(records)
    return df.sort_values("name").reset_index(drop=True)


if __name__ == "__main__":
    processed = process_country_metadata(RAW_METADATA)
    PROCESSED_METADATA.parent.mkdir(parents=True, exist_ok=True)
    processed.to_csv(PROCESSED_METADATA, index=False)
    print(f"Saved {len(processed)} records to {PROCESSED_METADATA}")
