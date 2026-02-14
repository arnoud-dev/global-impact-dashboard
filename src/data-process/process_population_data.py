import json
from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_POP_DATA_FOLDER = PROJECT_ROOT / "data/raw/population"
PROCESSED_POP = PROJECT_ROOT / "data/processed/processed_population_data.csv"


def process_population_data(folder_path):
    folder = Path(folder_path)
    records = []

    for json_file in sorted(folder.glob("*.json")):
        with json_file.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        if not isinstance(payload, list) or len(payload) < 2:
            continue

        for entry in payload[1] or []:
            if not isinstance(entry, dict):
                continue

            records.append(
                {
                    "CountryID": entry.get("country", {}).get("id"),
                    "Country": entry.get("country", {}).get("value"),
                    "Year": entry.get("date"),
                    "Population": entry.get("value"),
                }
            )


    processed_data = pd.DataFrame(records)
    if processed_data.empty:
        return processed_data

    processed_data["Year"] = pd.to_numeric(processed_data["Year"], errors="coerce")
    processed_data["Population"] = pd.to_numeric(processed_data["Population"], errors="coerce")
    processed_data = processed_data.dropna(subset=["Country", "Year", "Population"])
    processed_data["Year"] = processed_data["Year"].astype(int)
    processed_data = processed_data.sort_values(["CountryID", "Year"], ascending=[True, False])

    return processed_data


if __name__ == "__main__":
    print("Processing population data...")
    processed = process_population_data(RAW_POP_DATA_FOLDER)
    if processed.empty:
        print("No population records found.")
    else:
        PROCESSED_POP.parent.mkdir(parents=True, exist_ok=True)
        processed.to_csv(PROCESSED_POP, index=False)
        print(f"Saved {len(processed)} records to {PROCESSED_POP}")
