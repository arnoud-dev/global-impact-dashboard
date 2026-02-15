import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

SCENARIO_NAME = "Historical"
INPUT_FILE = PROJECT_ROOT / "data/processed/merged_worldbank_metrics.csv"

def load_dimension_maps(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT id, iso3 FROM "DimCountry"')
        country_map = {iso: cid for cid, iso in cur.fetchall()}

        cur.execute('SELECT id, year FROM "DimYear"')
        year_map = {year: yid for yid, year in cur.fetchall()}

        cur.execute('SELECT id, scenario_name FROM "DimScenario"')
        scenario_map = {name: sid for sid, name in cur.fetchall()}

    return country_map, year_map, scenario_map

def main():
    if not INPUT_FILE.exists():
        print(f"Missing merged file: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)

    conn = psycopg2.connect(**DB_CONFIG)
    country_map, year_map, scenario_map = load_dimension_maps(conn)
    scenario_key = scenario_map[SCENARIO_NAME]

    rows = []

    for _, r in df.iterrows():
        iso3 = r["Country Code"]
        year = r["year"]

        if iso3 not in country_map or year not in year_map:
            continue

        rows.append((
            country_map[iso3],
            year_map[year],
            scenario_key,
            int(r.get("population")) if pd.notna(r.get("population")) else None,
            float(r.get("total_co2_emissions")) if pd.notna(r.get("total_co2_emissions")) else None,
            float(r.get("methane_emissions")) if pd.notna(r.get("methane_emissions")) else None,
            float(r.get("total_ghg_emissions")) if pd.notna(r.get("total_ghg_emissions")) else None,
            float(r.get("energy_consumption_total")) if pd.notna(r.get("energy_consumption_total")) else None,
            float(r.get("electricity_consumption")) if pd.notna(r.get("electricity_consumption")) else None,
            float(r.get("renewable_energy_share")) if pd.notna(r.get("renewable_energy_share")) else None,
            float(r.get("gdp")) if pd.notna(r.get("gdp")) else None,
            "World Bank",
            r.get("last_updated")
        ))

    if not rows:
        print("No rows to insert.")
        return

    query = """
        INSERT INTO "FactCountryYearMetrics"
        (
            country_key,
            year_key,
            scenario_key,
            population,
            total_co2_emissions,
            methane_emissions,
            total_ghg_emissions,
            energy_consumption_total,
            electricity_consumption,
            renewable_energy_share,
            gdp,
            data_source,
            last_updated
        )
        VALUES %s
        ON CONFLICT (country_key, year_key, scenario_key)
        DO UPDATE SET
            population = EXCLUDED.population,
            total_co2_emissions = EXCLUDED.total_co2_emissions,
            methane_emissions = EXCLUDED.methane_emissions,
            total_ghg_emissions = EXCLUDED.total_ghg_emissions,
            energy_consumption_total = EXCLUDED.energy_consumption_total,
            electricity_consumption = EXCLUDED.electricity_consumption,
            renewable_energy_share = EXCLUDED.renewable_energy_share,
            gdp = EXCLUDED.gdp,
            last_updated = EXCLUDED.last_updated;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, rows)

    conn.commit()
    conn.close()
    print(f"FactCountryYearMetrics loaded successfully ({len(rows)} rows).")

if __name__ == "__main__":
    main()
