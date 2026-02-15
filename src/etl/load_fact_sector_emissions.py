import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

INPUT_FILE = "data/processed/sector_emissions.csv"
SCENARIO_ID = 1


def load_dimension_maps(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT id, iso3 FROM "DimCountry"')
        country_map = {iso: cid for cid, iso in cur.fetchall()}

        cur.execute('SELECT id, year FROM "DimYear"')
        year_map = {year: yid for yid, year in cur.fetchall()}

        cur.execute('SELECT id, sector_name FROM "DimSector"')
        sector_map = {name: sid for sid, name in cur.fetchall()}

    return country_map, year_map, sector_map


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)

    country_map, year_map, sector_map = load_dimension_maps(conn)

    print("Loading processed emissions data...")
    df = pd.read_csv(INPUT_FILE)

    print("Converting sectors to rows...")
    df_long = df.melt(
        id_vars=["Entity", "Code", "Year"],
        var_name="sector",
        value_name="emissions"
    )

    df_long = df_long.dropna(subset=["emissions"])

    rows = []

    print("Mapping dimension keys...")

    for _, r in df_long.iterrows():
        iso3 = r["Code"]
        year = r["Year"]
        sector = r["sector"]
        emissions = r["emissions"]

        if iso3 not in country_map:
            continue
        if year not in year_map:
            continue
        if sector not in sector_map:
            continue

        rows.append((
            year_map[year],
            country_map[iso3],
            sector_map[sector],
            SCENARIO_ID,
            float(emissions)
        ))

    print(f"Rows ready for insert: {len(rows)}")

    query = """
        INSERT INTO "FactSectorEmissions"
        (year_key, country_key, sector_key, scenario_key, emissions)
        VALUES %s;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, rows)

    conn.commit()
    conn.close()

    print("FactSectorEmissions successfully loaded.")


if __name__ == "__main__":
    main()
