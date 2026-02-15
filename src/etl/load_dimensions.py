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

DIM_FILES = {
    "DimRegion": PROJECT_ROOT / "data/dimensions/dim_region.csv",
    "DimYear": PROJECT_ROOT / "data/dimensions/dim_year.csv",
    "DimSector": PROJECT_ROOT / "data/dimensions/dim_sector.csv",
    "DimScenario": PROJECT_ROOT / "data/dimensions/dim_scenario.csv",
}


def load_dimension(conn, table, file_path):
    print(f"Loading {table}...")

    df = pd.read_csv(file_path)
    values = [tuple(r) for r in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;')

        query = f'''
            INSERT INTO "{table}" ({",".join(df.columns)})
            VALUES %s;
        '''

        execute_values(cur, query, values)

    conn.commit()
    print(f"{table} loaded ({len(values)} rows)")


def load_countries(conn):
    print("Loading DimCountry...")

    df_country = pd.read_csv(
        PROJECT_ROOT / "data/dimensions/dim_country.csv"
    )

    df_region = pd.read_sql(
        'SELECT id, region_id FROM "DimRegion"',
        conn
    )

    region_map = dict(zip(df_region["region_id"], df_region["id"]))

    df_country["region_key"] = df_country["region_id"].map(region_map)

    values = [
        tuple(r)
        for r in df_country[
            ["iso3","iso2","country_name",
             "region_key","capital","longitude","latitude"]
        ].itertuples(index=False, name=None)
    ]

    with conn.cursor() as cur:
        cur.execute(
            'TRUNCATE TABLE "DimCountry" RESTART IDENTITY CASCADE;'
        )

        query = '''
            INSERT INTO "DimCountry"
            (iso3, iso2, country_name, region_key,
             capital, longitude, latitude)
            VALUES %s;
        '''

        execute_values(cur, query, values)

    conn.commit()
    print(f'DimCountry loaded ({len(values)} rows)')


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)

    for table, file_path in DIM_FILES.items():
        load_dimension(conn, table, file_path)

    load_countries(conn)

    conn.close()
    print("All dimensions loaded successfully.")


if __name__ == "__main__":
    main()
