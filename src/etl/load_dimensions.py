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

DIM_FILES = {
    "DimYear": "data/dimensions/dim_year.csv",
    "DimCountry": "data/dimensions/dim_country.csv",
    "DimSector": "data/dimensions/dim_sector.csv",
    "DimScenario": "data/dimensions/dim_scenario.csv",
}


def load_dimension(conn, table, file_path):
    print(f"Loading {table}...")

    df = pd.read_csv(file_path)

    cols = list(df.columns)

    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    query = f"""
        INSERT INTO "{table}" ({",".join(cols)})
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)

    conn.commit()
    print(f"{table} loaded ({len(values)} rows)")


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)

    for table, file_path in DIM_FILES.items():
        load_dimension(conn, table, file_path)

    conn.close()
    print("All dimensions loaded successfully.")


if __name__ == "__main__":
    main()
