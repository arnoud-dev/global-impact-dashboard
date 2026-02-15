DROP TABLE IF EXISTS "FactSectorEmissions" CASCADE;
DROP TABLE IF EXISTS "FactCountryYearMetrics" CASCADE;
DROP TABLE IF EXISTS "DimCountry" CASCADE;
DROP TABLE IF EXISTS "DimRegion" CASCADE;
DROP TABLE IF EXISTS "DimYear" CASCADE;
DROP TABLE IF EXISTS "DimSector" CASCADE;
DROP TABLE IF EXISTS "DimScenario" CASCADE;


CREATE TABLE "DimRegion" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  region_id TEXT UNIQUE,
  region_name TEXT NOT NULL
);


CREATE TABLE "DimCountry" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  country_name TEXT NOT NULL,
  iso2 TEXT,
  iso3 TEXT NOT NULL UNIQUE,
  region_key INTEGER REFERENCES "DimRegion"(id),
  capital TEXT,
  longitude DOUBLE PRECISION,
  latitude DOUBLE PRECISION
);


CREATE TABLE "DimYear" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  year INTEGER NOT NULL UNIQUE,
  decade INTEGER NOT NULL
);


CREATE TABLE "DimSector" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sector_name TEXT NOT NULL UNIQUE
);


CREATE TABLE "DimScenario" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  scenario_name TEXT NOT NULL,
  scenario_type TEXT,
  UNIQUE (scenario_name, scenario_type)
);


CREATE TABLE "FactCountryYearMetrics" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  country_key INTEGER NOT NULL,
  year_key INTEGER NOT NULL,
  scenario_key INTEGER NOT NULL,

  population BIGINT,
  total_co2_emissions DOUBLE PRECISION,
  methane_emissions DOUBLE PRECISION,
  total_ghg_emissions DOUBLE PRECISION,
  energy_consumption_total DOUBLE PRECISION,
  electricity_consumption DOUBLE PRECISION,
  renewable_energy_share DOUBLE PRECISION,
  gdp DOUBLE PRECISION,

  data_source TEXT,
  last_updated TIMESTAMP,

  UNIQUE (country_key, year_key, scenario_key),

  FOREIGN KEY (country_key) REFERENCES "DimCountry"(id),
  FOREIGN KEY (year_key) REFERENCES "DimYear"(id),
  FOREIGN KEY (scenario_key) REFERENCES "DimScenario"(id)
);


CREATE TABLE "FactSectorEmissions" (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  year_key INTEGER NOT NULL,
  country_key INTEGER NOT NULL,
  sector_key INTEGER NOT NULL,
  scenario_key INTEGER NOT NULL,

  emissions DOUBLE PRECISION,

  UNIQUE (year_key, country_key, sector_key, scenario_key),

  FOREIGN KEY (country_key) REFERENCES "DimCountry"(id),
  FOREIGN KEY (year_key) REFERENCES "DimYear"(id),
  FOREIGN KEY (sector_key) REFERENCES "DimSector"(id),
  FOREIGN KEY (scenario_key) REFERENCES "DimScenario"(id)
);


CREATE INDEX idx_fact_country_year
ON "FactCountryYearMetrics" (country_key, year_key);

CREATE INDEX idx_fact_sector_lookup
ON "FactSectorEmissions" (country_key, year_key, sector_key);
