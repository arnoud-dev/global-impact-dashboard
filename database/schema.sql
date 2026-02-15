CREATE TABLE "FactCountryYearMetrics" (
  "id" INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "country_key" integer NOT NULL,
  "year_key" integer NOT NULL,
  "scenario_key" integer NOT NULL,
  "population" bigint,
  "total_co2_emissions" float,
  "methane_emissions" float,
  "total_ghg_emissions" float,
  "energy_consumption_total" float,
  "electricity_consumption" float,
  "renewable_energy_share" float,
  "gdp" float,
  "data_source" varchar,
  "dataset_version" varchar,
  "last_updated" timestamp
);

CREATE TABLE "FactSectorEmissions" (
  "id" INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "year_key" integer NOT NULL,
  "country_key" integer NOT NULL,
  "sector_key" integer NOT NULL,
  "scenario_key" integer NOT NULL,
  "emissions" float
);

CREATE TABLE "DimCountry" (
  "id" integer PRIMARY KEY,
  "country_name" varchar,
  "iso2" varchar,
  "iso3" varchar,
  "region" varchar
);

CREATE TABLE "DimYear" (
  "id" integer PRIMARY KEY,
  "year" integer,
  "decade" integer
);

CREATE TABLE "DimSector" (
  "id" integer PRIMARY KEY,
  "sector_name" varchar
);

CREATE TABLE "DimScenario" (
  "id" integer PRIMARY KEY,
  "scenario_name" varchar,
  "scenario_type" varchar
);

ALTER TABLE "FactCountryYearMetrics" ADD FOREIGN KEY ("country_key") REFERENCES "DimCountry" ("id");

ALTER TABLE "FactCountryYearMetrics" ADD FOREIGN KEY ("year_key") REFERENCES "DimYear" ("id");

ALTER TABLE "FactCountryYearMetrics" ADD FOREIGN KEY ("scenario_key") REFERENCES "DimScenario" ("id");

ALTER TABLE "FactSectorEmissions" ADD FOREIGN KEY ("country_key") REFERENCES "DimCountry" ("id");

ALTER TABLE "FactSectorEmissions" ADD FOREIGN KEY ("year_key") REFERENCES "DimYear" ("id");

ALTER TABLE "FactSectorEmissions" ADD FOREIGN KEY ("sector_key") REFERENCES "DimSector" ("id");

ALTER TABLE "FactSectorEmissions" ADD FOREIGN KEY ("scenario_key") REFERENCES "DimScenario" ("id");
