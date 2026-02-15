#!/bin/bash

save_dir="data/raw/emissions"
mkdir -p "$save_dir"

file="$save_dir/ghg_emissions_by_sector.csv"

echo "Fetching sector emissions dataset..."

curl -fsS \
  "https://ourworldindata.org/grapher/ghg-emissions-by-sector.csv" \
  > "$file"

countries_file="$save_dir/countries.csv"
aggregates_file="$save_dir/aggregates.csv"

echo "Entity,Code" > "$countries_file"
echo "Entity,Code" > "$aggregates_file"

awk -F',' 'NR>1 {print $1","$2}' "$file" | sort -u | \
while IFS=',' read -r entity code; do
  if [[ "$code" == OWID_* ]]; then
    echo "$entity,$code" >> "$aggregates_file"
  else
    echo "$entity,$code" >> "$countries_file"
  fi
done

echo "Saved:"
echo "  Countries  → $countries_file"
echo "  Aggregates → $aggregates_file"
