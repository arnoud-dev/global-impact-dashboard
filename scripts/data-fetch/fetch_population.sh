#!/bin/bash

save_dir="data/raw/population"
mkdir -p "$save_dir"

echo "Fetching all countries..."

curl -fsS \
  "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=20000" \
  > "$save_dir/all_countries.json"

echo "Saved raw data → $save_dir/all_countries.json"
