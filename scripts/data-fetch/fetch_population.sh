#!/bin/bash

save_dir="data/raw/population"
mkdir -p "$save_dir"

countries=(
  BEL FRA NLD DEU FIN
  USA CHN IND JPN GBR
  ITA ESP CAN BRA AUS
  KOR IDN MEX ZAF TUR
)

for code in "${countries[@]}"; do
  echo "Fetching $code..."

  curl -fsS \
    "https://api.worldbank.org/v2/country/$code/indicator/SP.POP.TOTL?format=json&per_page=1000" \
    > "$save_dir/$code.json"

  echo "Saved raw data → $save_dir/$code.json"
  echo "---------------------------"
done
