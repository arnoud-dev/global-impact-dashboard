#!/bin/bash

save_dir="data/raw/co2"
mkdir -p "$save_dir"

echo "Downloading CO2 dataset..."

curl -fLs \
  "https://api.worldbank.org/v2/en/indicator/EN.GHG.CO2.MT.CE.AR5?downloadformat=csv" \
  -o "$save_dir/co2_worldbank.zip"

echo "Extracting CSV..."
unzip -oq "$save_dir/co2_worldbank.zip" -d "$save_dir"

echo "Cleaning files..."

mv "$save_dir"/API_*.csv "$save_dir/co2.csv"

rm "$save_dir/co2_worldbank.zip"
rm -f "$save_dir"/Metadata_*.csv

echo "Done"
