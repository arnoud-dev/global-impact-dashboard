#!/bin/bash
set -e

echo "Fetching all datasets..."

./fetch_worldbank_indicators.sh
./fetch_country_metadata.sh
./fetch_sector_emissions.sh

echo "All datasets updated."
