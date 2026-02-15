#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Fetching all datasets..."

"$SCRIPT_DIR/fetch_worldbank_indicators.sh"
"$SCRIPT_DIR/fetch_country_metadata.sh"
"$SCRIPT_DIR/fetch_sector_emissions.sh"

echo "All datasets updated."
