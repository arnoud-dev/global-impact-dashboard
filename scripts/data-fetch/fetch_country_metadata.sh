#!/bin/bash
set -e

save_dir="data/raw/metadata"
mkdir -p "$save_dir"

output="$save_dir/worldbank_countries.json"
tmp="$save_dir/tmp_pages"

mkdir -p "$tmp"
rm -f "$tmp"/*.json

echo "Fetching World Bank country metadata..."

page=1

while true; do
  file="$tmp/page_$page.json"

  echo "Downloading page $page..."

  curl -fsS \
    "https://api.worldbank.org/v2/country?format=json&per_page=300&page=$page" \
    > "$file"

  if ! grep -q '"id"' "$file"; then
    rm "$file"
    break
  fi

  ((page++))
done

echo "Combining pages..."

jq -s '.' "$tmp"/*.json > "$output"

rm -r "$tmp"

echo "Saved metadata → $output"
