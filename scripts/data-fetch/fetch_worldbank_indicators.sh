#!/bin/bash
set -e

save_dir="data/raw/worldbank"
mkdir -p "$save_dir"

declare -A indicators=(
    ["EG.USE.PCAP.KG.OE"]="energy_use_per_capita"
    ["EG.USE.ELEC.KH.PC"]="electricity_consumption_per_capita"
    ["EG.FEC.RNEW.ZS"]="renewable_energy_share"
    ["NY.GDP.MKTP.CD"]="gdp_current_usd"
    ["SP.POP.TOTL"]="population_total"

    # Emissions
    ["EN.GHG.CO2.MT.CE.AR5"]="co2_emissions"
    ["EN.GHG.CH4.MT.CE.AR5"]="methane_emissions"
    ["EN.GHG.ALL.MT.CE.AR5"]="total_ghg_emissions"
)

for indicator in "${!indicators[@]}"; do
    name="${indicators[$indicator]}"
    zipfile="$save_dir/$indicator.zip"
    tmpdir="$save_dir/tmp_extract"

    echo "Downloading $indicator → $name.csv"

    rm -rf "$tmpdir"
    mkdir -p "$tmpdir"

    curl -fsS \
      "https://api.worldbank.org/v2/country/all/indicator/${indicator}?downloadformat=csv" \
      -o "$zipfile"

    if ! file "$zipfile" | grep -q "Zip archive"; then
        echo "Failed download for $indicator"
        head "$zipfile"
        rm "$zipfile"
        continue
    fi

    unzip -oq "$zipfile" -d "$tmpdir"

    api_file=$(find "$tmpdir" -name "API_*.csv" | head -n 1)
    mv "$api_file" "$save_dir/$name.csv"

    rm -rf "$tmpdir"
    rm "$zipfile"

    echo "Saved → $save_dir/$name.csv"
done

echo "All World Bank indicator downloads completed."
