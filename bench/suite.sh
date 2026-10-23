#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")"

out=${1:-results/$(date +%Y%m%d-%H%M%S)}
passes=${PASSES:-2}
tools=(pg-cdc debezium sequin pgcdc)
scenarios=("backlog" "latency --rate 1000" "latency --rate 3000")

for pass in $(seq 1 "$passes"); do
  dir="$out/pass$pass"
  mkdir -p "$dir"
  python3 calibrate.py --out "$dir"
  for spec in "${scenarios[@]}"; do
    for tool in "${tools[@]}"; do
      python3 calibrate.py --quick --out "$dir"
      python3 run.py "$tool" $spec --out "$dir"
    done
  done
done

docker ps -aq --filter label=cdcbench=1 | xargs docker rm -f >/dev/null 2>&1
docker network rm cdcbench >/dev/null 2>&1
python3 summarize.py "$out" >"$out/tables.md"
echo "tables in $out/tables.md"
