#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 1 ]; then printf '用法：./scripts/replay.sh ord-订单编号\n'; exit 1; fi
docker compose exec -T php-web php artisan demo:replay-created "$1"
