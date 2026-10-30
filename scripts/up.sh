#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f .env ]; then cp .env.example .env; fi
if ! grep -Eq '^APP_KEY=base64:.+' .env; then
  demo_key="$(openssl rand -base64 32)"
  awk -v demo_key="$demo_key" '{if (/^APP_KEY=/) print "APP_KEY=base64:" demo_key; else print}' .env > .env.tmp
  mv .env.tmp .env
  chmod 600 .env
fi
./scripts/build.sh
docker compose up -d --no-build --wait --wait-timeout 180
printf '\n学习页面：http://localhost:%s\n' "$(sed -n 's/^PHP_PORT=//p' .env | tail -1)"
printf '日志：docker compose logs -f php-outbox java-review php-results\n'
