#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down
printf 'Demo 已停止，订单、审核与 Kafka 数据仍保留在本项目的 Docker volumes 中。\n'
