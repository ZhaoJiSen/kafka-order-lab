#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# 使用兼容构建器绕过当前机器 BuildKit 的 lease 缓存错误；仅影响本条命令。
export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-0}"
file_proxy=""
if [ -f .env ]; then file_proxy="$(sed -n 's/^BUILD_PROXY=//p' .env | tail -1)"; fi
demo_proxy="${BUILD_PROXY:-$file_proxy}"
if ! docker image inspect mq-order-lab-php-runtime:local >/dev/null 2>&1; then
 docker build --build-arg HTTP_PROXY="$demo_proxy" --build-arg HTTPS_PROXY="$demo_proxy" -f php-laravel/Dockerfile.runtime -t mq-order-lab-php-runtime:local php-laravel
fi
php_base=mq-order-lab-php-runtime:local
if docker image inspect mq-order-lab-php-deps:local >/dev/null 2>&1; then php_base=mq-order-lab-php-deps:local; fi
docker build --build-arg PHP_BASE_IMAGE="$php_base" --build-arg HTTP_PROXY="$demo_proxy" --build-arg HTTPS_PROXY="$demo_proxy" -t mq-order-lab-php:local php-laravel
java_base=maven:3.9.11-eclipse-temurin-21
if docker image inspect mq-order-lab-java-deps:local >/dev/null 2>&1; then java_base=mq-order-lab-java-deps:local; fi
docker build --build-arg JAVA_BASE_IMAGE="$java_base" --build-arg BUILD_PROXY="$demo_proxy" -t mq-order-lab-java:local java-review
