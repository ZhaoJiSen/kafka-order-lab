#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# 只为当前进程选择 Java 21，不修改终端配置或全局默认 Java。
java21_home=""
if command -v brew >/dev/null 2>&1; then
  brew_java21="$(brew --prefix)/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
  if [[ -x "$brew_java21/bin/java" ]]; then java21_home="$brew_java21"; fi
fi
if [[ -z "$java21_home" && -x /usr/libexec/java_home ]]; then
  java21_home="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
fi
if [[ -n "$java21_home" ]]; then
  export JAVA_HOME="$java21_home"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if ! command -v java >/dev/null 2>&1 || ! java -version 2>&1 | head -n 1 | grep -Eq 'version "21[.\"]'; then
  echo '需要 Java 21。macOS 可先运行：brew install openjdk@21' >&2
  exit 1
fi
if ! command -v mvn >/dev/null 2>&1; then
  echo '需要 Maven。macOS 可先运行：brew install maven' >&2
  exit 1
fi
mkdir -p .local/data .local/logs
exec mvn -s settings-local.xml spring-boot:run -Dspring-boot.run.profiles=local "$@"
