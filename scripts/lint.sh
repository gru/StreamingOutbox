#!/usr/bin/env bash
# Линт кода: соответствие .editorconfig через dotnet format (--verify-no-changes).
# Обязателен после любых изменений кода/тестов (AGENTS.md, раздел «Линт»).
# Запуск с хоста (docker на хосте, сборочных инструментов локально нет):
#   scripts/lint.sh
set -euo pipefail
cd "$(dirname "$0")/.."

docker run --rm -v "$PWD":/app -w /app mcr.microsoft.com/dotnet/sdk:8.0 \
    dotnet format StreamingOutbox.sln --verify-no-changes

echo "линт: OK"
