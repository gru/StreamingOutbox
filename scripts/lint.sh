#!/usr/bin/env bash
# Линт кода: соответствие .editorconfig через dotnet format (--verify-no-changes).
# Обязателен после любых изменений кода/тестов (AGENTS.md, раздел «Линт»).
# Запуск с хоста (docker на хосте, сборочных инструментов локально нет):
#   scripts/lint.sh
# Репозиторий монтируется read-only, lint работает с копией исходников внутри
# одноразового контейнера: obj/bin не появляются в папке проекта — запись из
# контейнера в bind-mount ломает file-watcher IDE (VirtioFS теряет события,
# подсветка/типы восстанавливаются только пересборкой).
set -euo pipefail
cd "$(dirname "$0")/.."

docker run --rm -v "$PWD":/src:ro mcr.microsoft.com/dotnet/sdk:8.0 \
    bash -ceu 'mkdir -p /work && cd /src && tar --exclude=bin --exclude=obj --exclude=.git -cf - . | tar -C /work -xf - && cd /work && dotnet format StreamingOutbox.sln --verify-no-changes'

echo "линт: OK"
