#!/usr/bin/env bash
# Полный прогон тестов решения (unit + интеграционные) — dependencies.md §2
# «Интеграционные тесты»: одноразовый мини-стенд pg-it/kafka-it
# (docker-compose.tests.yml, проект streaming-outbox-it) поднимается перед
# прогоном и сносится после; порты наружу не публикуются — тесты исполняются
# в контейнере sdk:8.0 в сети стенда. Интеграционные тесты всегда требуют
# поднятый стенд (skip-логики нет): подключение передаётся стандартными env
# CDCOUTBOX__* — те же имена, что у сервиса.
# Быстрые unit-прогоны без стенда:
#   docker run --rm -v "$PWD":/w -w /w mcr.microsoft.com/dotnet/sdk:8.0 \
#       dotnet test tests/CdcOutbox.UnitTests
set -euo pipefail
cd "$(dirname "$0")/.."

compose() { docker compose -p streaming-outbox-it -f docker-compose.tests.yml "$@"; }
cleanup() { compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

compose up -d --wait

docker run --rm --network streaming-outbox-it_default \
    -v "$PWD":/w -w /w \
    -e CDCOUTBOX__POSTGRES__CONNECTIONSTRING='Host=pg-it;Port=5432;Database=outbox;Username=postgres;Password=postgres' \
    -e CDCOUTBOX__KAFKA__BOOTSTRAPSERVERS='kafka-it:9092' \
    mcr.microsoft.com/dotnet/sdk:8.0 \
    bash -ceu 'dotnet restore StreamingOutbox.sln --disable-parallel && dotnet test StreamingOutbox.sln --no-restore'
