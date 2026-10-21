#!/usr/bin/env bash
# Ручной повторный запуск идемпотентной инициализации БД (роли, БД outbox,
# таблица outbox.messages, synchronized_standby_slots). Безопасен при
# повторном вызове. ОБЯЗАТЕЛЕН после каждой смены primary (failover/switchover):
# synchronized_standby_slots пишется через ALTER SYSTEM локально в
# postgresql.auto.conf текущего лидера (docs/dependencies.md §5).
set -euo pipefail
cd "$(dirname "$0")/../.."

docker compose run --rm --no-deps init-db
# --no-deps: без реконсиляции графа зависимостей — иначе compose пересоздаёт
# pg-контейнеры и роняет живой Patroni-кластер (init-db сам ждёт haproxy в цикле).
