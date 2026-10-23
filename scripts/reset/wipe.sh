#!/usr/bin/env bash
# Полный сброс тестового стенда: контейнеры, сети и volumes ТОЛЬКО этого проекта
# (compose project streaming-outbox). Данные других Docker-проектов не затрагиваются.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "==> Остановка и удаление контейнеров, сетей и volumes стенда..."
# cdc-outbox-2 закрыт профилем redundancy: без флага down его не видит —
# контейнер пережил бы wipe, держал сеть проекта и после нового up
# переподключился к новому кластеру (грабли профильных сервисов старого
# стенда: «Resource is still in use» при каждом down).
docker compose --profile redundancy down -v --remove-orphans --timeout 30

# Мини-стенд интеграционных тестов (scripts/test.sh сносит его сам; здесь —
# остатки после жёсткого прерывания прогона).
docker compose -p streaming-outbox-it -f docker-compose.tests.yml down -v --remove-orphans >/dev/null 2>&1 || true
echo "==> Готово. Стенд полностью удалён. Для запуска с нуля: docker compose up -d --build"
