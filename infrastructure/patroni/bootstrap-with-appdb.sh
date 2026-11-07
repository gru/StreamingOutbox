#!/bin/bash
# Кастомный bootstrap-метод Patroni. Готовит PGDATA, в котором С САМОГО НАЧАЛА существует
# прикладная БД `outbox`.

set -euo pipefail

DATA_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --datadir=*|--data_dir=*) DATA_DIR="${1#*=}" ;;
    --scope=*) ;;
  esac
  shift
done
[[ -n "$DATA_DIR" ]] || { echo "bootstrap-with-appdb: --datadir not given" >&2; exit 1; }

# Уже инициализированный PGDATA (рестарт с сохранными томами): выходим с успехом,
# БД существует с момента первоначального bootstrap.
[[ -f "$DATA_DIR/PG_VERSION" ]] && exit 0

PG_BIN=/usr/lib/postgresql/17/bin
SOCK_DIR=/tmp/bootstrap-with-appdb
PORT=5499
mkdir -p "$SOCK_DIR"

"$PG_BIN/initdb" -D "$DATA_DIR" \
  --encoding=UTF8 --locale=C.UTF-8 --data-checksums \
  --auth-local=trust --auth-host=scram-sha-256 >/dev/null

cleanup() { "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m immediate -w stop >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Временный инстанс только на unix-сокете (TCP выключен) ровно на время CREATE DATABASE.
"$PG_BIN/pg_ctl" -D "$DATA_DIR" -w -t 60 \
  -o "-c listen_addresses= -c unix_socket_directories=$SOCK_DIR -p $PORT" \
  start >/dev/null
psql -h "$SOCK_DIR" -p "$PORT" -U postgres -d postgres \
  -v ON_ERROR_STOP=1 -c "CREATE DATABASE outbox" >/dev/null

"$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast -w stop >/dev/null
trap - EXIT
echo "bootstrap-with-appdb: PGDATA готов, БД outbox создана"
