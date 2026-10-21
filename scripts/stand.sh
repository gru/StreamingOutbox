#!/usr/bin/env bash
# stand.sh — harness управления тестовым стендом streaming-outbox (docs/stand.md).
#
# Использование: scripts/stand.sh <команда> [аргументы]
#
# Жизненный цикл:
#   up [--build]            поднять стенд и дождаться init-db / kafka-init (exit 0)
#   down                    остановить контейнеры (тома и контейнеры остаются)
#   wipe                    полный сброс: контейнеры+сети+тома (reset/wipe.sh)
#   bootstrap               повторный init-db; ОБЯЗАТЕЛЕН после смены primary
#
# Диагностика:
#   status                  сводка: контейнеры, Patroni, сервисы, Prometheus,
#                           список остановленных контейнеров
#   check                   полный health-check стенда (ИТОГ n/n; код != 0 при провале)
#   primary                 текущий leader Patroni-кластера
#   logs <сервис> [N]       последние N строк логов (по умолчанию 50)
#
# Сервисы (по одному; граф зависимостей не трогается):
#   restart <сервис>        перезапуск одного контейнера (docker compose restart)
#   rebuild <сервис>        пересборка и перезапуск одного сервиса (build + up -d --no-deps)
#
# Инъекции отказов и восстановление (docker compose kill = «ручная остановка»:
# рестарт-политика НЕ применяется, возврат только явным restore):
#   fail pg                 kill текущего leader -> выборы нового -> bootstrap
#   fail pg1|pg2|pg3        kill конкретной ноды (реплика: кворум жив)
#   fail pg-all             kill всех трёх нод (полный отказ PG; при возврате
#                           возможна гонка промоушенов — restore стартует пошагово)
#   fail kafka1|kafka2|kafka3   kill одного брокера (кластер жив: RF3/min.isr 2)
#   fail kafka-all          полный outage Kafka (валидатор на KafkaFlow может
#                           упасть — рестарт-политика поднимет после возврата;
#                           Writer от Kafka не зависит и продолжает писать)
#   fail haproxy            kill маршрутизатора (кластер жив, путь записи рвётся)
#   restore [target]        docker compose start: без аргумента — все остановленные
#                           (pg-ноды пошагово); target: pg1|pg2|pg3|kafka1..3|haproxy;
#                           после восстановления pg-нод — ожидание лидера + bootstrap
#   switchover [candidate]  graceful-переключение на кандидата (по умолчанию —
#                           текущий sync_standby) -> ожидание лидера -> bootstrap.
#                           ВНИМАНИЕ: при живом консюмере логического слота (будущий
#                           outbox-сервис) switchover может виснуть — лечится
#                           рестартом сервиса
#
# Тестовый контур (до появления outbox-сервиса доставка в Kafka не идёт —
# вердикт по любому прогону ожидаемо fail с missing=all, docs/stand.md):
#   load <total> [size] [rate]   нагрузочный прогон Writer с ожиданием завершения
#   verdict [runId]              вердикт валидатора (по умолчанию — последний прогон)
set -euo pipefail
cd "$(dirname "$0")/.."

# .env — порты и пароли (в git не попадает; дефолты как в .env.example)
if [ -f .env ]; then . ./.env; fi
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
PATRONI_API_PASSWORD="${PATRONI_API_PASSWORD:-patroni}"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-18080}"
KAFKA_VALIDATOR_HTTP_PORT="${KAFKA_VALIDATOR_HTTP_PORT:-18083}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"

if [ -t 1 ]; then
  GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; CYAN=$'\033[36m'; NC=$'\033[0m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; NC=''
fi

TOTAL=0; FAILED=0
ok()  { TOTAL=$((TOTAL + 1)); echo "  ${GREEN}ok${NC}  $1"; }
bad() { TOTAL=$((TOTAL + 1)); FAILED=$((FAILED + 1)); echo "  ${RED}BAD${NC} $1"; }
info() { echo "  ${YELLOW}--${NC} $1"; }
section() { echo; echo "${CYAN}==> ${1}${NC}"; }
finish() {
  echo
  if [ "${FAILED}" -eq 0 ]; then
    echo "${GREEN}ИТОГ: ${TOTAL}/${TOTAL} ok${NC}"
  else
    echo "${RED}ИТОГ: провалов ${FAILED} из ${TOTAL} проверок${NC}"
    exit 1
  fi
}

usage() { sed -n '3,57p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

# ---------- общие помощники ----------

# psql на primary: через haproxy из первого живого pg-контейнера.
psql_primary() {
  local out node
  for node in pg1 pg2 pg3; do
    docker compose ps --status running --format '{{.Name}}' 2>/dev/null \
      | grep -q -e "-${node}-1" || continue
    if out=$(docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD}" "${node}" \
        psql -h haproxy -U postgres "$@" 2>/dev/null); then
      printf '%s\n' "${out}"
      return 0
    fi
  done
  return 1
}

db_value() { psql_primary -d outbox -tAc "$1" 2>/dev/null | head -1 || true; }

db_check() { # $1=запрос, $2=ожидание, $3=название
  local v
  v=$(db_value "$1")
  if [ "${v}" = "$2" ]; then ok "$3"; else bad "$3 (фактически: '${v:-<пусто/отказ>}')"; fi
}

# Код ответа HTTP (000 — соединение не установлено; curl печатает его сам и при
# ошибке, отдельный fallback не нужен — иначе «000000»).
http_code() { curl -s -o /dev/null -m 5 -w '%{http_code}' "$1" 2>/dev/null || true; }

# /cluster Patroni из первого отвечающего REST-порта (лидер-нода может лежать).
patroni_cluster() {
  local p out
  for p in 8008 8009 8010; do
    out=$(curl -s -m 3 "localhost:${p}/cluster" 2>/dev/null) || continue
    if printf '%s' "${out}" | python3 -c '
import json, sys
try:
    json.load(sys.stdin)["members"]
except Exception:
    sys.exit(1)' >/dev/null 2>&1; then
      printf '%s' "${out}"
      return 0
    fi
  done
  return 1
}

cluster_leader() { patroni_cluster | python3 -c '
import json, sys
ms = json.load(sys.stdin)["members"]
print("".join(m["name"] for m in ms if m["role"] == "leader"))' 2>/dev/null || true; }

cluster_sync() { patroni_cluster | python3 -c '
import json, sys
ms = json.load(sys.stdin)["members"]
print("".join(m["name"] for m in ms if m["role"] == "sync_standby"))' 2>/dev/null || true; }

patroni_summary() {
  patroni_cluster | python3 -c '
import json, sys
ms = json.load(sys.stdin)["members"]
ldr = [m["name"] for m in ms if m["role"] == "leader"]
syn = [m["name"] for m in ms if m["role"] == "sync_standby"]
print("leader=%s sync=%s members=%d timeline=%s" % (
    ",".join(ldr) or "?", ",".join(syn), len(ms), ms[0].get("timeline", "?")))' 2>/dev/null \
    || echo "недоступен"
}

# Ожидание ровно одного лидера: $1 — ожидаемое имя; пусто — любой;
# «!имя» — любой, КРОМЕ указанного (после kill лидера DCS ещё несколько секунд
# держит старую запись — «любой» вернул бы убитого лидера мгновенно).
# Печатает имя лидера; код != 0 — не дождались (в stdout тогда текущее значение).
wait_leader() {
  local expect="${1:-}" tmo="${2:-120}"
  local start now leaders cluster
  start=$(date +%s)
  leaders=""
  while :; do
    if cluster=$(patroni_cluster); then
      leaders=$(printf '%s' "${cluster}" | python3 -c '
import json, sys
ms = json.load(sys.stdin)["members"]
print(",".join(m["name"] for m in ms if m["role"] == "leader"))' 2>/dev/null || true)
      if [ -n "${leaders}" ] && ! echo "${leaders}" | grep -q ','; then
        if [ -z "${expect}" ]; then
          printf '%s' "${leaders}"; return 0
        elif case "${expect}" in '!'*) [ "${leaders}" != "${expect#!}" ] ;; *) [ "${leaders}" = "${expect}" ] ;; esac; then
          printf '%s' "${leaders}"; return 0
        fi
      fi
    fi
    now=$(date +%s)
    if [ $((now - start)) -ge "${tmo}" ]; then
      printf '%s' "${leaders}"
      return 1
    fi
    sleep 2
  done
}

# Ожидание healthy-статуса контейнера (у всех целей kill есть healthcheck).
wait_healthy() { # $1=сервис, $2=таймаут_сек
  local i=0 st
  while [ "${i}" -le $(($2 / 3)) ]; do
    st=$(container_status "$1")
    if echo "${st}" | grep -q '^Up.*healthy'; then return 0; fi
    sleep 3
    i=$((i + 1))
  done
  return 1
}

# Идемпотентный init-db (refresh synchronized_standby_slots на текущем лидере).
run_bootstrap() {
  echo "  -> bootstrap: apply на лидере (synchronized_standby_slots, dependencies.md §5)"
  if docker compose run --rm --no-deps init-db 2>&1 | tail -2; then
    return 0
  fi
  echo "${RED}  bootstrap не удался${NC}"
  return 1
}

# Settle после смены топологии PG: Patroni при demote рвёт сессии Writer'а
# (57P01), и ПЕРВЫЙ проба /ready попадает на мёртвый коннект пула Npgsql —
# разовая 503, коннект вычищается, следующие пробы проходят. Ждём готовность,
# чтобы «check» сразу после переключения не ловил это ожидаемое окно.
wait_writer_ready() { # $1=таймаут_сек
  local i=0 code
  while [ "${i}" -le $(($1 / 2)) ]; do
    code=$(http_code "localhost:${WRITER_HTTP_PORT}/ready")
    if [ "${code}" = "200" ]; then return 0; fi
    sleep 2
    i=$((i + 1))
  done
  return 1
}

settle_writer() {
  echo "Ожидание готовности Writer /ready (потолок 30с)..."
  if wait_writer_ready 30; then
    echo "  ${GREEN}Writer /ready: 200${NC}"
  else
    echo "${YELLOW}  Writer /ready не вернулся к 200 за 30с — проверь: ./scripts/stand.sh check${NC}"
  fi
}

# Мониторинг: postgres-exporter после серии переключений лидера может залипнуть
# на мёртвых соединениях (EOF на каждом скрейпе, pg_up пропадает и сам не
# восстанавливается — проверено на стенде) — лечится рестартом контейнера.
settle_monitoring() {
  local i=0 v
  while [ "${i}" -le 15 ]; do
    v=$(prom_query 'pg_up')
    if [ "${v}" = "1" ]; then echo "  ${GREEN}pg_up=1${NC}"; return 0; fi
    sleep 2
    i=$((i + 1))
  done
  echo "${YELLOW}  pg_up не вернулся за 30с — рестарт postgres-exporter (залипающий пул соединений)${NC}"
  docker compose restart postgres-exporter >/dev/null 2>&1 || true
  i=0
  while [ "${i}" -le 15 ]; do
    v=$(prom_query 'pg_up')
    if [ "${v}" = "1" ]; then echo "  ${GREEN}pg_up=1 после рестарта exporter'а${NC}"; return 0; fi
    sleep 2
    i=$((i + 1))
  done
  echo "${RED}  pg_up не появился — проверь postgres-exporter: ./scripts/stand.sh logs postgres-exporter${NC}"
  return 1
}

prom_query() {
  curl -s -m 5 --get "localhost:${PROMETHEUS_PORT}/api/v1/query" \
    --data-urlencode "query=$1" 2>/dev/null \
    | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["data"]["result"]
    print(r[0]["value"][1] if r else "")
except Exception:
    pass' 2>/dev/null || true
}

wait_oneshot() { # $1=сервис, $2=таймаут_сек
  local i=0 st
  while [ "${i}" -le $(($2 / 5)) ]; do
    st=$(docker compose ps -a --format '{{.Status}}' "$1" 2>/dev/null || true)
    case "${st}" in
      'Exited (0)'*) return 0 ;;
      Exited*) return 1 ;;
    esac
    sleep 5
    i=$((i + 1))
  done
  return 1
}

container_status() { docker compose ps -a --format '{{.Status}}' "$1" 2>/dev/null || true; }

stopped_services() { # долгоживущие остановленные контейнеры (one-shot исключены)
  docker compose ps -a --status exited --format '{{.Service}}' 2>/dev/null \
    | grep -v -e '^init-db$' -e '^kafka-init$' || true
}

# ---------- команды: жизненный цикл ----------

cmd_up() {
  local build_flag=""
  if [ "${1:-}" = "--build" ]; then build_flag="--build"; fi
  docker compose up -d ${build_flag}
  section "Ожидание init-db (первичный bootstrap кластера — до ~3 мин)"
  if wait_oneshot init-db 300; then
    echo "  init-db: Exited 0"
  else
    echo "${RED}init-db не завершился кодом 0${NC}; последние логи:"
    docker compose logs --tail 30 init-db || true
    exit 1
  fi
  if wait_oneshot kafka-init 300; then
    echo "  kafka-init: Exited 0"
  else
    echo "${RED}kafka-init не завершился кодом 0${NC}; последние логи:"
    docker compose logs --tail 30 kafka-init || true
    exit 1
  fi
  echo "Стенд поднят. Полная проверка: ./scripts/stand.sh check"
}

cmd_down() {
  docker compose stop
  echo "Контейнеры остановлены (тома сохранены). Запуск: ./scripts/stand.sh up"
}

# ---------- команды: диагностика ----------

cmd_status() {
  docker compose ps -a
  echo
  echo "Patroni:    $(patroni_summary)"
  echo "Writer:     /ready -> $(http_code "localhost:${WRITER_HTTP_PORT}/ready")"
  echo "Validator:  $(curl -s -m 3 "localhost:${KAFKA_VALIDATOR_HTTP_PORT}/api/validation/state" 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print("state=%s records=%s runs=%s" % (d["state"], d["recordsConsumed"], d["runsSeen"]))' 2>/dev/null \
    || echo 'недоступен')"
  echo "Prometheus: targets up $(curl -s -m 3 "localhost:${PROMETHEUS_PORT}/api/v1/targets?state=active" 2>/dev/null \
    | python3 -c 'import json,sys; ts=json.load(sys.stdin)["data"]["activeTargets"]; print("%d/%d" % (sum(1 for t in ts if t["health"]=="up"), len(ts)))' 2>/dev/null \
    || echo 'недоступен')"
  local stopped
  stopped=$(stopped_services | tr '\n' ' ')
  if [ -n "${stopped}" ]; then
    echo "${YELLOW}Остановлены (возврат: ./scripts/stand.sh restore): ${stopped}${NC}"
  fi
}

cmd_primary() { echo "Patroni: $(patroni_summary)"; }

cmd_check() {
  section "Compose-конфигурация"
  if docker compose config -q 2>/dev/null; then ok "docker compose config валиден"; else bad "docker compose config невалиден"; fi

  section "Контейнеры"
  local s st
  for s in etcd1 etcd2 etcd3 pg1 pg2 pg3 haproxy kafka1 kafka2 kafka3 prometheus writer kafka-validator; do
    st=$(container_status "${s}")
    if echo "${st}" | grep -q '^Up.*healthy'; then ok "${s}: ${st}"; else bad "${s}: ${st:-<нет контейнера>}"; fi
  done
  for s in postgres-exporter kafka-exporter; do
    st=$(container_status "${s}")
    if echo "${st}" | grep -q '^Up'; then ok "${s}: ${st} (без healthcheck — target-up в Prometheus)"; else bad "${s}: ${st:-<нет контейнера>}"; fi
  done
  for s in init-db kafka-init; do
    st=$(container_status "${s}")
    if echo "${st}" | grep -q '^Exited (0)'; then ok "${s}: Exited 0"; else bad "${s}: ${st:-<нет контейнера>}"; fi
  done

  section "Patroni-кластер"
  local cluster
  if cluster=$(patroni_cluster); then
    ok "REST /cluster отвечает"
    local members leaders sync
    members=$(printf '%s' "${cluster}" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["members"]))')
    leaders=$(printf '%s' "${cluster}" | python3 -c 'import json,sys; print(sum(1 for m in json.load(sys.stdin)["members"] if m["role"]=="leader"))')
    sync=$(printf '%s' "${cluster}" | python3 -c 'import json,sys; print(sum(1 for m in json.load(sys.stdin)["members"] if m["role"]=="sync_standby"))')
    [ "${members}" = "3" ] && ok "3 члена кластера" || bad "членов кластера: ${members}"
    [ "${leaders}" = "1" ] && ok "ровно один leader" || bad "leader-нод: ${leaders}"
    [ "${sync}" -ge 1 ] 2>/dev/null && ok "sync_standby присутствует (синхронная репликация)" || bad "sync_standby отсутствует"
  else
    bad "REST /cluster недоступен (8008-8010)"
  fi
  db_check "SELECT pg_is_in_recovery()" "f" "через haproxy доступен primary (pg_is_in_recovery=f)"
  local slots
  slots=$(db_value "SHOW synchronized_standby_slots")
  if [ -n "${slots}" ]; then ok "synchronized_standby_slots = ${slots}"; else bad "synchronized_standby_slots пуст"; fi

  section "Роли и объекты БД (primary, БД outbox)"
  db_check "SELECT count(*) FROM pg_database WHERE datname='outbox'" "1" "БД outbox существует"
  db_check "SELECT to_regclass('outbox.messages') IS NOT NULL" "t" "таблица outbox.messages существует"
  db_check "SELECT count(*) FROM pg_roles WHERE rolname='sys_writer' AND rolcanlogin AND NOT rolreplication" "1" "роль sys_writer (LOGIN, NOREPLICATION)"
  db_check "SELECT count(*) FROM pg_roles WHERE rolname='sys_replication' AND rolcanlogin AND rolreplication" "1" "роль sys_replication (LOGIN, REPLICATION)"
  db_check "SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname='sys_monitor' AND m.roleid=(SELECT oid FROM pg_roles WHERE rolname='pg_monitor') AND m.inherit_option" "1" "sys_monitor: pg_monitor с inherit=true"
  db_check "SELECT count(*) FROM information_schema.role_table_grants WHERE table_schema='outbox' AND table_name='messages' AND grantee='sys_writer' AND privilege_type IN ('INSERT','DELETE')" "2" "sys_writer: INSERT+DELETE на outbox.messages"
  db_check "SELECT count(*) FROM information_schema.column_privileges WHERE table_schema='outbox' AND table_name='messages' AND grantee='sys_writer' AND column_name='id' AND privilege_type='SELECT'" "1" "sys_writer: колоночный SELECT(id)"
  db_check "SELECT has_schema_privilege('sys_writer','outbox','USAGE')" "t" "sys_writer: USAGE на схему outbox"
  db_check "SELECT has_sequence_privilege('sys_writer', pg_get_serial_sequence('outbox.messages','id'), 'USAGE')" "t" "sys_writer: USAGE на identity-последовательность"
  local pubs
  pubs=$(db_value "SELECT count(*) FROM pg_publication")
  if [ "${pubs}" = "0" ]; then
    info "publication/slot отсутствуют — ожидаемо до появления outbox-сервиса (dependencies.md §4-§5)"
  else
    info "publication существует (publications=${pubs}) — outbox-сервис уже развёрнут"
  fi

  section "Kafka: topic outbox.events"
  local kdesc
  kdesc=$(docker compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka1:9092 --describe --topic outbox.events 2>/dev/null | head -1 || true)
  if echo "${kdesc}" | grep -q 'Topic: outbox.events'; then
    ok "topic существует"
    echo "${kdesc}" | grep -q 'PartitionCount: 6' && ok "6 партиций" || bad "PartitionCount != 6"
    echo "${kdesc}" | grep -q 'ReplicationFactor: 3' && ok "replication factor 3" || bad "ReplicationFactor != 3"
    echo "${kdesc}" | grep -q 'min.insync.replicas=2' && ok "min.insync.replicas=2" || bad "min.insync.replicas != 2"
  else
    bad "topic не описывается (kafka1 недоступен или topic нет)"
  fi

  section "Сервисы тестового контура"
  local hb
  hb=$(curl -s -m 5 "localhost:${WRITER_HTTP_PORT}/health" 2>/dev/null || true)
  [ "${hb}" = '{"status":"Healthy"}' ] && ok "Writer /health: ${hb}" || bad "Writer /health: '${hb:-<нет ответа>}'"
  [ "$(http_code "localhost:${WRITER_HTTP_PORT}/ready")" = "200" ] \
    && ok "Writer /ready: 200 (postgres-чек)" || bad "Writer /ready != 200"
  [ "$(http_code "localhost:${KAFKA_VALIDATOR_HTTP_PORT}/health")" = "200" ] \
    && ok "KafkaValidator /health: 200" || bad "KafkaValidator /health != 200"
  [ "$(http_code "localhost:${KAFKA_VALIDATOR_HTTP_PORT}/ready")" = "200" ] \
    && ok "KafkaValidator /ready: 200 (движок running)" || bad "KafkaValidator /ready != 200"
  local vstate
  vstate=$(curl -s -m 5 "localhost:${KAFKA_VALIDATOR_HTTP_PORT}/api/validation/state" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || true)
  [ "${vstate}" = "running" ] && ok "валидатор: state=running" || bad "валидатор: state='${vstate:-<нет ответа>}'"

  section "Мониторинг"
  [ "$(http_code "localhost:${PROMETHEUS_PORT}/-/ready")" = "200" ] \
    && ok "Prometheus /-/ready" || bad "Prometheus не готов"
  local tup ttotal
  ttotal=$(curl -s -m 5 "localhost:${PROMETHEUS_PORT}/api/v1/targets?state=active" 2>/dev/null \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["data"]["activeTargets"]))' 2>/dev/null || echo 0)
  tup=$(curl -s -m 5 "localhost:${PROMETHEUS_PORT}/api/v1/targets?state=active" 2>/dev/null \
    | python3 -c 'import json,sys; print(sum(1 for t in json.load(sys.stdin)["data"]["activeTargets"] if t["health"]=="up"))' 2>/dev/null || echo 0)
  if [ "${tup}" = "8" ] && [ "${ttotal}" = "8" ]; then
    ok "все 8 таргетов up (etcd×3, writer, validator, 2 exporter'а, prometheus)"
  else
    bad "таргеты up ${tup}/${ttotal} (ожидается 8/8)"
  fi
  [ "$(prom_query 'pg_up')" = "1" ] && ok "pg_up=1" || bad "pg_up != 1 (postgres-exporter)"
  [ "$(prom_query 'kafka_brokers')" = "3" ] && ok "kafka_brokers=3" || bad "kafka_brokers != 3"

  finish
}

# ---------- команды: сервисы ----------

cmd_restart() {
  local s="${1:?укажи сервис, например: stand.sh restart writer}"
  docker compose restart "${s}"
  echo "Перезапущен ${s}. После рестарта pg-контейнеров при необходимости: stand.sh restart haproxy"
}

cmd_rebuild() {
  local s="${1:?укажи сервис, например: stand.sh rebuild writer}"
  docker compose build "${s}"
  docker compose up -d --no-deps "${s}"
  echo "Сервис ${s} пересобран и перезапущен (--no-deps: граф зависимостей не тронут)."
}

cmd_logs() {
  local s="${1:?укажи сервис, например: stand.sh logs writer 100}"
  local n="${2:-50}"
  docker compose logs --tail "${n}" "${s}"
}

# ---------- команды: инъекции отказов ----------

cmd_fail() {
  local target="${1:?usage: stand.sh fail pg|pg1|pg2|pg3|pg-all|kafka1|kafka2|kafka3|kafka-all|haproxy}"
  case "${target}" in
    pg|pg1|pg2|pg3|pg-all|kafka1|kafka2|kafka3|kafka-all|haproxy) ;;
    *) echo "Неизвестный target: ${target}"; usage 1 ;;
  esac

  local node cur_leader
  case "${target}" in
    pg)
      cur_leader=$(cluster_leader)
      [ -n "${cur_leader}" ] || { echo "${RED}Лидер не определён — кластер без лидера${NC}"; exit 1; }
      node="${cur_leader}"
      ;;
    pg1|pg2|pg3)
      cur_leader=$(cluster_leader)
      node="${target}"
      ;;
  esac

  case "${target}" in
    pg|pg1|pg2|pg3)
      echo "kill ${node} (лидер сейчас: ${cur_leader:-<не определён>})"
      docker compose kill "${node}"
      echo "  ${node} остановлен (kill = ручная остановка; возврат: stand.sh restore ${node})"
      if [ "${node}" = "${cur_leader}" ]; then
        echo "Ожидание выборов нового лидера (потолок 120с)..."
        local t0 new
        t0=$(date +%s)
        new=""
        if new=$(wait_leader "!${cur_leader}" 120); then
          echo "  ${GREEN}Лидер: ${cur_leader} -> ${new} (выборы за $(( $(date +%s) - t0 ))с)${NC}"
          run_bootstrap
          settle_writer
          settle_monitoring
        else
          echo "${RED}Новый лидер не избран за 120с (текущее: ${new:-нет})${NC}"
          exit 1
        fi
      fi
      ;;
    pg-all)
      for node in pg1 pg2 pg3; do
        docker compose kill "${node}"
        echo "  ${node} остановлен"
      done
      echo "Полный отказ PG. Возврат: stand.sh restore (старт пошаговый, затем выборы + bootstrap)"
      ;;
    kafka1|kafka2|kafka3)
      docker compose kill "${target}"
      echo "  ${target} остановлен (кластер жив: RF3/min.insync.replicas=2). Возврат: stand.sh restore ${target}"
      ;;
    kafka-all)
      docker compose kill kafka1 kafka2 kafka3
      echo "Полный outage Kafka."
      info "валидатор (KafkaFlow) может упасть по известному дефекту стека — рестарт-политика поднимет его после возврата брокеров; Writer от Kafka не зависит"
      echo "Возврат: stand.sh restore"
      ;;
    haproxy)
      docker compose kill haproxy
      echo "  haproxy остановлен (кластер PG жив, путь записи рвётся — Writer /ready падает). Возврат: stand.sh restore haproxy"
      ;;
  esac
}

cmd_restore() {
  local target="${1:-}"
  local started pg_touched
  started=""
  pg_touched=0

  if [ -n "${target}" ]; then
    case "${target}" in
      pg1|pg2|pg3|kafka1|kafka2|kafka3|haproxy) ;;
      *) echo "Неизвестный target: ${target} (pg1|pg2|pg3|kafka1..3|haproxy)"; exit 1 ;;
    esac
    docker compose start "${target}"
    started="${target}"
    case "${target}" in pg*) pg_touched=1 ;; esac
  else
    local svcs s
    svcs=$(stopped_services)
    if [ -z "${svcs}" ]; then
      echo "Остановленных контейнеров нет."
      return 0
    fi
    for s in ${svcs}; do
      docker compose start "${s}"
      started="${started} ${s}"
      # pg-ноды стартуют пошагово (пауза 3с) — анти-флап промоушенов Patroni
      case "${s}" in pg*) sleep 3; pg_touched=1 ;; esac
    done
    started="${started# }"
  fi

  echo "Запущено:${started}"
  local s
  for s in ${started}; do
    if wait_healthy "${s}" 120; then
      echo "  ${s}: $(container_status "${s}")"
    else
      echo "  ${YELLOW}${s}: не стал healthy за 120с ($(container_status "${s}"))${NC}"
    fi
  done

  if [ "${pg_touched}" = "1" ]; then
    echo "Ожидание лидера после восстановления pg-нод..."
    local new
    new=""
    if new=$(wait_leader "" 120); then
      echo "  ${GREEN}Лидер: ${new}${NC}"
      run_bootstrap
      settle_writer
      settle_monitoring
    else
      echo "${RED}Лидер не избран за 120с (текущее: ${new:-нет})${NC}"
      exit 1
    fi
  fi
  echo "Кластер: $(patroni_summary)"
  echo "Проверка: ./scripts/stand.sh check"
}

cmd_switchover() {
  local leader candidate
  leader=$(cluster_leader)
  [ -n "${leader}" ] || { echo "${RED}Лидер не определён${NC}"; exit 1; }
  candidate="${1:-}"
  if [ -z "${candidate}" ]; then
    candidate=$(cluster_sync)
    [ -n "${candidate}" ] || { echo "sync_standby не найден; укажи кандидата: stand.sh switchover pg2"; exit 1; }
  fi
  case "${candidate}" in
    pg1|pg2|pg3) ;;
    *) echo "Кандидат должен быть pg1|pg2|pg3"; exit 1 ;;
  esac
  if [ "${candidate}" = "${leader}" ]; then
    echo "Кандидат ${candidate} уже лидер"; exit 0
  fi

  local port
  case "${candidate}" in
    pg1) port=8008 ;;
    pg2) port=8009 ;;
    pg3) port=8010 ;;
  esac
  echo "Switchover ${leader} -> ${candidate} (REST-запрос на порт кандидата :${port})"
  local resp code body
  resp=$(curl -s -m 60 -u "patroni:${PATRONI_API_PASSWORD}" \
    -X POST "localhost:${port}/switchover" \
    -H 'Content-Type: application/json' \
    -d "{\"leader\": \"${leader}\", \"candidate\": \"${candidate}\"}" \
    -w '\n%{http_code}') || resp=$'\n000'
  code=$(printf '%s' "${resp}" | tail -1)
  body=$(printf '%s' "${resp}" | sed '$d')
  echo "  REST ${code}: ${body:-<пусто>}"
  case "${code}" in
    200) ;;
    503) info "посредник ответил 503 (переключение может быть запланировано) — продолжаю ожидание" ;;
    *) echo "${RED}switchover отклонён${NC}"; exit 1 ;;
  esac

  echo "Ожидание лидера ${candidate} (потолок 120с)..."
  local new
  new=""
  if new=$(wait_leader "${candidate}" 120); then
    echo "  ${GREEN}Лидер: ${leader} -> ${new}${NC}"
    run_bootstrap
    settle_writer
    settle_monitoring
  else
    echo "${RED}${candidate} не стал лидером за 120с (лидер: ${new:-нет})${NC}"
    exit 1
  fi
}

# ---------- команды: тестовый контур ----------

cmd_load() { # $1=total, $2=size(256), $3=rate(без лимита)
  local total="${1:?usage: stand.sh load <total> [size] [rate]}"
  local size="${2:-256}"
  local rate="${3:-}"
  local body="{\"totalMessages\": ${total}, \"messageSizeBytes\": ${size}"
  if [ -n "${rate}" ]; then body="${body}, \"ratePerSecond\": ${rate}"; fi
  body="${body}}"
  local resp runid
  resp=$(curl -s -X POST "localhost:${WRITER_HTTP_PORT}/api/generation/start" \
    -H 'Content-Type: application/json' -d "${body}")
  runid=$(printf '%s' "${resp}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["runId"])' 2>/dev/null) \
    || { echo "${RED}Не удалось запустить прогон: ${resp}${NC}"; exit 1; }
  echo "Прогон ${runid}: total=${total} size=${size}B rate=${rate:-без лимита}"
  local i st
  st="?"
  for i in $(seq 1 600); do
    st=$(curl -s -m 5 "localhost:${WRITER_HTTP_PORT}/api/generation/state" 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || echo '?')
    case "${st}" in
      running|idle) sleep 2 ;;
      *) break ;;
    esac
  done
  echo "Финальное состояние: ${st}"
  curl -s "localhost:${WRITER_HTTP_PORT}/api/generation/results" | python3 -m json.tool
}

cmd_verdict() { # $1=runId (по умолчанию — последний прогон: реестр валидатора,
  # а если topic пуст (outbox-сервис ещё не доставляет) — последний прогон Writer)
  local rid="${1:-}"
  if [ -z "${rid}" ]; then
    rid=$(curl -s -m 5 "localhost:${KAFKA_VALIDATOR_HTTP_PORT}/api/validation/runs" 2>/dev/null \
      | python3 -c 'import json,sys; a=json.load(sys.stdin); print(a[-1]["runId"] if a else "")' 2>/dev/null || true)
  fi
  if [ -z "${rid}" ]; then
    rid=$(curl -s -m 5 "localhost:${WRITER_HTTP_PORT}/api/generation/results" 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("runId") or "")' 2>/dev/null || true)
  fi
  if [ -z "${rid}" ]; then
    echo "Нет прогонов ни в реестре валидатора, ни у Writer."
    exit 1
  fi
  echo "Вердикт по прогону ${rid} (ожидаемый набор из Writer: fetchFromWriter):"
  curl -s -X POST "localhost:${KAFKA_VALIDATOR_HTTP_PORT}/api/validation/validate" \
    -H 'Content-Type: application/json' \
    -d "{\"runId\": \"${rid}\", \"fetchFromWriter\": true}" | python3 -m json.tool
}

# ---------- диспетчер ----------

cmd="${1:-}"
if [ -n "${cmd}" ] && [ "$#" -gt 0 ]; then shift; fi
case "${cmd}" in
  up)         cmd_up "$@" ;;
  down)       cmd_down ;;
  wipe)       exec scripts/reset/wipe.sh ;;
  bootstrap)  exec scripts/init/apply-bootstrap.sh ;;
  status)     cmd_status ;;
  check)      cmd_check ;;
  primary)    cmd_primary ;;
  restart)    cmd_restart "$@" ;;
  rebuild)    cmd_rebuild "$@" ;;
  logs)       cmd_logs "$@" ;;
  fail)       cmd_fail "$@" ;;
  restore)    cmd_restore "$@" ;;
  switchover) cmd_switchover "$@" ;;
  load)       cmd_load "$@" ;;
  verdict)    cmd_verdict "$@" ;;
  -h|--help|help|"") usage ;;
  *) echo "Неизвестная команда: ${cmd}"; usage 1 ;;
esac
