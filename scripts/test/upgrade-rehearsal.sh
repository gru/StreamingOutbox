#!/usr/bin/env bash
# upgrade-rehearsal.sh — репетиция обновления PostgreSQL по пути dump/restore
# (порядок обновления — AGENTS.md «Порядок обновления PostgreSQL»). Сценарий «большого апгрейда»:
# тишина (idle-предупреждение + нулевой checkpoint lag) -> stop cdc-outbox-1 ->
# dump БД outbox -> «новый кластер» (DROP/CREATE DATABASE + restore; слот
# исчезает вместе со старой БД — допущение пути dump/restore) -> apply-bootstrap
# (CONNECT-гранты ролей в plain pg_dump не входят) -> recovery-старт
# (CDCOUTBOX__RECOVERYMODE=true на один запуск) -> свежий прогон pass ->
# возврат к дефолту.
#
# Использование: scripts/test/upgrade-rehearsal.sh
# Предусловия: стенд поднят (scripts/stand.sh up), cdc-outbox-1 активен,
# нет активного прогона Writer. СЦЕНАРИЙ РАЗРУШИТЕЛЬНЫЙ для БД outbox
# (DROP DATABASE WITH (FORCE) + восстановление из дампа) — запускать отдельно
# от приёмочного харнесса. Код возврата 0 — все проверки зелёные.
set -uo pipefail
cd "$(dirname "$0")/../.."

# .env — порты и пароли (в git не попадает; дефолты как в .env.example)
if [ -f .env ]; then . ./.env; fi
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-18080}"
KAFKA_VALIDATOR_HTTP_PORT="${KAFKA_VALIDATOR_HTTP_PORT:-18083}"
CDC_OUTBOX_1_HTTP_PORT="${CDC_OUTBOX_1_HTTP_PORT:-18084}"
CDC_OUTBOX_2_HTTP_PORT="${CDC_OUTBOX_2_HTTP_PORT:-18085}"

WBASE="localhost:${WRITER_HTTP_PORT}"
VBASE="localhost:${KAFKA_VALIDATOR_HTTP_PORT}"
M1="localhost:${CDC_OUTBOX_1_HTTP_PORT}"

if [ -t 1 ]; then
  GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; CYAN=$'\033[36m'; NC=$'\033[0m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; NC=''
fi

PASS=0; FAIL=0
ok()  { PASS=$((PASS + 1)); echo "  ${GREEN}✔${NC} $1"; }
bad() { FAIL=$((FAIL + 1)); echo "  ${RED}✘${NC} $1"; }
info() { echo "  ${YELLOW}--${NC} $1"; }
section() { echo; echo "${CYAN}==> ${1}${NC}"; }
finish() {
  echo
  if [ "${FAIL}" -eq 0 ]; then
    echo "${GREEN}ИТОГ: PASS=${PASS} FAIL=0${NC}"
  else
    echo "${RED}ИТОГ: PASS=${PASS} FAIL=${FAIL}${NC}"
    exit 1
  fi
}

# ---------- помощники (паттерн scripts/stand.sh / integration-delivery.sh) ----------

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

jfield() { # $1=файл JSON, $2=python-выражение над d
  python3 -c "import json; d=json.load(open('$1')); print($2)" 2>/dev/null || true
}

writer_start() { # $1=JSON спеки; stdout: runId (пусто при отказе)
  local code
  code=$(curl -s -m 10 -o /tmp/upgr-start.json -w '%{http_code}' \
    -X POST "${WBASE}/api/generation/start" -H 'Content-Type: application/json' -d "$1" 2>/dev/null || true)
  case "${code}" in
    2*) jfield /tmp/upgr-start.json 'd["runId"]' ;;
    *) echo ""; echo "старт прогона отклонён (HTTP ${code})" >&2 ;;
  esac
}

wait_writer_finished() { # $1=бюджет сек
  local i st
  st="?"
  for i in $(seq 1 $(( $1 / 2 ))); do
    st=$(curl -s -m 5 "${WBASE}/api/generation/state" 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || echo '?')
    case "${st}" in
      running|idle) sleep 2 ;;
      *) return 0 ;;
    esac
  done
  return 1
}

writer_results() { curl -s -m 10 "${WBASE}/api/generation/results" -o /tmp/upgr-results.json 2>/dev/null; }
wr() { jfield /tmp/upgr-results.json "$1"; }

wait_vready() { # $1=бюджет сек
  local i code
  for i in $(seq 1 $(( $1 / 2 ))); do
    code=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "${VBASE}/ready" 2>/dev/null || true)
    [ "${code}" = "200" ] && return 0
    sleep 2
  done
  return 1
}

wait_run_unique() { # $1=runId $2=ожидаемое $3=бюджет сек; stdout — последнее значение
  local i u=""
  for i in $(seq 1 $(( $3 / 3 ))); do
    curl -s -m 5 -o /tmp/upgr-run.json "${VBASE}/api/validation/runs/$1" 2>/dev/null || true
    u=$(jfield /tmp/upgr-run.json 'd["uniqueIds"]')
    [ "${u:-}" = "$2" ] && { printf '%s' "${u}"; return 0; }
    sleep 3
  done
  printf '%s' "${u:-}"
  return 1
}

validate_run() { # $1=runId; stdout: HTTP-код; тело -> /tmp/upgr-verdict.json
  curl -s -m 60 -o /tmp/upgr-verdict.json -w '%{http_code}' \
    -X POST "${VBASE}/api/validation/validate" \
    -H 'Content-Type: application/json' \
    -d "{\"runId\": \"$1\", \"fetchFromWriter\": true}" 2>/dev/null || true
}

vd() { jfield /tmp/upgr-verdict.json "$1"; }

assert_verdict_strict() { # $1=метка $2=written — чистый прогон: дубликатов 0
  local label="$1" written="$2" v u dup
  v=$(vd 'd["verdict"]')
  if [ "${v}" = "pass" ]; then ok "${label}: verdict=pass"; else bad "${label}: verdict='${v:-<нет>}'"; fi
  [ "$(vd 'd["missingWrittenIdsCount"]')" = "0" ] && ok "${label}: missing=0" || bad "${label}: missing=$(vd 'd["missingWrittenIdsCount"]')"
  [ "$(vd 'd["unexpectedExtraIdsCount"]')" = "0" ] && ok "${label}: unexpected=0" || bad "${label}: unexpected=$(vd 'd["unexpectedExtraIdsCount"]')"
  u=$(vd 'd["kafka"]["uniqueIds"]')
  [ "${u:-}" = "${written}" ] && ok "${label}: unique=${u}==written" || bad "${label}: unique='${u:-<нет>}' != written=${written}"
  dup=$(vd 'd["kafka"]["duplicateRecords"]')
  [ "${dup:-0}" = "0" ] && ok "${label}: дубликатов 0" || bad "${label}: дубликатов ${dup}"
}

metrics_value() { # $1=имя метрики; stdout: значение (cdc-outbox-1)
  curl -s -m 5 "${M1}/metrics" 2>/dev/null \
    | grep -F "$1{slot=\"outbox_slot\"}" | awk '{print $2}'
}

wait_active1() { # $1=бюджет сек $2=метка
  local i a=""
  for i in $(seq 1 $(( $1 / 2 ))); do
    a=$(metrics_value 'outbox_processor_is_active')
    [ "${a}" = "1" ] && { ok "$2: cdc-outbox-1 is_active=1"; return 0; }
    sleep 2
  done
  bad "$2: cdc-outbox-1 is_active='${a:-?}'"
  return 1
}

outbox2_running() {
  docker compose --profile redundancy ps -a --format '{{.Status}}' cdc-outbox-2 2>/dev/null | grep -q '^Up'
}

# =========================================================================
section "T0. Префлайт и базовый прогон (состояние «до обновления»)"

if curl -s -m 5 "${WBASE}/api/generation/state" 2>/dev/null \
  | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["state"] in ("running","stopping") else 1)' 2>/dev/null; then
  bad "T0: Writer сейчас выполняет прогон — дождись завершения и перезапусти сценарий"
  finish
fi

PAIR_BEFORE=0
if outbox2_running; then
  PAIR_BEFORE=1
  info "пара Active-Passive поднята — cdc-outbox-2 останавливаю на время сценария (репетируется одиночный инстанс)"
  docker compose --profile redundancy stop cdc-outbox-2 >/dev/null 2>&1 || true
  sleep 20
fi

for s in pg1 pg2 pg3 haproxy kafka1 kafka2 kafka3 writer kafka-validator cdc-outbox-1; do
  st=$(docker compose ps -a --format '{{.Status}}' "${s}" 2>/dev/null || true)
  if echo "${st}" | grep -q '^Up.*healthy'; then ok "${s}: healthy"; else bad "${s}: ${st:-<нет контейнера>}"; fi
done
wait_active1 60 "T0"

RUN0=$(writer_start '{"totalMessages": 200, "messageSizeBytes": 256}')
if [ -n "${RUN0}" ]; then
  ok "T0: базовый прогон стартовал (${RUN0})"
else
  bad "T0: базовый прогон не стартовал"
  finish
fi
wait_writer_finished 600
writer_results
[ "$(wr 'd["state"]')" = "completed" ] && ok "T0: state=completed" || bad "T0: state=$(wr 'd["state"]')"
[ "$(wr 'd["writtenMessages"]')" = "200" ] && ok "T0: written=200" || bad "T0: written=$(wr 'd["writtenMessages"]')"
u=$(wait_run_unique "${RUN0}" 200 180)
[ "${u}" = "200" ] && ok "T0: доставлено 200 уникальных" || bad "T0: unique='${u:-?}' != 200"
code=$(validate_run "${RUN0}")
[ "${code}" = "200" ] && assert_verdict_strict "T0" 200 || bad "T0: validate HTTP ${code}"
# Снимок счётчиков базового прогона: после recovery старая позиция не пере-доставляется
curl -s -m 5 -o /tmp/upgr-run0.json "${VBASE}/api/validation/runs/${RUN0}" 2>/dev/null || true
RUN0_RECORDS=$(jfield /tmp/upgr-run0.json 'd["records"]')

# =========================================================================
section "T1. Тишина: idle-предупреждение и нулевой checkpoint lag"

WARN0=$(docker compose logs cdc-outbox-1 2>/dev/null | grep -cF 'Простой потока' || true)
i=0
warn_seen=0
while [ "${i}" -lt 24 ]; do
  WARN1=$(docker compose logs cdc-outbox-1 2>/dev/null | grep -cF 'Простой потока' || true)
  if [ "${WARN1:-0}" -gt "${WARN0:-0}" ]; then warn_seen=1; break; fi
  sleep 5
  i=$((i + 1))
done
if [ "${warn_seen}" = "1" ]; then
  ok "T1: idle-предупреждение появилось (порог 30с, каденс интервала)"
else
  bad "T1: idle-предупреждение не появилось за 120с"
fi

lag=""
i=0
while [ "${i}" -lt 24 ]; do
  lag=$(metrics_value 'outbox_checkpoint_lag_bytes')
  if python3 -c "import sys; sys.exit(0 if float('${lag:-nan}') == 0 else 1)" 2>/dev/null; then
    break
  fi
  sleep 5
  i=$((i + 1))
done
if python3 -c "import sys; sys.exit(0 if float('${lag:-nan}') == 0 else 1)" 2>/dev/null; then
  ok "T1: outbox_checkpoint_lag_bytes=0 — всё доставленное подтверждено"
else
  bad "T1: checkpoint lag='${lag:-?}' (не 0)"
fi

# =========================================================================
section "T2. Остановка cdc-outbox-1 (graceful: финальная checkpoint-транзакция)"

if docker compose stop -t 40 cdc-outbox-1 >/dev/null 2>&1; then
  ok "T2: cdc-outbox-1 остановлен"
else
  bad "T2: stop cdc-outbox-1 не удался"
fi

# =========================================================================
section "T3. Dump БД outbox (схема+данные; таблица пустая — дамп маленький)"

DUMP="/tmp/outbox-upgrade-$(date +%s).sql"
if docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD}" pg1 \
    pg_dump -h haproxy -U postgres -d outbox > "${DUMP}" 2>/tmp/upgr-dump.err; then
  ok "T3: pg_dump выполнен (${DUMP}, $(wc -c < "${DUMP}" | tr -d ' ') байт)"
else
  bad "T3: pg_dump упал: $(head -3 /tmp/upgr-dump.err)"
fi
if grep -qF 'CREATE TABLE outbox.messages' "${DUMP}"; then
  ok "T3: дамп содержит таблицу outbox.messages"
else
  bad "T3: в дампе нет CREATE TABLE outbox.messages"
fi
if grep -qF 'CREATE PUBLICATION' "${DUMP}"; then
  ok "T3: дамп содержит CREATE PUBLICATION (pg_dump 17 выгружает публикации)"
else
  bad "T3: в дампе нет CREATE PUBLICATION — вернёт apply-bootstrap"
fi

# =========================================================================
section "T4. «Новый кластер»: DROP/CREATE DATABASE + restore + bootstrap"

if psql_primary -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE outbox WITH (FORCE)" \
    -c "CREATE DATABASE outbox"; then
  ok "T4: БД outbox пересоздана (слот исчез вместе со старой БД)"
else
  bad "T4: DROP/CREATE DATABASE не удался"
fi
slot_gone=$(db_value "SELECT count(*) FROM pg_replication_slots WHERE slot_name='outbox_slot'")
[ "${slot_gone:-1}" = "0" ] && ok "T4: слот outbox_slot отсутствует" || bad "T4: слот на месте (${slot_gone:-?} строк)"

if docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD}" pg1 \
    psql -h haproxy -U postgres -d outbox -v ON_ERROR_STOP=1 -f - < "${DUMP}" > /tmp/upgr-restore.log 2>&1; then
  ok "T4: дамп восстановлен в новую БД"
else
  bad "T4: restore упал: $(tail -3 /tmp/upgr-restore.log)"
fi

# CONNECT-гранты ролей в plain pg_dump не входят — их возвращает bootstrap
if ./scripts/init/apply-bootstrap.sh > /tmp/upgr-bootstrap.log 2>&1; then
  ok "T4: apply-bootstrap выполнен (гранты/publication/sequence — идемпотентно)"
else
  bad "T4: apply-bootstrap упал: $(tail -3 /tmp/upgr-bootstrap.log)"
fi

[ "$(db_value "SELECT has_database_privilege('sys_writer','outbox','CONNECT')")" = "t" ] \
  && ok "T4: sys_writer CONNECT (вернул bootstrap)" || bad "T4: sys_writer без CONNECT"
[ "$(db_value "SELECT has_database_privilege('sys_replication','outbox','CONNECT')")" = "t" ] \
  && ok "T4: sys_replication CONNECT" || bad "T4: sys_replication без CONNECT"
[ "$(db_value "SELECT has_database_privilege('sys_monitor','outbox','CONNECT')")" = "t" ] \
  && ok "T4: sys_monitor CONNECT" || bad "T4: sys_monitor без CONNECT"
[ "$(db_value "SELECT count(*) FROM pg_publication WHERE pubname='outbox_publication' AND pubinsert")" = "1" ] \
  && ok "T4: publication outbox_publication (publish='insert')" || bad "T4: publication отсутствует/неверна"
[ "$(db_value "SELECT count(*) FROM pg_publication_tables WHERE pubname='outbox_publication' AND schemaname='outbox' AND tablename='messages'")" = "1" ] \
  && ok "T4: publication содержит outbox.messages" || bad "T4: publication без таблицы"
[ "$(db_value "SELECT to_regclass('outbox.messages') IS NOT NULL")" = "t" ] \
  && ok "T4: таблица outbox.messages восстановлена" || bad "T4: таблицы нет"

# =========================================================================
section "T5. Recovery-старт (CDCOUTBOX__RECOVERYMODE=true на один запуск)"

if CDCOUTBOX__RECOVERYMODE=true docker compose up -d --no-deps cdc-outbox-1 >/dev/null 2>&1; then
  ok "T5: cdc-outbox-1 запущен с recovery_mode"
else
  bad "T5: запуск с recovery_mode не удался"
fi
i=0
while [ "${i}" -lt 40 ]; do
  st=$(docker compose ps -a --format '{{.Status}}' cdc-outbox-1 2>/dev/null || true)
  echo "${st}" | grep -q '^Up.*healthy' && break
  sleep 3
  i=$((i + 1))
done
st=$(docker compose ps -a --format '{{.Status}}' cdc-outbox-1 2>/dev/null || true)
if echo "${st}" | grep -q '^Up.*healthy'; then ok "T5: контейнер healthy"; else bad "T5: контейнер '${st}'"; fi

wait_active1 120 "T5"
rec=$(metrics_value 'outbox_recovery_mode')
[ "${rec}" = "1" ] && ok "T5: outbox_recovery_mode=1" || bad "T5: outbox_recovery_mode='${rec:-?}'"
slot_rec=$(db_value "SELECT count(*) FROM pg_replication_slots WHERE slot_name='outbox_slot' AND plugin='pgoutput' AND failover")
[ "${slot_rec}" = "1" ] && ok "T5: слот пересоздан (pgoutput, failover=true)" || bad "T5: слот не пересоздан (${slot_rec:-?})"

# Логи recovery (старая/новая позиция) — показать, не ассертить (формат строк — реализации)
docker compose logs --since 5m cdc-outbox-1 2>/dev/null | grep -iE 'recover|восстанов' | tail -5 | sed 's/^/    /' || true

# Прогон доставки при live-конфликте: отправляем сообщение, ждём доставки
RUNX=$(writer_start '{"totalMessages": 50, "messageSizeBytes": 256}')
if [ -n "${RUNX}" ]; then
  wait_writer_finished 300
  writer_results
  [ "$(wr 'd["writtenMessages"]')" = "50" ] && ok "T5: после recovery запись работает (50 записано)" || bad "T5: written=$(wr 'd["writtenMessages"]')"
else
  bad "T5: контрольный прогон записи не стартовал"
fi

# Обязательство: recovery НЕ пере-доставляет старое (позиция слота — текущая)
sleep 10
curl -s -m 5 -o /tmp/upgr-run0b.json "${VBASE}/api/validation/runs/${RUN0}" 2>/dev/null || true
RUN0_RECORDS_AFTER=$(jfield /tmp/upgr-run0b.json 'd["records"]')
if [ "${RUN0_RECORDS_AFTER:-x}" = "${RUN0_RECORDS:-y}" ]; then
  ok "T5: базовый прогон НЕ пере-доставлен (records=${RUN0_RECORDS:-?} неизменен)"
else
  bad "T5: записи базового прогона изменились (${RUN0_RECORDS:-?} -> ${RUN0_RECORDS_AFTER:-?})"
fi

# =========================================================================
section "T6. Свежий прогон после «обновления»: полный цикл доставки"

u=$(wait_run_unique "${RUNX:-}" 50 180)
[ "${u}" = "50" ] && ok "T6: контрольный прогон доставлен (50 уникальных)" || bad "T6: unique='${u:-?}' != 50"
code=$(validate_run "${RUNX:-}")
[ "${code}" = "200" ] && assert_verdict_strict "T6(контрольный)" 50 || bad "T6: validate HTTP ${code}"
# Повторный validate базового прогона НЕ делается: fetchFromWriter сверяется только с последним
# прогоном Writer (здесь — контрольным 50), для старого runId валидатор отвечает HTTP 409.
# Инвариант «базовый прогон не пере-доставлен» закрыт проверкой records в T5.

# =========================================================================
section "T7. Возврат к дефолту (без recovery-флага) + полный health-check"

docker compose stop -t 40 cdc-outbox-1 >/dev/null 2>&1 || true
if docker compose up -d --no-deps cdc-outbox-1 >/dev/null 2>&1; then
  ok "T7: cdc-outbox-1 перезапущен без recovery-флага"
else
  bad "T7: перезапуск не удался"
fi
wait_active1 120 "T7"
rec=$(metrics_value 'outbox_recovery_mode')
[ "${rec}" = "0" ] && ok "T7: outbox_recovery_mode=0 (дефолт)" || bad "T7: outbox_recovery_mode='${rec:-?}'"

if [ "${PAIR_BEFORE}" = "1" ]; then
  info "возвращаю cdc-outbox-2 (пара была поднята до сценария)"
  docker compose --profile redundancy up -d --no-deps cdc-outbox-2 >/dev/null 2>&1 || true
  i=0
  while [ "${i}" -lt 40 ]; do
    st=$(docker compose --profile redundancy ps -a --format '{{.Status}}' cdc-outbox-2 2>/dev/null || true)
    echo "${st}" | grep -q '^Up.*healthy' && break
    sleep 3
    i=$((i + 1))
  done
  a2=""
  i=0
  while [ "${i}" -lt 30 ]; do
    a2=$(curl -s -m 5 "localhost:${CDC_OUTBOX_2_HTTP_PORT}/metrics" 2>/dev/null \
      | grep -F 'outbox_processor_is_active{slot="outbox_slot"}' | awk '{print $2}')
    [ -n "${a2}" ] && break
    sleep 2
    i=$((i + 1))
  done
  a1=$(metrics_value 'outbox_processor_is_active')
  if [ -n "${a1}" ] && [ -n "${a2}" ] && [ $(( a1 + a2 )) -eq 1 ]; then
    ok "T7: пара восстановлена, активен ровно один (1=${a1}, 2=${a2})"
  else
    bad "T7: пара: cdc-outbox-1='${a1:-?}' cdc-outbox-2='${a2:-?}'"
  fi
fi

if ./scripts/stand.sh check > /tmp/upgr-check.log 2>&1; then
  ok "T7: stand.sh check: $(grep -o 'ИТОГ: [0-9]*/[0-9]* ok' /tmp/upgr-check.log | tail -1)"
else
  bad "T7: stand.sh check провален:"
  tail -30 /tmp/upgr-check.log | sed 's/^/    /'
fi

finish
