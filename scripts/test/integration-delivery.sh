#!/usr/bin/env bash
# integration-delivery.sh — приёмочный харнесс доставки (задача 11,
# docs/implementation-plan.md). Живой end-to-end прогон конвейера
# outbox -> Kafka: запись -> вердикт -> рестарт-продолжение ->
# отказ-восстановление (PG failover, отказ одного брокера, полный outage
# Kafka; при поднятой паре cdc-outbox-2 — failover Active-Passive) ->
# полный health-check.
#
# Использование: scripts/test/integration-delivery.sh
# Предусловия: стенд поднят (scripts/stand.sh up), cdc-outbox-1 активен.
# Все инъекции восстанавливаются — стенд остаётся рабочим. Код возврата 0 —
# все проверки зелёные. Инъекции и восстановление делегированы scripts/stand.sh
# (общая хореография: fail сам делает выборы+bootstrap+settle, restore —
# ожидание healthy). Вердикт: pass ⇔ missing=0 && unexpected=0, дубликаты
# не фейлят (at-least-once).
set -uo pipefail
cd "$(dirname "$0")/../.."

# .env — порты и пароли (в git не попадает; дефолты как в .env.example)
if [ -f .env ]; then . ./.env; fi
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-18080}"
KAFKA_VALIDATOR_HTTP_PORT="${KAFKA_VALIDATOR_HTTP_PORT:-18083}"
CDC_OUTBOX_1_HTTP_PORT="${CDC_OUTBOX_1_HTTP_PORT:-18084}"
CDC_OUTBOX_2_HTTP_PORT="${CDC_OUTBOX_2_HTTP_PORT:-18085}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
WBASE="localhost:${WRITER_HTTP_PORT}"
VBASE="localhost:${KAFKA_VALIDATOR_HTTP_PORT}"
M1="localhost:${CDC_OUTBOX_1_HTTP_PORT}"
M2="localhost:${CDC_OUTBOX_2_HTTP_PORT}"

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

# ---------- общие помощники (паттерн scripts/stand.sh) ----------

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

cluster_leader() {
  patroni_cluster | python3 -c '
import json, sys
ms = json.load(sys.stdin)["members"]
print("".join(m["name"] for m in ms if m["role"] == "leader"))' 2>/dev/null || true
}

# ---------- помощники: Writer / валидатор ----------

writer_start() { # $1=JSON спеки; stdout: runId (пусто при отказе)
  local code
  code=$(curl -s -m 10 -o /tmp/intd-start.json -w '%{http_code}' \
    -X POST "${WBASE}/api/generation/start" -H 'Content-Type: application/json' -d "$1" 2>/dev/null || true)
  case "${code}" in
    2*) jfield /tmp/intd-start.json 'd["runId"]' ;;
    *) echo ""; echo "старт прогона отклонён (HTTP ${code}: $(head -c 200 /tmp/intd-start.json 2>/dev/null))" >&2 ;;
  esac
}

jfield() { # $1=файл JSON, $2=python-выражение над d
  python3 -c "import json; d=json.load(open('$1')); print($2)" 2>/dev/null || true
}

wait_writer_finished() { # $1=бюджет сек; 0 — прогон завершился
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

writer_results() { curl -s -m 10 "${WBASE}/api/generation/results" -o /tmp/intd-results.json 2>/dev/null; }
wr() { jfield /tmp/intd-results.json "$1"; }

wait_vready() { # $1=бюджет сек
  local i code
  for i in $(seq 1 $(( $1 / 2 ))); do
    code=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "${VBASE}/ready" 2>/dev/null || true)
    [ "${code}" = "200" ] && return 0
    sleep 2
  done
  return 1
}

wait_run_unique() { # $1=runId $2=ожидаемое unique $3=бюджет сек; stdout — последнее значение
  local i u=""
  for i in $(seq 1 $(( $3 / 3 ))); do
    curl -s -m 5 -o /tmp/intd-run.json "${VBASE}/api/validation/runs/$1" 2>/dev/null || true
    u=$(jfield /tmp/intd-run.json 'd["uniqueIds"]')
    [ "${u:-}" = "$2" ] && { printf '%s' "${u}"; return 0; }
    sleep 3
  done
  printf '%s' "${u:-}"
  return 1
}

validate_run() { # $1=runId $2=fetchFromWriter(true|false); stdout: HTTP-код; тело -> /tmp/intd-verdict.json
  curl -s -m 60 -o /tmp/intd-verdict.json -w '%{http_code}' \
    -X POST "${VBASE}/api/validation/validate" \
    -H 'Content-Type: application/json' \
    -d "{\"runId\": \"$1\", \"fetchFromWriter\": $2}" 2>/dev/null || true
}

vd() { jfield /tmp/intd-verdict.json "$1"; }

# Вердикт: pass + нет доказанных потерь/аномалий + unique==written.
# $1=метка $2=ожидаемое written; дубликаты репортятся, не фейлят (at-least-once).
assert_verdict() {
  local label="$1" written="$2" v u dup
  v=$(vd 'd["verdict"]')
  if [ "${v}" = "pass" ]; then ok "${label}: verdict=pass"; else bad "${label}: verdict='${v:-<нет>}'"; fi
  if [ "$(vd 'd["missingWrittenIdsCount"]')" = "0" ]; then ok "${label}: missing=0"; else bad "${label}: missing=$(vd 'd["missingWrittenIdsCount"]')"; fi
  if [ "$(vd 'd["unexpectedExtraIdsCount"]')" = "0" ]; then ok "${label}: unexpected=0"; else bad "${label}: unexpected=$(vd 'd["unexpectedExtraIdsCount"]')"; fi
  u=$(vd 'd["kafka"]["uniqueIds"]')
  if [ "${u:-}" = "${written}" ]; then ok "${label}: unique=${u}==written"; else bad "${label}: unique='${u:-<нет>}' != written=${written}"; fi
  dup=$(vd 'd["kafka"]["duplicateRecords"]')
  if [ "${dup:-0}" = "0" ]; then
    ok "${label}: дубликатов 0"
  else
    ok "${label}: дубликатов ${dup} (допустимо, at-least-once — окно неподтверждённого)"
  fi
}

# Прогон Writer до завершения + базовые ассерты итогов; результат — runId
# в глобальной RID_OUT (не stdout: печать и захват "$(…)" несовместимы).
# $1=спека $2=метка $3=ожидаемое written
run_and_finish() {
  local spec="$1" label="$2" total="$3" st wr_f
  RID_OUT=$(writer_start "${spec}")
  if [ -z "${RID_OUT}" ]; then bad "${label}: старт прогона"; return 0; fi
  ok "${label}: прогон стартовал (${RID_OUT})"
  if wait_writer_finished 600; then
    ok "${label}: прогон завершился"
  else
    bad "${label}: прогон не завершился за 600с"
  fi
  writer_results
  st=$(wr 'd["state"]')
  if [ "${st}" = "completed" ]; then ok "${label}: state=completed"; else bad "${label}: state='${st}' (stopReason=$(wr 'd["stopReason"]'))"; fi
  wr_f=$(wr 'd["writtenMessages"]')
  if [ "${wr_f:-}" = "${total}" ]; then ok "${label}: written=${wr_f}"; else bad "${label}: written='${wr_f:-<нет>}' != ${total}"; fi
  if [ "$(wr 'd["failedMessages"]')" = "0" ]; then ok "${label}: failed=0"; else bad "${label}: failed=$(wr 'd["failedMessages"]')"; fi
}

# ---------- помощники: состояние cdc-outbox ----------

metrics_value() { # $1=base $2=имя метрики; stdout: значение
  curl -s -m 5 "$1/metrics" 2>/dev/null \
    | grep -F "$2{slot=\"outbox_slot\"}" | awk '{print $2}'
}

is_active1() { metrics_value "${M1}" 'outbox_processor_is_active'; }
is_active2() { metrics_value "${M2}" 'outbox_processor_is_active'; }
failovers1()  { metrics_value "${M1}" 'outbox_failovers_total'; }
failovers2()  { metrics_value "${M2}" 'outbox_failovers_total'; }

# Метрика экземпляра по stand.sh-имени (outbox-1|outbox-2).
is_active_of() { if [ "$1" = "outbox-1" ]; then is_active1; else is_active2; fi; }
failovers_of() { if [ "$1" = "outbox-1" ]; then failovers1; else failovers2; fi; }

outbox2_running() {
  docker compose --profile redundancy ps -a --format '{{.Status}}' cdc-outbox-2 2>/dev/null | grep -q '^Up'
}

# Ждёт активного лидера: одиночный режим — is_active=1 у cdc-outbox-1;
# пара — метрики обоих присутствуют и активен ровно один.
# $1=бюджет сек $2=метка
wait_leader_active() {
  local i a1 a2
  for i in $(seq 1 $(( $1 / 2 ))); do
    a1=$(is_active1)
    if [ "${PAIR}" = "1" ]; then
      a2=$(is_active2)
      if [ -n "${a1}" ] && [ -n "${a2}" ] && [ $(( a1 + a2 )) -eq 1 ]; then
        ok "$2: активен ровно один (cdc-outbox-1=${a1}, cdc-outbox-2=${a2})"
        return 0
      fi
    else
      if [ "${a1}" = "1" ]; then
        ok "$2: cdc-outbox-1 is_active=1"
        return 0
      fi
    fi
    sleep 2
  done
  bad "$2: лидерство не восстановилось за ${1}с (cdc-outbox-1='${a1:-?}', cdc-outbox-2='${a2:-?}')"
  return 1
}

num_ge() { python3 -c "import sys; sys.exit(0 if float('$1') >= float('$2') else 1)"; }
num_gt() { python3 -c "import sys; sys.exit(0 if float('$1') >  float('$2') else 1)"; }

# =========================================================================
section "T0. Префлайт"

PAIR=0
if outbox2_running; then
  PAIR=1
  info "пара Active-Passive поднята (cdc-outbox-2 running) — фаза T5 включена"
fi

for s in pg1 pg2 pg3 haproxy kafka1 kafka2 kafka3 writer kafka-validator cdc-outbox-1; do
  st=$(docker compose ps -a --format '{{.Status}}' "${s}" 2>/dev/null || true)
  if echo "${st}" | grep -q '^Up.*healthy'; then ok "${s}: healthy"; else bad "${s}: ${st:-<нет контейнера>}"; fi
done

a1=$(is_active1)
if [ "${PAIR}" = "1" ]; then
  a2=$(is_active2)
  if [ -n "${a1}" ] && [ -n "${a2}" ] && [ $(( a1 + a2 )) -eq 1 ]; then
    ok "активен ровно один экземпляр (cdc-outbox-1=${a1}, cdc-outbox-2=${a2})"
  else
    bad "активность пары: cdc-outbox-1='${a1:-?}' cdc-outbox-2='${a2:-?}'"
  fi
else
  [ "${a1}" = "1" ] && ok "cdc-outbox-1 is_active=1 (лидер)" || bad "cdc-outbox-1 is_active='${a1:-<нет метрики>}'"
fi

slot_active=$(db_value "SELECT active_pid IS NOT NULL FROM pg_replication_slots WHERE slot_name='outbox_slot'")
[ "${slot_active}" = "t" ] && ok "слот outbox_slot активен (active_pid)" || bad "слот outbox_slot: active_pid IS NOT NULL='${slot_active:-<отказ>}'"

if wait_vready 90; then ok "валидатор /ready"; else bad "валидатор /ready не 200"; fi
vstate=$(curl -s -m 5 "${VBASE}/api/validation/state" 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || true)
[ "${vstate}" = "running" ] && ok "валидатор state=running" || bad "валидатор state='${vstate:-?}'"

# =========================================================================
section "T1. Запись: INSERT+DELETE в одной транзакции, таблица всегда пустая"

run_and_finish '{"totalMessages": 300, "messageSizeBytes": 256}' "T1" 300
RUN1="${RID_OUT}"
[ "$(wr 'd["ambiguousUnresolved"]')" = "0" ] && ok "T1: ambiguousUnresolved=0" || bad "T1: ambiguousUnresolved=$(wr 'd["ambiguousUnresolved"]')"
table_rows=$(db_value "SELECT count(*) FROM outbox.messages")
[ "${table_rows}" = "0" ] && ok "T1: таблица outbox.messages пустая (${table_rows} строк)" || bad "T1: в таблице ${table_rows} строк"

# =========================================================================
section "T2. Вердикт чистого прогона (500-уникальных, без отказов)"

if [ -n "${RUN1}" ]; then
  u=$(wait_run_unique "${RUN1}" 300 180)
  [ "${u}" = "300" ] && ok "T2: доставлено 300 уникальных" || bad "T2: unique='${u:-?}' != 300"
  code=$(validate_run "${RUN1}" true)
  if [ "${code}" = "200" ]; then
    if [ "$(vd 'd["expected"]["source"]')" = "writer" ] && [ "$(vd 'd["expected"]["writtenCount"]')" = "300" ]; then
      ok "T2: ожидаемый набор из Writer (writtenCount=300)"
    else
      bad "T2: expected: source=$(vd 'd["expected"]["source"]') writtenCount=$(vd 'd["expected"]["writtenCount"]')"
    fi
    assert_verdict "T2" 300
  else
    bad "T2: validate HTTP ${code}"
  fi
else
  bad "T2: пропущен — прогон T1 не стартовал"
fi

# =========================================================================
section "T2b. Контракт headers: значения — текст (UTF-8), живая проба"

HDR_GUID=$(uuidgen | tr 'A-Z' 'a-z')
HDR_MARKER="hdr-probe-$(date +%s)"
HDR_ID=$(db_value "SELECT nextval('outbox.messages_id_seq')")
if [ -n "${HDR_ID}" ]; then
  key_hex=$(printf '%s' "${HDR_ID}" | od -An -tx1 | tr -d ' \n')
  body_json='{"id":'"${HDR_ID}"',"runId":"'"${HDR_GUID}"'","eventType":"probe","payload":"headers-format-probe","timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'"}'
  body_hex=$(printf '%s' "${body_json}" | od -An -tx1 | tr -d ' \n')
  headers_json='{"trace-id":"'"${HDR_MARKER}"'","content-type":"application/json"}'
  if psql_primary -d outbox -v ON_ERROR_STOP=1 <<SQL
BEGIN;
INSERT INTO outbox.messages (id, topic, key, body, headers, created_at)
OVERRIDING SYSTEM VALUE
VALUES (${HDR_ID}, 'outbox.events', '\x${key_hex}', '\x${body_hex}', '${headers_json}', now());
DELETE FROM outbox.messages WHERE id = ${HDR_ID};
COMMIT;
SQL
  then
    ok "T2b: probe-сообщение вставлено и удалено одной транзакцией (id=${HDR_ID})"
  else
    bad "T2b: вставка probe-сообщения не удалась"
  fi
  table_rows=$(db_value "SELECT count(*) FROM outbox.messages")
  [ "${table_rows}" = "0" ] && ok "T2b: таблица по-прежнему пустая" || bad "T2b: в таблице ${table_rows} строк"

  # Ждём, пока валидатор увидит runId пробы (доставка до Kafka свершилась)
  u=$(wait_run_unique "${HDR_GUID}" 1 60)
  [ "${u}" = "1" ] && ok "T2b: probe доставлен (валидатор видит run)" || bad "T2b: probe не доставлен (unique='${u:-?}')"

  # Читаем заголовки записи напрямую из topic: значения должны быть строками
  docker compose exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka1:9092 --topic outbox.events --from-beginning \
    --timeout-ms 20000 --property print.headers=true \
    > /tmp/intd-headers.log 2>/dev/null || true
  hline=$(grep -F "trace-id:${HDR_MARKER}" /tmp/intd-headers.log | head -1)
  if [ -n "${hline}" ]; then
    ok "T2b: заголовок trace-id:${HDR_MARKER} виден в topic текстом (UTF-8)"
    case "${hline}" in
      *"content-type:application/json"*) ok "T2b: заголовок content-type:application/json — второй заголовок записи" ;;
      *) bad "T2b: content-type в заголовках записи не найден (${hline})" ;;
    esac
    case "${hline}" in
      *$'\t'"${HDR_ID}"$'\t'*) ok "T2b: запись найдена по ключу id=${HDR_ID} (key=UTF-8 десятичного id)" ;;
      *) info "T2b: ключ записи — см. вывод (ожидается ${HDR_ID})" ;;
    esac
  else
    bad "T2b: заголовок trace-id:${HDR_MARKER} не найден в topic"
  fi
else
  bad "T2b: не удалось зарезервировать id (nextval)"
fi

# =========================================================================
section "T3. Рестарт cdc-outbox-1 в середине прогона: продолжение от confirmed_flush"

RUN3=$(writer_start '{"totalMessages": 600, "messageSizeBytes": 256, "ratePerSecond": 30}')
if [ -n "${RUN3}" ]; then
  ok "T3: прогон стартовал (${RUN3}; ~20с при 30/с, рестарт через 8с)"
  sleep 8
  if docker compose restart cdc-outbox-1 >/dev/null 2>&1; then
    ok "T3: cdc-outbox-1 перезапущен"
  else
    bad "T3: рестарт cdc-outbox-1 не удался"
  fi
  wait_leader_active 120 "T3(после рестарта)"
  if wait_writer_finished 600; then ok "T3: прогон завершился"; else bad "T3: прогон не завершился за 600с"; fi
  writer_results
  [ "$(wr 'd["state"]')" = "completed" ] && ok "T3: state=completed" || bad "T3: state=$(wr 'd["state"]')"
  [ "$(wr 'd["writtenMessages"]')" = "600" ] && ok "T3: written=600" || bad "T3: written=$(wr 'd["writtenMessages"]')"
  u=$(wait_run_unique "${RUN3}" 600 270)
  [ "${u}" = "600" ] && ok "T3: доставлено 600 уникальных" || bad "T3: unique='${u:-?}' != 600"
  code=$(validate_run "${RUN3}" true)
  [ "${code}" = "200" ] && assert_verdict "T3" 600 || bad "T3: validate HTTP ${code}"
else
  bad "T3: прогон не стартовал"
fi

# =========================================================================
section "T4a. Отказ PG: kill лидера Patroni под нагрузкой"

LEADER_BEFORE=$(cluster_leader)
if [ -n "${LEADER_BEFORE}" ]; then
  ok "T4a: текущий лидер ${LEADER_BEFORE}"
else
  bad "T4a: лидер не определён — фаза пропущена"
fi
if [ -n "${LEADER_BEFORE}" ]; then
  RUN4A=$(writer_start '{"totalMessages": 600, "messageSizeBytes": 256, "ratePerSecond": 30}')
  if [ -n "${RUN4A}" ]; then
    ok "T4a: прогон стартовал (${RUN4A})"
    sleep 8
    if ./scripts/stand.sh fail pg 2>&1 | sed 's/^/    /'; then
      ok "T4a: fail pg отработал (выборы + bootstrap + settle)"
    else
      bad "T4a: fail pg завершился ошибкой"
    fi
    if wait_writer_finished 600; then ok "T4a: прогон завершился (Writer ретраит через failover)"; else bad "T4a: прогон не завершился"; fi
    writer_results
    [ "$(wr 'd["state"]')" = "completed" ] && ok "T4a: state=completed" || bad "T4a: state=$(wr 'd["state"]')"
    wr4a=$(wr 'd["writtenMessages"]')
    u=$(wait_run_unique "${RUN4A}" "${wr4a:-600}" 270)
    [ "${u}" = "${wr4a:-600}" ] && ok "T4a: доставлено всё написанное (${u})" || bad "T4a: unique='${u:-?}' != ${wr4a:-600}"
    wait_leader_active 180 "T4a(доставка после failover)"
    code=$(validate_run "${RUN4A}" true)
    [ "${code}" = "200" ] && assert_verdict "T4a" "${wr4a:-600}" || bad "T4a: validate HTTP ${code}"
    if ./scripts/stand.sh restore "${LEADER_BEFORE}" 2>&1 | sed 's/^/    /'; then
      ok "T4a: ${LEADER_BEFORE} возвращён (standby)"
    else
      bad "T4a: restore ${LEADER_BEFORE} завершился ошибкой"
    fi
    members=$(patroni_cluster | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["members"]))' 2>/dev/null || true)
    leaders=$(patroni_cluster | python3 -c 'import json,sys; ms=json.load(sys.stdin)["members"]; print(sum(1 for m in ms if m["role"]=="leader"))' 2>/dev/null || true)
    [ "${members}" = "3" ] && [ "${leaders}" = "1" ] \
      && ok "T4a: кластер цел (3 члена, 1 лидер)" || bad "T4a: кластер: members=${members:-?} leaders=${leaders:-?}"
  else
    bad "T4a: прогон не стартовал"
  fi
fi

# =========================================================================
section "T4b. Отказ одного брокера Kafka (кластер жив: RF3/minISR2)"

# Отказ должен прийтись на нагрузку: прогон 300@30/с (~10с), kill в середине
RUN4B=$(writer_start '{"totalMessages": 300, "messageSizeBytes": 256, "ratePerSecond": 30}')
if [ -n "${RUN4B}" ]; then
  ok "T4b: прогон стартовал (${RUN4B})"
  sleep 6
  if ./scripts/stand.sh fail kafka2 2>&1 | sed 's/^/    /'; then
    ok "T4b: kafka2 остановлен"
  else
    bad "T4b: fail kafka2 завершился ошибкой"
  fi
  if wait_writer_finished 600; then ok "T4b: прогон завершился"; else bad "T4b: прогон не завершился"; fi
  writer_results
  wr4b=$(wr 'd["writtenMessages"]')
  u=$(wait_run_unique "${RUN4B}" "${wr4b:-300}" 270)
  [ "${u}" = "${wr4b:-300}" ] && ok "T4b: доставлено всё написанное (${u}) — доставка не останавливалась" || bad "T4b: unique='${u:-?}' != ${wr4b:-300}"
  code=$(validate_run "${RUN4B}" true)
  [ "${code}" = "200" ] && assert_verdict "T4b" "${wr4b:-300}" || bad "T4b: validate HTTP ${code}"
  if ./scripts/stand.sh restore kafka2 2>&1 | sed 's/^/    /'; then
    ok "T4b: kafka2 восстановлен"
  else
    bad "T4b: restore kafka2 завершился ошибкой"
  fi
  brokers=""
  for i in $(seq 1 40); do
    brokers=$(prom_query 'kafka_brokers')
    [ "${brokers}" = "3" ] && break
    sleep 3
  done
  [ "${brokers}" = "3" ] && ok "T4b: kafka_brokers=3" || bad "T4b: kafka_brokers='${brokers:-?}'"
else
  bad "T4b: прогон не стартовал"
fi

# =========================================================================
section "T4c. Полный outage Kafka: WAL копится, после restore догоняет"

RET0=$(prom_query 'pg_slot_wal_retained_bytes{slot_name="outbox_slot"}')
RUN4C=$(writer_start '{"totalMessages": 600, "messageSizeBytes": 256, "ratePerSecond": 30}')
if [ -n "${RUN4C}" ]; then
  ok "T4c: прогон стартовал (${RUN4C})"
  sleep 8
  if ./scripts/stand.sh fail kafka-all 2>&1 | sed 's/^/    /'; then
    ok "T4c: полный outage Kafka"
  else
    bad "T4c: fail kafka-all завершился ошибкой"
  fi
  sleep 15
  RET1=$(prom_query 'pg_slot_wal_retained_bytes{slot_name="outbox_slot"}')
  if [ -n "${RET0}" ] && [ -n "${RET1}" ] && num_gt "${RET1}" "${RET0}"; then
    ok "T4c: WAL у слота растёт (${RET0} -> ${RET1} байт) — LSN не подтверждается"
  else
    bad "T4c: retained WAL не растёт (${RET0:-?} -> ${RET1:-?})"
  fi
  if wait_writer_finished 600; then ok "T4c: прогон Writer завершился (запись от Kafka не зависит)"; else bad "T4c: прогон не завершился"; fi
  writer_results
  wr4c=$(wr 'd["writtenMessages"]')
  [ "$(wr 'd["state"]')" = "completed" ] && ok "T4c: state=completed" || bad "T4c: state=$(wr 'd["state"]')"
  if ./scripts/stand.sh restore 2>&1 | sed 's/^/    /'; then
    ok "T4c: брокеры восстановлены"
  else
    bad "T4c: restore завершился ошибкой"
  fi
  brokers=""
  for i in $(seq 1 40); do
    brokers=$(prom_query 'kafka_brokers')
    [ "${brokers}" = "3" ] && break
    sleep 3
  done
  [ "${brokers}" = "3" ] && ok "T4c: kafka_brokers=3" || bad "T4c: kafka_brokers='${brokers:-?}'"
  if ! wait_vready 120; then
    info "T4c: валидатор поднимался рестарт-политикой — ждём /ready дополнительно"
    wait_vready 120 || bad "T4c: валидатор /ready не 200"
  fi
  u=$(wait_run_unique "${RUN4C}" "${wr4c:-600}" 600)
  [ "${u}" = "${wr4c:-600}" ] && ok "T4c: backlog дренирован (${u} уникальных)" || bad "T4c: unique='${u:-?}' != ${wr4c:-600}"
  wait_leader_active 180 "T4c(после restore)"
  code=$(validate_run "${RUN4C}" true)
  [ "${code}" = "200" ] && assert_verdict "T4c" "${wr4c:-600}" || bad "T4c: validate HTTP ${code}"
else
  bad "T4c: прогон не стартовал"
fi

# =========================================================================
if [ "${PAIR}" = "1" ]; then
  section "T5. Active-Passive: fail текущего лидера — доставку принимает партнёр"

  # лидер определяется по факту (после T4c им может быть любой из пары):
  # kill лидера обязан дать переключение, kill standby — нет
  LEADER=""; TAKER=""
  a1=""; a2=""
  for i in $(seq 1 30); do
    a1=$(is_active1); a2=$(is_active2)
    if [ -n "${a1}" ] && [ -n "${a2}" ] && [ $(( a1 + a2 )) -eq 1 ]; then break; fi
    sleep 2
  done
  if [ "${a1}" = "1" ] && [ "${a2}" = "0" ]; then
    LEADER=outbox-1; TAKER=outbox-2
  elif [ "${a1}" = "0" ] && [ "${a2}" = "1" ]; then
    LEADER=outbox-2; TAKER=outbox-1
  fi

  if [ -n "${LEADER}" ]; then
    F0=$(failovers_of "${TAKER}")
    RUN5=$(writer_start '{"totalMessages": 900, "messageSizeBytes": 256, "ratePerSecond": 30}')
    if [ -n "${RUN5}" ]; then
      ok "T5: прогон стартовал (${RUN5}); лидер cdc-${LEADER} — fail его, доставку принимает cdc-${TAKER}"
      sleep 8
      if ./scripts/stand.sh fail "${LEADER}" 2>&1 | sed 's/^/    /'; then
        ok "T5: cdc-${LEADER} (лидер) остановлен"
      else
        bad "T5: fail ${LEADER} завершился ошибкой"
      fi
      at=""
      for i in $(seq 1 45); do
        at=$(is_active_of "${TAKER}")
        [ "${at}" = "1" ] && break
        sleep 2
      done
      [ "${at}" = "1" ] && ok "T5: cdc-${TAKER} принял лидерство (is_active=1)" || bad "T5: cdc-${TAKER} is_active='${at:-?}'"
      F1=$(failovers_of "${TAKER}")
      # счётчик OTel без записей не экспортируется: taker, ни разу не бывший
      # лидером в этом процессе, до переключения метрики не имеет (F0 пуст = 0)
      if [ -n "${F1}" ] && num_ge "${F1}" "$(( ${F0:-0} + 1 ))"; then
        ok "T5: outbox_failovers_total cdc-${TAKER} +1 (${F0:-нет метрики} -> ${F1})"
      else
        bad "T5: outbox_failovers_total cdc-${TAKER} '${F0:-?}' -> '${F1:-?}'"
      fi
      if wait_writer_finished 600; then ok "T5: прогон завершился"; else bad "T5: прогон не завершился"; fi
      writer_results
      wr5=$(wr 'd["writtenMessages"]')
      u=$(wait_run_unique "${RUN5}" "${wr5:-900}" 270)
      [ "${u}" = "${wr5:-900}" ] && ok "T5: доставлено всё написанное (${u})" || bad "T5: unique='${u:-?}' != ${wr5:-900}"
      code=$(validate_run "${RUN5}" true)
      [ "${code}" = "200" ] && assert_verdict "T5" "${wr5:-900}" || bad "T5: validate HTTP ${code}"
      if ./scripts/stand.sh restore "${LEADER}" 2>&1 | sed 's/^/    /'; then
        ok "T5: cdc-${LEADER} возвращён"
      else
        bad "T5: restore ${LEADER} завершился ошибкой"
      fi
      al=""
      for i in $(seq 1 60); do
        al=$(is_active_of "${LEADER}"); at=$(is_active_of "${TAKER}")
        if [ -n "${al}" ] && [ "${al}" = "0" ] && [ "${at}" = "1" ]; then break; fi
        sleep 2
      done
      if [ "${al}" = "0" ] && [ "${at}" = "1" ]; then
        ok "T5: cdc-${LEADER} вернулся standby (is_active=0), лидерство не украл"
      else
        bad "T5: после возврата cdc-${LEADER}='${al:-?}' cdc-${TAKER}='${at:-?}'"
      fi
    else
      bad "T5: прогон не стартовал"
    fi
  else
    bad "T5: лидер не определён (cdc-outbox-1='${a1:-?}', cdc-outbox-2='${a2:-?}') — фаза пропущена"
  fi
else
  info "T5 пропущен: пара не поднята (профиль redundancy выключен)"
fi

# =========================================================================
section "Финал: полный health-check стенда"

if ./scripts/stand.sh check > /tmp/intd-check.log 2>&1; then
  ok "stand.sh check: $(grep -o 'ИТОГ: [0-9]*/[0-9]* ok' /tmp/intd-check.log | tail -1)"
else
  bad "stand.sh check провален:"
  tail -30 /tmp/intd-check.log | sed 's/^/    /'
fi

finish
