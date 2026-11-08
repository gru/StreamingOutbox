# Лог фазы B — сценарии вне харнесса (счётчики до/после, инъекции, вердикты)

```text

================================================================
B1. Отказ реплики PG (fail pg2; лидер pg1 цел, sync pg3 цел) под нагрузкой 600@30/s
ДО: is_active(cdc-1)=1 failovers=1 reconnects=0
прогон a6543a58-a7e5-4413-b38b-acd62a418914 стартовал (22:33:37)
kill pg2 (лидер сейчас: pg1)
 Container streaming-outbox-pg2-1  Killing
 Container streaming-outbox-pg2-1  Killed
  pg2 остановлен (kill = ручная остановка; возврат: stand.sh restore pg2)
прогон завершён: state=completed (22:33:54, 6с после инъекции)
ПОСЛЕ: is_active(cdc-1)=1 reconnects=0
  pg_up=1
Кластер: leader=pg1 sync=pg3 members=3 timeline=13
Проверка: ./scripts/stand.sh check
Patroni: leader=pg1 sync=pg3 members=3 timeline=13
RID_B1=a6543a58-a7e5-4413-b38b-acd62a418914
written=600 failed=0 ambiguous=0
Вердикт по прогону a6543a58-a7e5-4413-b38b-acd62a418914 (ожидаемый набор из Writer: fetchFromWriter):
{
    "runId": "a6543a58-a7e5-4413-b38b-acd62a418914",
    "verdict": "pass",
    "computedAtUtc": "2026-11-07T19:34:31.8290503+00:00",
    "kafka": {
        "records": 600,
        "uniqueIds": 600,
        "duplicateRecords": 0
    },
    "expected": {
        "source": "writer",
        "writtenCount": 600,
        "ambiguousCount": 0,
        "failedCount": 0
    },
    "missingWrittenIdsCount": 0,
    "missingWrittenIdsSample": [],
    "ambiguousDeliveredCount": 0,
    "ambiguousNotDeliveredCount": 0,
    "unexpectedExtraIdsCount": 0,
    "unexpectedExtraIdsSample": [],
    "cdc": null
}

================================================================
B2. Полный отказ PG (fail pg-all) под нагрузкой 600@30/s
ДО: is_active(cdc-1)=1 reconnects=0
прогон 4a6de8dc-e8ca-44ee-8ff5-5ef54da3f810 стартовал (22:34:52)
 Container streaming-outbox-pg1-1  Killing
 Container streaming-outbox-pg1-1  Killed
  pg1 остановлен
 Container streaming-outbox-pg2-1  Killing
 Container streaming-outbox-pg2-1  Killed
  pg2 остановлен
 Container streaming-outbox-pg3-1  Killing
 Container streaming-outbox-pg3-1  Killed
  pg3 остановлен
Полный отказ PG. Возврат: stand.sh restore (старт пошаговый, затем выборы + bootstrap)
Writer /ready во время outage: HTTP 503
ОБЗОР outage: reconnects=1 is_active=1 (через 21с)
Ожидание готовности Writer /ready (потолок 30с)...
  Writer /ready: 200
  pg_up не вернулся за 30с — рестарт postgres-exporter (залипающий пул соединений)
  pg_up=1 после рестарта exporter'а
Кластер: leader=pg1 sync=pg2 members=3 timeline=14
Проверка: ./scripts/stand.sh check
restore завершён через 72с от инъекции
Writer /ready после restore: HTTP 200
прогон завершён: state=completed (72с после инъекции)
written=600 failed=0 ambiguous=0
ПОСЛЕ: reconnects=2 errors_total=2
RID_B2=4a6de8dc-e8ca-44ee-8ff5-5ef54da3f810
verdict=pass records=1000 unique=600 dup=400 missing=0 unexpected=0

================================================================
B3. Отказ HAProxy (fail haproxy) под нагрузкой 600@30/s
ДО: reconnects=2 errors_total=2
прогон 21790457-1476-4820-a4b7-a94d441aca07 стартовал (22:37:30)
 Container streaming-outbox-haproxy-1  Killing
 Container streaming-outbox-haproxy-1  Killed
  haproxy остановлен (кластер PG жив, путь записи рвётся — Writer /ready падает). Возврат: stand.sh restore haproxy
ВО время outage: reconnects=4 is_active=1 writer_state=failed (через 15с)
  haproxy: Up 6 seconds (healthy)
Кластер: leader=pg1 sync=pg2 members=3 timeline=14
Проверка: ./scripts/stand.sh check
haproxy восстановлен через 23с от инъекции
прогон завершён: state=failed (23с после инъекции)
written=500 failed=100 ambiguous=0
ПОСЛЕ: reconnects=4 errors_total=5
RID_B3=21790457-1476-4820-a4b7-a94d441aca07
verdict=pass records=500 unique=500 dup=0 missing=0 unexpected=0

================================================================
B4. Kill standby (fail outbox-2; лидер cdc-outbox-1) под нагрузкой 600@30/s
ДО: is_active(cdc-1)=1 failovers(cdc-1)=1 reconnects=5
прогон 5687e6a7-87d2-4294-a85c-158cc0e3d0e2 стартовал (22:40:56)
 Container streaming-outbox-cdc-outbox-2-1  Killing
 Container streaming-outbox-cdc-outbox-2-1  Killed
  cdc-outbox-2 остановлен (kill = ручная остановка; возврат: stand.sh restore outbox-2)
прогон завершён: state=completed (6с после инъекции; полный прогон 600@30/с ~= 20с)
written=600 failed=0 ambiguous=0
ПОСЛЕ: is_active(cdc-1)=1 failovers(cdc-1)=1 reconnects=5 (лидерство не тронуто)
  cdc-outbox-2: Up 6 seconds (healthy)
Кластер: leader=pg1 sync=pg2 members=3 timeline=14
Проверка: ./scripts/stand.sh check
после возврата: cdc-1=1 cdc-2=0 (ожидание: 1/0 — вернулся standby)
verdict=pass records=600 unique=600 dup=0 missing=0 unexpected=0

================================================================
B5. Полный отказ доставки (fail outbox-all) под нагрузкой 1000@50/s — WAL retention от дренированного базлайна
БАЗЛАЙН (drained): checkpoint_lag=0 retained=471400
прогон 0032c5da-ce53-47ef-be8f-dec3a5774148 стартовал (22:42:27)
 Container streaming-outbox-cdc-outbox-1-1  Killing
 Container streaming-outbox-cdc-outbox-1-1  Killed
  cdc-outbox-1 остановлен (возврат: stand.sh restore outbox-1)
 Container streaming-outbox-cdc-outbox-2-1  Killing
 Container streaming-outbox-cdc-outbox-2-1  Killed
  cdc-outbox-2 остановлен (возврат: stand.sh restore outbox-2)
прогон Writer завершён: state=completed (13с после инъекции — запись от CDC не зависит)
written=1000 failed=0 ambiguous=0
В OUTAGE: retained=680128 (было 471400) wal_status/active=reserved|f — слот не потребляется, WAL копится
  cdc-outbox-1: Up 6 seconds (healthy)
  cdc-outbox-2: Up 7 seconds (healthy)
Кластер: leader=pg1 sync=pg2 members=3 timeline=14
Проверка: ./scripts/stand.sh check
restore через 41с от инъекции
лидерство: cdc-1=1 cdc-2=0 (через 41с от инъекции)
дренаж: unique=1000 (41с от инъекции)
ПОСЛЕ ДРЕНАЖА: retained=675256 failovers: cdc-1=1 cdc-2=нет
RID_B5=0032c5da-ce53-47ef-be8f-dec3a5774148
22 kB|22 kB
verdict=pass records=1100 unique=1000 dup=100 missing=0 unexpected=0

================================================================
B6. Switchover PG (graceful, лидер pg1 -> кандидат sync pg2) под нагрузкой 600@30/s
Patroni: leader=pg1 sync=pg2 members=3 timeline=14
ДО: reconnects(cdc-1)=0
прогон 344b689f-9133-4f5a-9c28-e5ff63c1c47e стартовал (22:44:21)
zsh:16: command not found: timeout
Patroni: leader=pg1 sync=pg2 members=3 timeline=14
прогон завершён: state=completed (10с после switchover-фазы)
written=600 failed=0 ambiguous=0
ПОСЛЕ: reconnects(cdc-1)=0 is_active=1
RID_B6=344b689f-9133-4f5a-9c28-e5ff63c1c47e

================================================================
B6 (повтор — в первом заходе timeout(1) отсутствовал в macOS, инъекция не выполнилась)
прогон 0a3cb276-0436-47aa-9df6-87dfa675bca2 стартовал (22:44:54)
Switchover pg1 -> pg2 (REST-запрос на порт кандидата :8009)
  REST 503: Switchover status unknown
  -- посредник ответил 503 (переключение может быть запланировано) — продолжаю ожидание
Ожидание лидера pg2 (потолок 120с)...
pg2 не стал лидером за 120с (лидер: pg1)
switchover-фаза: 141с
Patroni: leader=pg1 sync=pg2 members=3 timeline=?
рестарт активного cdc-outbox-1 (лекарство documented грабли) 22:55:28
 Container streaming-outbox-cdc-outbox-1-1  Started
кластер: pg1:replica/stopped pg2:leader/running pg3:replica/running (3с ожидания)
прогон завершён: state=failed (632с от switchover-старта)
written=400 failed=200 ambiguous=0
Patroni: leader=pg2 sync= members=3 timeline=15
ALTER PUBLICATION
[init-db] Bootstrap complete.
ПОСЛЕ: reconnects(cdc-1)=0 is_active=0
RID_B6=0a3cb276-0436-47aa-9df6-87dfa675bca2
verdict=pass records=400 unique=400 dup=0 missing=0 unexpected=0
  ok  kafka_brokers=3

ИТОГ: провалов 1 из 62 проверок
```
