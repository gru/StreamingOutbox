# implementation-plan.md — план реализации outbox-сервиса (cdc-outbox)

План разбит на небольшие задачи; каждая завершается живой проверкой на стенде — это точка контроля владельца. Порядок: конвейер доставки (чтение → публикация → checkpoint) → устойчивость → режимы → резервирование → приёмка. Основание — `recommendations.md`, `dependencies.md` §2–§5, `outbox-table.md`, `metrics.md`, `stand.md`.

Отражает решение владельца: специальный drain-режим исключён — вместо него idle-предупреждение (`recommendations.md` «Контроль простоя потока»).

## Чек-лист

- [x] 1. Конфигурация и каркас процесса
- [x] 2. Failover-слот сервисом + проверка объектов (publication и topic — от init)
- [ ] 3. Чтение слота → `OutboxMessage`
- [ ] 4. Публикация в Kafka: продюсер Confluent.Kafka
- [ ] 5. Checkpoint Store в Kafka + подтверждение LSN
- [ ] 6. Устойчивость: переподключения, backpressure, graceful stop
- [ ] 7. Recovery Mode
- [ ] 8. Idle-предупреждение
- [ ] 9. Метрики: полный аудит
- [ ] 10. Active-Passive: consumer group на `cdc-checkpoints` (профиль redundancy)
- [ ] 11. Приёмочные сценарии и синхронизация документации

## Правила, общие для всех задач

- **Docs-first**: где в docs есть пробел по задаче (помечено «docs:») — контракт сначала фиксируется в docs, потом код.
- Финал каждой задачи: сборка и все тесты решения зелёные в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (новые юнит-тесты задачи в `tests/CdcOutbox.UnitTests` обязательны; полный прогон — `./scripts/test.sh`: поднимает/сносит мини-стенд интеграционных тестов pg-it/kafka-it и передаёт подключение стандартными env `CDCOUTBOX__*` — интеграционные тесты без стенда не исполняются, skip-логики нет, dependencies.md §2), `./scripts/lint.sh`, `./scripts/stand.sh check` 58/58 (60/60 с профилем `redundancy`).
- Коммит — только по команде владельца.
- Проверочная БД-команда (паттерн stand.sh — через haproxy, уходит текущему лидеру; контейнер pg1 должен быть жив, иначе другой живой pg-контейнер):

  ```bash
  docker compose exec -T pg1 psql -h haproxy -U postgres -d outbox -tAc "…"
  ```

---

## Задача 1. Конфигурация и каркас процесса (доставки ещё нет)

Состав:

- docs: таблица конфигурации `CDCOUTBOX__*` в dependencies.md §2 (подключения PG/Kafka, имена слота/publication/topic, параметры продюсера Kafka — acks, идемпотентность, батчинг, таймауты доставки, `message.max.bytes`; размеры очереди и батчей, интервал checkpoint, `idle_timeout`) + состав `/ready` для cdc-outbox (в реализации — только postgres-чек: чек процессора исключён решением владельца — в Active-Passive standby не держит цикл, но экземпляр здоров; активность отслеживает метрика `outbox_processor_is_active`; ответ — дефолтный форматтер фреймворка, plain-text статус).
- Код: options-классы (`OutboxStorageOptions` — контракт `outbox-table.md`), `BackgroundService`-каркас цикла процессора, класс `OutboxMetrics` с каркасом (`is_active`, `last_iteration`, `errors`).

Проверка:

- `./scripts/stand.sh rebuild cdc-outbox-1` → `curl -s localhost:18084/ready` = Healthy; `/metrics` отдаёт `outbox_processor_is_active`.
- `./scripts/stand.sh check` 48/48; вердикт по прогону Writer — по-прежнему missing (доставки нет; контрольная точка «ничего не сломали»).

## Задача 2. Failover-слот сервисом + проверка объектов (publication и topic — от init)

Состав:

- Publication `outbox_publication`, гранты `sys_replication` и topic `cdc-checkpoints` создаёт инициализация стенда (init-db `03-outbox-publication.sql` + kafka-init): `sys_replication` не может создать publication — CREATE PUBLICATION требует CREATE на БД и владения таблицей (экспериментально подтверждено; ремарка — dependencies.md §4). Отдельная docs-подзадача отпала — контракт зафиксирован.
- Код: при старте — идемпотентное создание failover-слота `outbox_slot` (`failover=true`, БД `outbox`, dependencies.md §5; достаточно атрибута REPLICATION — у роли есть, каталог `pg_replication_slots` ей виден) и проверка существующих объектов: publication (имя, `publish='insert'`, таблица outbox.messages — каталоги `pg_publication*` видны роли), topic `cdc-checkpoints`; расхождение конфигурации — явная ошибка.

Проверка:

- psql: `select slot_name, plugin, failover from pg_replication_slots` → `outbox_slot / pgoutput / t`; рестарт контейнера ничего не меняет (идемпотентность); в Prometheus оживают `pg_slot_*`.
- Publication и topic уже проверены текущим `check` (58/58): `select pubname, pubinsert, pubupdate, pubdelete from pg_publication` → только insert; `docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --describe --topic cdc-checkpoints --bootstrap-server localhost:9092` → 1 партиция, RF 3, compact. Слот добавлен в `check` db_check'ой (57→58).
- Реализационные уточнения по итогам живой верификации: стартовый блок выделен в hosted-службу `OutboxStartupChecks` (стартует до цикла процессора, ошибка валит подъём хоста до веб-сервера; цикл `OutboxProcessor` не пришлось привязывать к инфраструктуре — его юнит-тест остался живым); перед созданием слота — явная проверка `pg_is_in_recovery()` (в PG17 создание логического слота на standby не завершается — сессия виснет, проверено на стенде); ошибка wal_level < logical приходит с SQLSTATE 55000 (проверено на отдельном postgres:17.2), транслируется в понятное сообщение. Существующий слот проверяется только по плагину: `failover=false` — WARN, не ошибка (решение владельца: failover — инфраструктурное решение о переживании promotion, чтению не требуется; созданный сервисом слот всегда failover=true). Publication проверяется по существованию, pubinsert и вхождению outbox-таблицы: публикация других операций — WARN, не ошибка (решение владельца: важны только INSERT, лишние записи игнорируются).

## Задача 3. Чтение слота → `OutboxMessage` (публикации нет)

Состав:

- Код: `StartReplication(PgOutputReplicationSlot…)` proto v1, типизированные сообщения Npgsql; только `Get<string>()` + свой парсинг (грабли dependencies.md §2), INSERT `outbox.messages` → `OutboxMessage` (bytea приходит hex-текстом), bounded-канал; **LSN не подтверждается**; метрика `read_messages_current`; юнит-тесты парсера.

Проверка:

- `./scripts/stand.sh load 500` → в логе декодировано суммарно 500 сообщений по границам транзакций, `outbox_processor_errors_total`=0; таблица пустая.
- Удерживаемый слотом WAL растёт (`pg_slot_wal_retained_bytes` в Prometheus) — доказательство, что позицию не двигаем.

## Задача 4. Публикация в Kafka: продюсер Confluent.Kafka

Состав:

- Код: публикация напрямую продюсером Confluent.Kafka (без интерфейса-транспорта — решение владельца, зафиксировано в `outbox-table.md` «Публикация в Kafka») из `OutboxMessage`: идемпотентный продюсер `acks=all`, батчинг, отслеживание delivery-репортов (событие опубликовано = получен ACK, не «отправлено в буфер»), маршрутизация по `Topic`/`Key`/`Partition` сообщения; метрики `sent_messages_total`, `publish_latency_seconds`, `pending_events_current`, `message_age_seconds`. LSN пока не подтверждается.
- Параметры Kafka продюсера (вносятся в конфиг-таблицу `CDCOUTBOX__*` из задачи 1): `bootstrap.servers` (kafka1-3:9092), `acks=all`, `enable.idempotence=true`, батчинг (`batch.num.messages` / `linger.ms`), `delivery.timeout.ms` и ретраи, `message.max.bytes`; дефолты фиксируются в docs на задаче 1.

Проверка:

- `./scripts/stand.sh load 500 && ./scripts/stand.sh verdict` → **pass, missing=0, unique=500** — первое сквозное доказательство доставки; `pending` возвращается к 0.
- Рестарт контейнера → повторная доставка неподтверждённых (дубликаты, вердикт всё равно pass) — наглядная демонстрация at-least-once до появления checkpoint.

## Задача 5. Checkpoint Store в Kafka + подтверждение LSN (замыкание at-least-once)

Состав:

- docs: правила согласования при старте (checkpoint ≤ `confirmed_flush` → берём слот; checkpoint > `confirmed_flush` или WAL недоступен → явная ошибка; слот отсутствует при живом checkpoint → ошибка, не пересоздавать — recommendations.md §2).
- Код: конвейер строго `WAL Read → Kafka Send → Kafka ACK → Durable Checkpoint → LSN Flush`: checkpoint в `cdc-checkpoints` (ключ = имя слота; сохранён = получен ACK записи), периодическое сохранение на границах транзакций, `SetReplicationStatus(TransactionEndLsn)` + `SendStatusUpdate()` только после; монотонность; метрики `checkpoint_age_seconds`, `checkpoint_lag_bytes`, `checkpoint_errors_total`.

Проверка:

- Под нагрузкой `confirmed_flush_lsn` двигается; `cdc-checkpoints` содержит записи checkpoint (kafka-console-consumer).
- Рестарт в середине прогона → продолжение от `confirmed_flush`, дубликаты ровно в окне неподтверждённого, verdict pass.
- Контроль нормального режима: stop сервиса → `pg_drop_replication_slot('outbox_slot')` → старт → явная ошибка «слот отсутствует», слот не пересоздаётся.

## Задача 6. Устойчивость: переподключения, backpressure, graceful stop

Состав:

- Код: классификация ошибок — транзиентные (разрывы, 55006, read_only после failover, недоступность Kafka — backoff с ресетом по сессиям ≥30с) и терминальные (инвалидированный слот, несовместимость checkpoint); bounded-канал как backpressure; корректный stop: дождаться ACK → checkpoint → подтверждение → закрыть соединение.

Проверка (после каждой инъекции — verdict pass):

- `./scripts/stand.sh fail pg` → переподключение к новому лидеру; `fail kafka2` → доставка продолжается; `fail kafka-all` под нагрузкой → WAL копится, после `restore` догоняет; `fail haproxy` → reconnect; `outbox_slot_reconnects_total` растёт.
- `docker compose stop cdc-outbox-1` под нагрузкой → чистое завершение (прогресс сохранён, verdict pass после старта).
- Известная грабля: graceful switchover при живом консюмере слота может виснуть — рестарт сервиса; зафиксировать в docs.

## Задача 7. Recovery Mode

Состав:

- docs: способ включения (рекомендация: env `CDCOUTBOX__RECOVERY_MODE=true` на один старт) и метрика режима — в metrics.md её нет, предложение: gauge `outbox_recovery_mode` (решение владельца при выполнении задачи).
- Код: в обычном режиме несовместимость checkpoint/слота — терминальная ошибка (из задачи 5); в recovery — старт от текущей позиции слота (или пересоздание слота от текущей позиции PG), новый checkpoint сохраняется **до** начала чтения, в логах и метриках старая/новая позиция.

Проверка:

- Сценарий дропа слота из задачи 5: без флага — терминальная ошибка, обработка стоит; с флагом — слот пересоздан, checkpoint переписан, свежий `load` доставляется (verdict pass).
- Вердикт старого прогона честно показывает пропущенные — режим осознанно допускает пропуск (осознанный отказ, recommendations.md «Recovery Mode»).

## Задача 8. Idle-предупреждение (контроль простоя потока)

Состав:

- docs: уже зафиксирован — `recommendations.md` «Контроль простоя потока» (`idle_timeout`; drain-режим исключён решением владельца).
- Код: таймер простоя в цикле чтения; истечение `idle_timeout` без событий → WARN в лог, не чаще одного раза за интервал; новое событие сбрасывает таймер; состояние сервиса и репликация не меняются.

Проверка:

- Без нагрузки WARN появляется в `docker compose logs cdc-outbox-1` с каденсом интервала и не чаще.
- Под нагрузкой (`stand.sh load 500`) предупреждений нет; после завершения прогона WARN возобновляется.

## Задача 9. Метрики: полный аудит

Состав:

- docs: толкование `outbox_saved_messages_total` / `partitioning_outbox_saved_messages_total` — сервис сам в outbox не пишет, счётчик по INSERT-изменениям слота (`publish='insert'` = 1 change на сохранённое).
- Код: все 15 метрик metrics.md, label `partition` = `outbox_partition` (в стенде 0), + решение по метрике recovery (задача 7).

Проверка:

- Все имена из metrics.md присутствуют в `curl -s localhost:18084/metrics`.
- Под нагрузкой: `sent_total` растёт, age/publish-latency имеют наблюдения, `checkpoint_age` < интервала сохранения, `lag_bytes` ≈ 0 в покое, ошибки 0; таргет cdc-outbox-1 в Prometheus up, графики живые.

## Задача 10. Active-Passive: арбитр на consumer group `cdc-checkpoints` (включение профиля redundancy)

Состав:

- Код: consumer group на `cdc-checkpoints` (partition назначен → активный; standby регулярно `poll()`), transactional producer с постоянным `transactional.id` от имени слота (fencing), транзакционный checkpoint (события + запись checkpoint одной транзакцией; LSN после commit), потеря лидерства → полная остановка без переинициализации producer'а, новый лидер: fencing → checkpoint `read_committed` (последнее значение по ключу, не group offset) → ожидание освобождения слота (55006 транзиент); метрики `is_active` 1/0 и `failovers_total`.

Проверка:

- Включить профиль по инструкции stand.md (+таргет prometheus, рестарт) → `check` 50/50; `is_active`: 1 у одного экземпляра, 0 у второго.
- Под нагрузкой `./scripts/stand.sh fail outbox-1` → rebalance, outbox-2 занимает слот (`active_pid` в pg_replication_slots), verdict pass, `outbox_failovers_total` +1.
- `./scripts/stand.sh restore outbox-1` → возвращается standby, лидерство не крадёт.

## Задача 11. Приёмочные сценарии и синхронизация документации

Состав:

- Код/инфра: живой integration-харнесс доставки (по образцу `integration-delivery.sh` старого стенда: запись → вердикт → рестарт-продолжение → отказ-восстановление); сценарий-репетиция обновления PostgreSQL по пути dump/restore (остановить запись → дождаться idle-предупреждений и нулевого `checkpoint_lag_bytes` → остановить cdc-outbox-1 → dump/restore БД → старт с `recovery_mode` → свежий прогон pass); синхронизация stand.md («что появится» → появилось) и AGENTS.md.

Проверка:

- Харнесс зелёный end-to-end; `check` 48/48 (50/50 с профилем `redundancy`); все тесты решения зелёные.
