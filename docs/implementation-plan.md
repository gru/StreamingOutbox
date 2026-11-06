# implementation-plan.md — план реализации outbox-сервиса (cdc-outbox)

План разбит на небольшие задачи; каждая завершается живой проверкой на стенде — это точка контроля владельца. Порядок: конвейер доставки (чтение → публикация → checkpoint) → устойчивость → режимы → резервирование → приёмка. Основание — `recommendations.md`, `dependencies.md` §2–§5, `outbox-table.md`, `metrics.md`, `stand.md`.

Отражает решение владельца: специальный drain-режим исключён — вместо него idle-предупреждение (`recommendations.md` «Контроль простоя потока»).

## Чек-лист

- [x] 1. Конфигурация и каркас процесса
- [x] 2. Failover-слот сервисом + проверка объектов (publication и topic — от init)
- [x] 3. Чтение слота → `OutboxMessage`
- [x] 4. Публикация в Kafka: продюсер Confluent.Kafka
- [x] 5. Checkpoint Store в Kafka + подтверждение LSN
- [x] 6. Устойчивость: переподключения, backpressure, graceful stop
- [x] 7. Recovery Mode
- [x] 8. Idle-предупреждение
- [x] 9. Метрики: полный аудит
- [x] 10. Active-Passive: consumer group на `cdc-checkpoints` (профиль redundancy)
- [x] 11. Приёмочные сценарии и синхронизация документации
- [x] 12. Бинарный протокол pgoutput (опция `binary`)

## Правила, общие для всех задач

- **Docs-first**: где в docs есть пробел по задаче (помечено «docs:») — контракт сначала фиксируется в docs, потом код.
- Финал каждой задачи: сборка и все тесты решения зелёные в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (новые юнит-тесты задачи в `tests/CdcOutbox.UnitTests` обязательны; полный прогон — `./scripts/test.sh`: поднимает/сносит мини-стенд интеграционных тестов pg-it/kafka-it и передаёт подключение стандартными env `CDCOUTBOX__*` — интеграционные тесты без стенда не исполняются, skip-логики нет, dependencies.md §2), `./scripts/lint.sh`, `./scripts/stand.sh check` 59/59 (62/62 с профилем `redundancy`).
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

Выполнено 2026-11-02 с отклонениями от исходных формулировок: env `CDCOUTBOX__RECOVERYMODE` (ключ одним словом — одинарное подчёркивание binder не превращает в разделитель, `RECOVERY_MODE` молча потерялось бы); метрика — gauge `outbox_recovery_mode` с label `slot` = имя слота (решение владельца — задел под несколько сервисов с разными слотами; унификация labels остальных метрик — заметка в задаче 9); пересоздание слота — только отсутствующего, существующий «непригодный» (пустой `confirmed_flush`/чужой плагин) — терминальная ошибка с инструкцией (решение владельца — автодроп чужого слота опасен); при согласованном состоянии — no-op с WARN (защита от забытого флага). Проверено: IT 2 теста (живые PG+Kafka), стенд — дроп слота → терминальная без флага → с флагом слот пересоздан + checkpoint `0/4A9BB58 → 0/4AE4148` до чтения → load 500 pass (дублей 0), записи старых runId в топике 0 (пропуск доказан), рестарт без флага — check 58/58.

## Задача 8. Idle-предупреждение (контроль простоя потока)

Состав:

- docs: уже зафиксирован — `recommendations.md` «Контроль простоя потока» (`idle_timeout`; drain-режим исключён решением владельца).
- Код: таймер простоя в цикле чтения; истечение `idle_timeout` без событий → WARN в лог, не чаще одного раза за интервал; новое событие сбрасывает таймер; состояние сервиса и репликация не меняются.

Проверка:

- Без нагрузки WARN появляется в `docker compose logs cdc-outbox-1` с каденсом интервала и не чаще.
- Под нагрузкой (`stand.sh load 500`) предупреждений нет; после завершения прогона WARN возобновляется.

Выполнено 2026-11-02: `IdleActivityTracker` (Processing; Stopwatch-таймер, WARN не чаще одного раза за интервал — каденс подтверждён владельцем) + параллельный idle-цикл в `OutboxSlotReader.ReadAsync` (проверка раз в 1 с, живёт со стримом на токене flush-цикла); активность = транзакция хотя бы с одним outbox-сообщением (`MarkActivity` в ветке Commit) — пустые коммиты и чужие таблицы таймер не сбрасывают; WARN только логирует, состояние/репликация не меняются; в паузах backoff между сессиями чтения предупреждений нет. Текст WARN скорректирован решением владельца 2026-11-02: без новых событий слот не подтверждается и WAL может накапливаться (heartbeat-механизм отсутствует — сознательное решение, сервис рассчитан на постоянно пишущиеся outbox) — сообщение выводит порог и фактический простой и предписывает проверить запись в outbox; формулировка «репликация активна, вмешательство не требуется» признана вводящей в заблуждение и убрана (из кода и recommendations.md). Проверено: unit 5 (трекер), IT 1 (живой PG: WARN на тишине → сброс сообщением → возобновление); стенд — check 58/58, тишина: WARN «31 с» → «61 с» → «91 с» (каденс ровно 30 с, значение растёт), окно 1200@20/с (55 с записи) — 0 WARN, после последней транзакции WARN «30 с» через интервал, verdict pass 1200/1200 (дублей 0). Тесты решения — 246.

## Задача 9. Метрики: полный аудит

Состав:

- docs: толкование `outbox_saved_messages_total` / `partitioning_outbox_saved_messages_total` — сервис сам в outbox не пишет, счётчик по INSERT-изменениям слота (`publish='insert'` = 1 change на сохранённое).
- Код: все 15 метрик metrics.md, label `partition` = `outbox_partition` (в стенде 0), + решение по метрике recovery (задача 7) — реализовано в задаче 7 как gauge `outbox_recovery_mode` с label `slot`; при аудите рассмотреть унификацию labels под мульти-слотовые сервисы (partition → slot, решение владельца 2026-11-02) — в модели «один слот = одна партиция checkpoint-топика» (задача 15) label `partition` метрик и константа `DefaultOutboxPartition` (`OutboxMetrics`, прикладная партиция outbox — НЕ партиция checkpoint-топика) удаляются.

Проверка:

- Все имена из metrics.md присутствуют в `curl -s localhost:18084/metrics`.
- Под нагрузкой: `sent_total` растёт, age/publish-latency имеют наблюдения, `checkpoint_age` < интервала сохранения, `lag_bytes` ≈ 0 в покое, ошибки 0; таргет cdc-outbox-1 в Prometheus up, графики живые.

Выполнено 2026-11-04. Сверка с исходными условиями (класс `OutboxMetrics` из ТЗ, список предоставил владелец): шесть действующих имён читающей стороны перенесены дословно; `outbox_saved_messages_total` — метрика пишущей стороны (сервис, вставляющий сообщения в outbox): наш сервис в outbox не пишет, факт сохранения — не его событие, в сервисе не реализуется (решение владельца 2026-11-04; в тестовом контуре роль закрывает `writer_messages_written_total` Writer, в проде — метрика бизнес-приложения); `partitioning_outbox_saved_messages_total` — `[Obsolete]` в исходнике (пометка «устаревшая» в metrics.md была перевёрнута относительно исходника и исправлена), не переносится. Labels унифицированы решением владельца 2026-11-04: label `slot` = `ReplicationOptions.SlotName` у ВСЕХ метрик (было: `partition` у 5 процессорных, у остальных ничего — несистемное наследие исходного списка; в мульти-слотовой модели задачи 15 без-label метрики смешали бы наблюдения разных слотов одного процесса); константа `DefaultOutboxPartition` удалена. Критерий нейминга зафиксирован в metrics.md §1: `outbox_<подсистема>_<показатель>` (processor / checkpoint / slot; без подсистемы — общесервисные события и свойства сообщения, поэтому `outbox_message_age_seconds` без `processor_`). `outbox_failovers_total` остаётся на задачу 10 (создаётся сразу с label `slot`). Итог: metrics.md — 14 метрик, в коде 13. Проверено: тесты 246 зелёные (157 unit + 24 IT); стенд — rebuild cdc-outbox-1, check 58/58; load 500: `read = sent = 500`, гистограммы age/publish_latency по 500 наблюдений, `checkpoint_age` ≈ 0.0002 с (< интервала 5 с), `lag_bytes` 0 после дренирования, ошибки 0, verdict pass (дублей 0); второй прогон 300@30/с — pass; Prometheus: таргет cdc-outbox up, рост `sent_messages_total` по scrape'ам (500 → 700 → 800).

## Задача 10. Active-Passive: арбитр на consumer group `cdc-checkpoints` (включение профиля redundancy)

Состав:

- Код: consumer group на `cdc-checkpoints` (partition назначен → активный; standby регулярно `poll()`), transactional producer с постоянным `transactional.id` от имени слота (fencing), транзакционный checkpoint (события + запись checkpoint одной транзакцией; LSN после commit), потеря лидерства → полная остановка без переинициализации producer'а, новый лидер: fencing → checkpoint `read_committed` (последнее значение по ключу, не group offset) → ожидание освобождения слота (55006 транзиент); метрики `is_active` 1/0 и `failovers_total`.
- **Стартовые проверки и Recovery Mode — только у лидера** (гонка, найдена задачей 2026-11-02, владелец: «в проде обеспечить один поднятый экземпляр невозможно»): standby не выполняет `OutboxStartupChecks` (включая `EnsureAsync` и recovery-запись checkpoint) до получения лидерства в consumer group; транзакционный checkpoint с fencing закрывает откат checkpoint'а назад recovery-экземпляром при работающем/сменяющемся лидере (окно между чтением `confirmed_flush` и `SaveAsync` — сегодня приводит к пере-доставке дублями, потери нет, но прогресс и логи дезинформируют). До этой задачи действует ограничение outbox-table.md «Recovery Mode»: recovery — при остановленном втором экземпляре.

Проверка:

- Включить профиль по инструкции stand.md (+таргет prometheus, рестарт) → `check` 50/50; `is_active`: 1 у одного экземпляра, 0 у второго.
- Под нагрузкой `./scripts/stand.sh fail outbox-1` → rebalance, outbox-2 занимает слот (`active_pid` в pg_replication_slots), verdict pass, `outbox_failovers_total` +1.
- `./scripts/stand.sh restore outbox-1` → возвращается standby, лидерство не крадёт.
- Recovery на standby/параллельно стартующем втором экземпляре: не выполняет запись и не откатывает checkpoint лидера (checkpoint в `cdc-checkpoints` не уходит назад при доставке активным в этот момент).

Выполнено 2026-11-04. Арбитраж: контрольная consumer group на `cdc-checkpoints` (`ControlGroupId`, дефолт `cdc-outbox`; CooperativeSticky — вернувшийся экземпляр не крадёт партицию; офсеты не коммитятся); единственный hosted-сервис — `LeadershipArbiter` (poll 1 с всегда: standby держит членство; стартовые свойства процесса — `is_active=0`, `recovery_mode` — с самого старта, оба таргета Prometheus up). Машина состояний лидерства `LeadershipCoordinator`: назначение партиции → `RegisterFailover()` → `InitTransactions` (fencing предыдущей эпохи; вызывается ТОЛЬКО при получении лидерства — повторный после отзывы партиций запрещён, при новом назначении разрешён) → лидерские стартовые проверки (`OutboxStartupChecks.RunAllAsync` — recovery/согласование/Ensure/publication только у лидера) → цикл доставки в собственном DI-скоупе (свежие `ReplicationProgress`/`OutboxPublisher`/`OutboxProcessor`/reader на период лидерства) → `is_active=1`. Отзыв партиций/потеря/fencing — graceful остановка (финальная checkpoint-транзакция, бюджет `FinalCheckpointWaitSeconds`), хост живёт в standby; терминальная ошибка доставки — остановка хоста (как раньше на старте). Продюсер общий транзакционный: `transactional.id = cdc-outbox-<slot>`, `transaction.timeout.ms = DeliveryTimeoutMs+60 000`; публикатор — единый последовательный цикл: батч = Begin → produce → await deliveries → (каденс `CheckpointIntervalSeconds` истёк и всё доставлено) checkpoint-запись в ТОЙ ЖЕ транзакции → Commit → отметка позиций; при простое и в финале — checkpoint-only транзакции (каденс-решение владельца); частичный сбой доставки — доставленная часть commit'ится, недоставленная повторяется; fencing (`ProducerFenced`/`InvalidProducerEpoch`/`InvalidProducerIdMapping`) → `LeadershipLostException` → остановка доставки без финального сохранения. `ReadLatest` — `read_committed` (прерванные записи невидимы — иначе ложный стоп reconcile/потеря в ветке отсутствующего слота); fencing-init до чтения снимает LSO-стену. KafkaValidator и IT-фикстура — `read_committed`. Контракт окружения — dependencies.md §2b (приложение для произвольного Kafka: предусловия env → вывод наших параметров формулами → права принципала (TRANSACTIONAL_ID WRITE/DESCRIBE, GROUP READ/DESCRIBE; `__transaction_state` прав не требует) → стенд-доказательство «изменений не требуется»; провижининг-параметры кодом НЕ проверяются — решение владельца). Термин «fencing» зафиксирован — recommendations.md «Fencing: термин и семантика» (определение, три свойства, механика эпох Kafka, защищаемая инварианта монотонности checkpoint, три линии защиты). `outbox_failovers_total` реализована: +1 на каждое получение лидерства (первое избрание = 1); `is_active` = лидерство+доставка. Проверено: тесты 264 зелёные на момент верификации (173 unit + 26 IT + 65 соседних; после ревью-правок — удалена настройка Acks с её тестами — 257: 166 unit + 26 IT + 65 соседних); lint OK; стенд: rebuild cdc-outbox-1 → check 59/59, `is_active=1`, `failovers_total=1`, load 500 → verdict pass (валидатор read_committed); пара (`--profile redundancy up -d --no-deps cdc-outbox-2` + таргет Prometheus + рестарт) → check 62/62, `is_active` 1/0; под нагрузкой 2000@30/с `fail outbox-1` → outbox-2 принял лидерство (~15–20 с: session timeout + fencing + проверки), `failovers_total` +1, `active_pid` у соединения outbox-2, verdict pass; graceful `docker compose stop cdc-outbox-1` → LeaveGroup → мгновенная передача; `restore outbox-1` → standby, лидерство не украл; recovery на standby (`CDCOUTBOX__RECOVERYMODE=true` при лидере outbox-2) — записи нет, checkpoint лидера не откатывается. Числа `check`: 59/59 (профиль выключен; +проверка is_active=1 у cdc-outbox-1), 62/62 (профиль включён; +проверка «в паре активен ровно один»).

## Задача 11. Приёмочные сценарии и синхронизация документации

Состав:

- Код/инфра: живой integration-харнесс доставки (по образцу `integration-delivery.sh` старого стенда: запись → вердикт → рестарт-продолжение → отказ-восстановление); сценарий-репетиция обновления PostgreSQL по пути dump/restore (остановить запись → дождаться idle-предупреждений и нулевого `checkpoint_lag_bytes` → остановить cdc-outbox-1 → dump/restore БД → старт с `recovery_mode` → свежий прогон pass); синхронизация stand.md («что появится» → появилось) и AGENTS.md.
- docs: проверить фактический формат значений `headers` (текст, бинарные данные, иное) и зафиксировать контракт заголовков в outbox-table.md — напоминание задачи 4: текущая реализация публикует JSON-объект строковых значений, формат значений при создании задачи не был зафиксирован.

Проверка:

- Харнесс зелёный end-to-end; `check` 48/48 (50/50 с профилем `redundancy`); все тесты решения зелёные.

Выполнено 2026-11-04 (решения владельца при планировании: фаза отказов = PG failover + минорный отказ Kafka + полная потеря Kafka; репетиция обновления — отдельный скрипт; C#-код не менялся — lint OK, тесты решения 257/257). `scripts/test/integration-delivery.sh`: T0 префлайт (контейнеры, слот активен, валидатор ready, детект пары cdc-outbox-2) → T1 запись 300 → T2 вердикт чистый (pass, дублей 0) → T2b живая проба контракта `headers` (INSERT+DELETE одной транзакцией с `trace-id`/`content-type`, чтение topic с `print.headers=true`) → T3 рестарт cdc-outbox-1 в середине 600@30/с → T4a `fail pg` (kill лидера Patroni под нагрузкой, pg2→pg3, timeline 4) → T4b `fail kafka2` → T4c `fail kafka-all` (рост `pg_slot_wal_retained_bytes`, после `restore` backlog дренируется) → T5 Active-Passive при поднятой паре (`fail outbox-1` → лидерство cdc-outbox-2) → финальный `stand.sh check`. Каркас — по образцу харнесса старого стенда (`set -uo pipefail`, накопительные PASS/FAIL + ok/bad, JSON через python3 без jq, wait-бюджеты; runId передаётся глобальной переменной — вызов в `$(...)` терял бы ассерты subshell'ом; инъекции делегированы `stand.sh fail/restore` — source невозможен, case-диспетчер). `scripts/test/upgrade-rehearsal.sh`: T0 базовый 200 pass → T1 тишина (idle-WARN «Простой потока» + `outbox_checkpoint_lag_bytes=0`) → T2 graceful stop → T3 `pg_dump` (факты: `CREATE PUBLICATION` в дамп ЕСТЬ, CONNECT-гранты ролей — нет) → T4 «новый кластер» (`DROP DATABASE WITH (FORCE)` + restore + `apply-bootstrap.sh`) → T5 recovery-старт (слот пересоздан, checkpoint переписан, базовый прогон не пере-доставлен) → T6 контрольный 50 strict-pass → T7 дефолт + check. Контракт `headers` зафиксирован (напоминание задачи 4): колонка varchar, значение — JSON-объект строковых значений; в Kafka имя как есть, значение — UTF-8-байты строки; нестроковые JSON-значения — ошибка формата (политика — задача 13), бинарные данные не поддерживаются (кодирование на стороне пишущего), повторяющиеся имена — последнее значение — `outbox-table.md` «Заголовки», маркер «подлежит проверке приёмкой» снят. Документация синхронизирована: stand.md (раздел «Приёмочные харнессы», «что появится» → появилось, числа check 59/59 базово / 62/62 с профилем), AGENTS.md (задачи 1–11, `verdict.Cdc` всегда null, транзакции/control-группа реализованы), metrics.md (таргет cdc-outbox-2 = профиль redundancy), docker-compose комментарии (pass ⇔ missing=0 && unexpected=0, дубликаты не фейлят), stand.sh help/комментарии (убрана устаревшая пометка о падении валидатора), readme (таблица статусов, сводка). Прогоны живьём: integration-delivery T0–T4b зелёные (чистый 300/300 дублей 0; headers-проба — оба заголовка в topic; рестарт в середине 600/600 pass; PG failover 600/600 pass; kafka2 300/300 дублей 0); T4c доставка 600/600 pass (дублей 0) при подтверждённом росте удерживаемого WAL, НО фаза вскрыла два дефекта задачи 10. Первый — повторное получение лидерства тем же процессом → `InitTransactions` на уже инициализированном продюсере → `KafkaException: Operation not valid in state Ready` → терминальный StopHost; исправлен 2026-11-04 (заказ владельца): продюсер стал scoped-сервисом периода лидерства — Program.cs (регистрации `IProducer`/`CheckpointStore`/`IOutboxStartupChecks` → scoped), `GrantAsync` создаёт скоуп периода лидерства, резолвит свежий продюсер и инициализирует его ДО стартовых проверок (порядок сохранён: fencing-init снимает LSO-стену read_committed-чтения); dispose скоупа после graceful остановки доставки освобождает и продюсер (доставка останавливается раньше dispose — финальный checkpoint успевает зафиксироваться), неудавшийся грант не течёт продюсером. Тесты: FakeProducer стал честным (повторный init — `KafkaException` `Local_State` «Operation not valid in state Ready»; Dispose фиксируется), харнесс LeadershipCoordinatorTests строит продюсер фабрикой на скоуп; юнит-регрессия `Grant_AfterRevoke_RecreatesProducer` (два разных продюсера, у каждого один init, первый Disposed, failovers=2 — на старом коде красный); IT `TransactionalProducerTests` — платформенное допущение на реальном клиенте (double-init того же экземпляра бросает; свежий экземпляр с тем же transactional.id инициализируется, открытая транзакция прежнего отклоняется fencing'ом — та же механика, что «опоздавший commit» гонки финального checkpoint). Тесты решения 259 (166 unit + 28 IT + 65 соседних), lint OK. Живьём: двойной отбой всех брокеров на холостом ходу (90 с > session timeout) — партиция вернулась тому же процессу (`outbox_failovers_total` 1→2 у живого контейнера), подписей `Operation not valid in state Ready` в логах обоих экземпляров 0, вердикт после pass, check 62/62. Второй — гонка финального checkpoint при outage: потеря лидерства → бюджет `FinalCheckpointWaitSeconds` истёк (брокеры лежат) → подтверждена последняя сохранённая позиция → опоздавший асинхронный commit дошёл после возврата брокеров → checkpoint опережает `confirmed_flush` → следующий старт — терминальный reconcile (OutboxStartupChecks) при доказанно доставленном окне (данные уже в Kafka, verdict pass); исправлен 2026-11-04 (заказ владельца): `CheckpointReconciler` больше не считает опережение при живом слоте терминальным — `OutboxStartupChecks` продолжает с позиции слота с WARN «…опережает confirmed_flush слота на N байт — продолжение с позиции слота» (обоснование — в комментарии кода и recommendations.md); checkpoint при этом не переписывается — первый доставленный батч перезапишет запись ключа. Терминальными остаются «слота нет при живом checkpoint» и пустой `confirmed_flush`; Recovery Mode не меняется (повод «checkpoint разошёлся с confirmed_flush» ушёл — штатный старт самовосстанавливается). Тесты: юнит `CheckpointAheadOfFlush_Allowed` (на старом коде красный), IT `CheckpointAheadOfFlush_WithoutRecovery_Continues` (живые PG+Kafka: опережение — обычный старт не бросает, checkpoint не переписан). Обоснование стратегии для всех вариантов расхождения — инварианта «checkpoint в одной транзакции с доставкой», матрица состояний старта, отклонённые альтернативы (терминал на опережении, предотвращение гонки, порог по размеру разрыва, терминализация «слот впереди», автоперезапись вниз), ссылки на документацию PG и перекрёстная проверка Debezium (`offset.mismatch.strategy`) — recommendations.md «Согласование checkpoint-стора с replication slot при старте». Полный прогон харнесса после фикса первого: 89 PASS / 9 FAIL — T0–T4b зелёные, T4c доставка pass 600/600 (дублей 0, WAL у слота 37840 → 445944 байт), но все 9 FAIL — следствие неисправленной гонки: оба экземпляра гибнут терминальным reconcile («Checkpoint 0/58C5EE8 опережает confirmed_flush на 201120 байт»), лидерство не восстанавливается, T5 и финальный check не выполняются; стенд возвращён recovery-процедурой (checkpoint 0/58C5EE8 → 0/5894D48), check 62/62. Репетиция dump/restore: 54/55 — единственный FAIL дефект ассерта харнесса (повторный validate базового runId — HTTP 409 по контракту `fetchFromWriter` «сверка только с последним прогоном Writer»; ассерт убран, инвариант «базовый не пере-доставлен» закрыт T5-проверкой records). Стенд после прогона восстановлен recovery-процедурой, `check` 62/62. Повторный полный прогон после фикса второго (2026-11-05): 97 PASS / 1 FAIL — гонка закрыта живьём: T4c пережит парой (после restore лидерство у cdc-outbox-2, WARN «Checkpoint 0/5B7D0D0 опережает confirmed_flush слота на 212344 байт — продолжение с позиции слота», доставка pass 600/600, дублей 300 = пере-доставленное окно at-least-once; подписей терминального reconcile и «Operation not valid in state Ready» в логах обоих экземпляров 0; recovery-процедура не потребовалась). Единственный FAIL — ассерт T5 `outbox_failovers_total` +1: харнесс T5 законтрактован на лидера cdc-outbox-1, но после пережитого T4c лидером стоит cdc-outbox-2 — kill standby переключения не вызывает (все содержательные проверки T5 зелёные: лидерство активно, verdict pass 900/900 дублей 0, возврат standby; финальный check 62/62); кандидат правки — kill текущего лидера по is_active — на решение владельца. Третий дефект вскрыл повторный прогон приёмки 2026-11-05 (пара пересобрана из HEAD; 96/2): транзиентный обрыв PG-чтения (`Npgsql EndOfStreamException`) при повторном получении лидерства после outage Kafka (окно ~3 с: грант 03:40:10, обрыв 03:40:13, перезапуск блеснул транзиентным соединением, пережившим отказы T4a/T4c) терминировал хост catch-all'ом гранта «Запуск доставки не удался. Хост завершается» — graceful exit 0 при рестарт-политике `no` оставлял экземпляр лежащим до ручного подъёма (в прогоне — до restore T5); доставка не пострадала (партнёр принял лидерство с WARN опережения), но пара молча деградировала до одного экземпляра. Исправлен 2026-11-05 (заказ владельца: хост не останавливается на ошибках, восстановимых в перспективе): `LeadershipCoordinator.GrantAsync` — цикл попыток с backoff `RetryDelaySeconds`/`MaxRetryDelaySeconds` (каждая попытка со свежим DI-скоупом — продюсер периода лидерства не течёт; лидерство удерживается: причина сбоя общая для пары, standby доставил бы не раньше; пауза и проверки — на токене, отменяемом отзывом партиций, poll-поток арбитра не блокируется; fencing при `InitTransactions` распознаётся через `Fencing.IsFenced` → standby без остановки хоста); терминальные нарушения стартовых проверок выделены в исключение `StartupCheckException` (слота нет при живом checkpoint, пустой confirmed_flush — reconcile и recovery-reject, recovery-позиция недоступна, чужой плагин слота, wal_level < logical, отсутствие publication/checkpoint-topic, битая запись checkpoint-стора) — остановка хоста; сетевые ветки проверок (метадата Kafka, чтение checkpoint-стора, `pg_is_in_recovery` — окно PG failover) остались обычными исключениями и потому повторяются. Тесты (на старом коде красные): разовый транзиент стартовых проверок повторяется свежим продюсером, доставка стартует, хост жив (2 продюсера по одному init, первый Disposed, failovers=1, errors=1); персистентный транзиент — попытки продолжаются без Fatal, скоупы освобождаются; fencing при инициализации — standby без Fatal и без повторов; отзыв во время повторов останавливает цикл без блокировки gate. Итог: 264 теста (170 CdcOutbox unit + 29 IT + 65 соседних), lint OK. Полный прогон харнесса после фикса (2026-11-05): 97 PASS / 1 FAIL — T4c пережит парой: после restore оба экземпляра живы («активен ровно один (cdc-outbox-1=0, cdc-outbox-2=1)» — зелёный, до фикса этот ассерт ловил лежащий экземпляр), WARN опережения на новом лидере, verdict pass 600/600 дублей 300, подписей «Хост завершается» и «Operation not valid in state Ready» 0; FAIL прежний — ассерт T5 failover-счётчика (kill зашитого cdc-outbox-1 при лидере cdc-outbox-2), правка T5 — на решение владельца. Четвёртая находка приёмки (2026-11-05) — не продукт, а тест: юнит `Grant_RegistersFailover_InitsProducerBeforeChecks_RunsDelivery` флакал на HEAD (3 из 4 прогонов красные — «should be 1 but was 0» по записям Kafka; на e7f8f17 4/4 зелёные — фикс гранта поднял вероятность гонки): условие ожидания `IsLeading` + запись ридера в канал срабатывает до дренирования канала публикатором, ассерты записей Kafka шли сразу после срабатывания; тест исправлен: ждёт появления самих записей (событие + checkpoint той же транзакцией; поллинг по Count без перечисления — список пишет поток публикатора, перечисление гонялось бы с Add), точные ассерты по топикам — после ожидания; верификация 6/6 × 170/170, полный прогон решения 264/264, lint OK. Повторный полный прогон харнесса на итоговом состоянии (2026-11-05): 97 PASS / 1 FAIL — содержательно та же картина (T4c пережит парой, «активен ровно один (cdc-outbox-1=0, cdc-outbox-2=1)» зелёный, verdict pass 600/600 дублей 300, подписей терминальной остановки в логах 0, финальный check 62/62), единственный FAIL — прежний ассерт T5. Правка T5 (2026-11-05, заказ владельца): kill-цель определяется по is_active (после T4c лидером может быть любой из пары — зашитый kill cdc-outbox-1 ловил standby и переключения не давал); kill именно лидера, лидерство и `outbox_failovers_total` проверяются у выжившего партнёра, возврат убитого — standby; пустой failover-счётчик до переключения трактуется как 0 — счётчик OTel без записей не экспортируется, taker после T3-рестарта, ни разу не лидерствовавший в этом процессе, метрики не имеет (вскрыто ручной прогонкой). Прогоны: 98 PASS / 0 FAIL ×2 (в обоих лидер к T5 — cdc-outbox-1: kill → takeover cdc-outbox-2, failovers 2→3 и 3→4, verdict pass 900/900, возврат standby, финальный check 62/62); ветка «лидер cdc-outbox-2» — ручная прогонка фазы (takeover cdc-outbox-1, failovers «нет метрики»→1, verdict pass 600/600, возврат standby). Первый полностью зелёный прогон приёмки задачи 11.

## Задача 12. Бинарный протокол pgoutput (опция `binary`)

Замена текстового формата значений pgoutput на бинарный (PostgreSQL 14+, стенд 17.2 — доступно): при `START_REPLICATION` в опции плагина передаётся `binary 'true'` — в Npgsql именованный параметр конструктора `PgOutputReplicationOptions(publicationName, protocolVersion, binary: true)`. Значения колонок приходят видом `TupleDataKind.BinaryValue` вместо `TextValue`; главный выигрыш — `bytea` (key/body) идёт сырыми байтами без hex-надбавки (тело не ×2 на проводе), timestamp не требует строкового парсинга.

Состав:

- docs: настройка включения — env `CDCOUTBOX__REPLICATION__BINARY` (`bool`, дефолт `false`) в конфиг-таблицу dependencies.md §2 (docs-first); контракт бинарных представлений колонок outbox-таблицы, определяемых по OID (`RelationMessage.Column.DataTypeId`, тип фиксируется в плане колонок при его построении): `bigint` — int64 big-endian, `integer` — int32 big-endian, `bytea` — сырые байты, `varchar` — UTF-8, `timestamptz` — int64 микросекунд от эпохи 2000-01-01 00:00:00 UTC.
- Код: чтение бинарных значений в `OutboxRowParser` рядом с текстовым путём (hex `\x…` сохраняется для текстового режима), выбор пути по настройке сессии и виду значения; `Kind == Null` обрабатывается одинаково для обоих форматов; `UnchangedToastedValue`/неизвестный OID — явная ошибка декодирования с именем колонки (как сегодня); защита от рассогласования (пришла `BinaryValue` в текстовой сессии и наоборот) уже есть — текст сообщения дополнить указанием фактической настройки.

Проверка:

- Юнит-тесты бинарного декодера на байтовых литералах: int64/int32/bytea/UTF-varchar/timestamptz (включая отрицательные микросекунды до 2000-01-01), NULL, мусорная длина → ошибка с именем колонки; guard-тест аллокаций бинарного пути — как у текстового.
- IT на pg-it: одна и та же вставка читается двумя сессиями (текстовой и бинарной) — декодированные `OutboxMessage` идентичны по всем полям, включая `CreatedAt` до микросекунд; `confirmed_flush` не подтверждается в обоих режимах.
- Стенд: включить env → `stand.sh rebuild cdc-outbox-1` → `load 500` → 500 декодировано по границам транзакций, `errors_total`=0; переключение настройки между рестартами не меняет результат декодирования (после задачи 4 — verdict pass 500/500 в обоих режимах).

Выполнено 2026-11-06. Docs-first: dependencies.md §2 — строка `CDCOUTBOX__REPLICATION__BINARY` (`ReplicationOptions.Binary`, дефолт `false`) и замечание о двух режимах значений pgoutput (текст — `Get<string>`; бинарный — сырые байты через `GetStream()` минуя конвертер типа: типизированный `Get<T>` идёт через конвертер по OID колонки и бинарным видом читается только для совпадающих типов — проверено по исходнику Npgsql 9.0.5, `RowDescriptionMessage.CreateForReplication` строит FieldDescription с TypeOID колонки); контракт бинарных представлений по OID — outbox-table.md «Бинарный формат значений» (таблица OID 20/23/17/1043/1184, защита от рассогласования, безопасность переключения между рестартами). Код: `ReplicationOptions.Binary` (без DataAnnotation — паттерн RecoveryMode); `OutboxColumnPlan` фиксирует OID колонок (`TypeIds`, `BuildColumnPlan(names, dataTypeIds)`), `EnsureBinaryTypes` проверяет контракт один раз на relation до чтения значений (несовпадение — FormatException с именем колонки); `OutboxSlotReader` — `binary: _binary ? true : null` в `PgOutputReplicationOptions` (выключенная настройка не передаётся вовсе — wire текстового режима не меняется), чтение ветками `ReadTextInsertAsync`/`ReadBinaryInsertAsync` (NULL одинаков, чужой Kind — явная ошибка с указанием фактической настройки), сырые байты значения — `GetStream()` + `ReadExactlyAsync`; `OutboxRowParser.ParseBinary` и декодеры `ParseBinaryInt64/Int32/Text/Timestamp` (int64/int32 big-endian через `BinaryPrimitives`, UTF-8, мкс от 2000-01-01 UTC с отрицательными и защитой диапазона DateTime; длина ≠ 8/4 — FormatException с именем колонки; key/body — сырые байты без копии). Тесты: `OutboxRowParserBinaryTests` (13: полная строка на байтовых литералах, отрицательные мкс, NULL required/optional, мусорные длины трёх типов, big-endian/кириллица/пустой bytea, guard аллокаций <400 байт — строка UTF-8 создаётся внутри ParseBinary, бюджет зеркален текстовому, фактический — ~300), `EnsureBinaryTypes` (норма/чужой OID с именем/Ignored-колонка), IT `Read_BinarySession_DecodesSameInsertAsTextSession` (два слота до вставки, текстовая и бинарная сессии декодируют одну транзакцию — списки идентичны по всем полям включая CreatedAt до микросекунд и TransactionEndLsn, confirmed_flush обоих слотов не сдвинулся; ключевой риск «GetStream даёт сырые байты» закрыт живым PG с первого прогона). Итог: lint OK, 280 тестов (185 CdcOutbox unit + 30 IT + 65 соседних; контроль флака юнитов 2×184/184). Стенд: compose-якорь env cdc-outbox — `CDCOUTBOX__REPLICATION__BINARY: "${CDCOUTBOX__REPLICATION__BINARY:-false}"` (паттерн RECOVERYMODE, включение на один up без правки файла). Прогоны живьём: пересборка cdc-outbox-1 (текстовый дефолт) → check 62/62; `CDCOUTBOX__REPLICATION__BINARY=true` (лидерство у бинарного экземпляра) → load 500: лог «декодировано» суммарно 500, `outbox_processor_errors_total` не экспортирован (0 ошибок), verdict pass 500/500 дублей 0; переключение обратно на false между рестартами → load 500 → verdict pass 500/500 дублей 0 (смена формата позицию слота и checkpoint не ломает); финал — пара пересоздана на общем образе (env BINARY=false у обоих), check 62/62.

## Задача 13. Политика для неисправимых ошибок данных (битые `headers`)

Основание (решение о выносе в задачу принято владельцем на задаче 4): сообщение с неразбираемым `headers` (не JSON-объект строковых значений) невозможно исправить — outbox-таблица всегда пустая (INSERT+DELETE одной транзакцией), единственная копия данных находится в WAL за слотом. До решения задачи публикация обрабатывает такие сообщения консервативно (реализация задачи 4): сообщение не публикуется и не пропускается, публикация задерживает его в очереди повторов с паузой между попытками (ERROR + `errors_total`, чтение останавливается backpressure'ом, WAL накапливается) — ни потери, ни тихого пропуска.

Состав:

- docs: решение владельца о судьбе таких сообщений — варианты: пропуск с явным аудитом (журнал/метрика/служебное событие), dead-letter topic, остановка до оператора; влияние на вердикты (пропуск = осознанная потеря — вердиктор обязан это видеть).
- Код: реализация выбранной политики + метрики (имя — по решению, например `outbox_processor_skipped_messages_total`).

Проверка:

- Битое `headers` в потоке → выбранное поведение наблюдается в метриках/журнале, последующие сообщения обрабатываются (или остановка — по решению), вердикт демонстрирует последствия осознанно.

## Задача 14. Старт с ограниченными правами в Kafka (ACL по сертификату)

Требование владельца (зафиксировано на задаче 4): сервис обязан стартовать, когда у принципала клиентского сертификата права только на ограниченный набор топиков (включая `cdc-checkpoints`), без кластерных прав. Сегодня стартовая проверка `CheckpointTopicVerifier` запрашивает метаданные всех топиков (`IAdminClient.GetMetadata` без аргументов) — при таком принципале брокер отвечает `TOPIC_AUTHORIZATION_FAILED`, и подъём хоста валится.

Состав:

- docs: минимальный набор прав принципала для старта и работы (уточнён задачей 10 — полная таблица dependencies.md §2b(в)): DESCRIBE на `cdc-checkpoints` (стартовая проверка), WRITE (+DESCRIBE) на `outbox.events` (транзакционный продюсер событий), WRITE (+DESCRIBE+READ) на `cdc-checkpoints` (checkpoint-записи; READ — контроль-консюмер арбитража и чтец `ReadLatest`), WRITE+DESCRIBE на TRANSACTIONAL_ID `cdc-outbox-<slot>` (InitProducerId/FindCoordinator — fencing), READ (+DESCRIBE) на GROUP `cdc-outbox`; кластерные права не требуются (IdempotentWrite — только для безтранзакционных); права на `__transaction_state` не выдаются — внутренний топик брокеров.
- Код: адресные запросы метаданных по имени топика вместо запроса всех топиков; аудит остальных обращений к Kafka на скрытые требования кластерных прав (обновление метаданных продюсера работает с правами уровня топика).
- Проверка: контур с принципалом ограниченных прав — сервис стартует, стартовые проверки зелёные, доставка работает.

## Задача 15. Общий checkpoint-топик на несколько слотов и сервисов (многопартиционность)

Требование владельца (зафиксировано на задаче 5): топик `cdc-checkpoints` может быть общим для нескольких outbox-сервисов, подключённых к разным слотам, — сервисов по несколько на слот (Active-Passive, задача 10), слотов много; при этом один экземпляр обслуживает строго один слот (решение владельца 2026-11-04: несколько экземпляров на слот — можно, несколько слотов у экземпляра — нельзя). Допущения задачи 5 это не выдерживают: топик создаётся kafka-init с 1 партицией, запись и чтение жёстко идут в партицию 0 (`CheckpointStore`), арбитраж задачи 10 проектируется как «одна партиция = один лидер». Формат записи (ключ — имя слота, очистка журнала по ключу) логически уже допускает много слотов, но партиционная модель и контракт топика — нет.

Состав:

- docs: контракт общего checkpoint-стора: число партиций > 1 (создание kafka-init), детерминированная маршрутизация записи по ключу-слоту (все записи одного слота — в одну партицию: на это опираются чтение последнего checkpoint и арбитраж задачи 10), формат сообщения с учётом нескольких слотов и сервисов на одном топике (версионирование значения), влияние на модель арбитра Active-Passive (партиция = владение слотом; один экземпляр обслуживает строго один слот — решение владельца 2026-11-04, «набор слотов» у партиции исключён).
- Код: `CheckpointStore` — вычисление партиции слота вместо жёсткой нулевой (запись и чтение согласованно); kafka-init — многопартиционный `cdc-checkpoints`; аудит всех мест с допущением «партиция 0» (включая проектируемую задачу 10).
- Проверка: несколько слотов на общем топике — записи каждого слота лежат в своей партиции, `ReadLatest` одного слота не видит чужих, очистка журнала по ключу не смешивает слоты; согласование при старте и доставка работают параллельно для нескольких слотов.

## Задача 16. OTel: трассировка (activity/span) и runtime-метрики

Решение владельца 2026-11-04 (аудит задачи 9): подключить уже заявленные в CdcOutbox.Host пакеты OpenTelemetry — `OpenTelemetry.Extensions.Hosting` 1.11.2 и `OpenTelemetry.Instrumentation.Runtime` 1.11.1 (в зависимостях с задачи 1, но в коде не задействованы: метрики сервиса — prometheus-net; сохранены как задел).

Состав:

- docs: границы трассировки конвейера доставки (activity/span: чтение слота → декодирование → публикация → ACK → checkpoint; протяжка trace-id из `headers` сообщения, если задан), выбор приёмника трейсов и семплирования (нагрузка на проде), сосуществование OTel runtime-метрик с `/metrics` prometheus-net (не ломать существующие имена и label `slot`).
- Код: `AddOpenTelemetry` в Program.cs (runtime-инструментация процесса), Activity-источники по подсистемам (`OutboxSlotReader`, `OutboxPublisher`, `CheckpointStore`), контекст span'ов — LSN/батч.
- Проверка: `/metrics` не деградирует (имена/labels неизменны), трейсы конвейера видны в приёмнике со span-контекстом, накладные расходы измерены.

## Задача 17. Комментарии кода: единый стиль

Правила выработаны ревью задачи 10 (2026-11-04) и применены к её новому коду; задача распространяет их на остальной код и тесты.

Правила:

- XML-doc метода/типа — краткое описание сути («что это»), без пересказа «что происходит»: детали либо очевидны из кода, либо — однострочный inline-комментарий у неочевидного места.
- В коде `CdcOutbox.*` нет ссылок на файлы документации (`docs/*.md`) — ни в runtime-строках, ни в комментариях (для Writer/KafkaValidator исторические ссылки оставлены — решение владельца «непринципиально»).
- Терминология — русские слова либо общепринятые латинские термины (fencing, checkpoint, standby); русификации запрещены: «ревок» → «отзыв партиций», «отфенсен» → «сработал fencing», «терм» → «период лидерства», «компакция» → «очистка журнала по ключу (compaction)», «абортированная» → «прерванная».
- Магические числа — в именованные константы; дубликаты тест-хелперов — в общие (образец — `TestAssert.AssertThrowsAsync`).

Состав:

- Код: класс-доки задачи 10 уже ужаты при ревью (`LeadershipCoordinator`, `LeadershipArbiter`, `OutboxPublisher`, `CheckpointStore`, `Fencing`, `OutboxProcessor`, `OutboxStartupChecks`, `OutboxProducerConfig`); остались исторические (задачи 2–8): `OutboxSlotReader`, `OutboxSlotProvisioner`, `ReplicationProgress`, `IdleActivityTracker` и др. Поведение задокументировано в docs/, класс-док — «что это»; неочевидные детали — inline.
- Тесты: упростить толерантный паттерн `Cancellation_DoesNotRegisterError` (split→Where→ShouldNotContain) до `ShouldNotContain("outbox_processor_errors_total{")` (вынос `WaitForAsync` в общий хелпер и `AssertThrowsAsync` уже сделаны при ревью задачи 10).
- Проверка: lint и все тесты решения зелёные; diff — только комментарии/константы/хелперы, поведение не меняется.
