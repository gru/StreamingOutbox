# dependencies.md — стек, схема и канонические SQL-примеры

Единый источник правды о версиях и схеме для сервиса `streaming-outbox` (чтение outbox-сообщений из PostgreSQL через слот логической репликации, публикация в Kafka).

Версии взяты из проверенной реализации исследовательского стенда `streaming-polling-outbox` как стартовая точка — дальше правим здесь, затем в коде/инфраструктуре.

---

## 1. Инфраструктурные зависимости

| Компонент | Версия | Образ / источник |
|---|---|---|
| PostgreSQL | **17.2** | `postgres:17.2` — базовый образ собственной сборки Patroni |
| Patroni | **4.1.5** | собственная сборка: `pip3 install "patroni[etcd3]==4.1.5" "psycopg2-binary==2.9.10"` поверх `postgres:17.2` (нативный arm64, прозрачный patroni.yml) |
| etcd (DCS) | **3.5.17** | `gcr.io/etcd-development/etcd:v3.5.17`, кластер ×3 |
| HAProxy | **3.0.30** | `haproxy:3.0.30-alpine` — маршрутизация на текущий primary (5432) |
| Apache Kafka (KRaft, без ZooKeeper) | **3.9.0** | `apache/kafka:3.9.0`, кластер ×3 |
| Prometheus | **2.54.1** | `prom/prometheus:v2.54.1` |
| postgres-exporter | **0.15.0** | `prometheuscommunity/postgres-exporter:v0.15.0` (имя с **дефисом**; вариант с подчёркиванием не существует) |
| kafka-exporter | **1.8.0** | `danielqsj/kafka-exporter:v1.8.0` |
| Оркестрация | Docker Compose | всё строго в Docker Compose |

Замечания к инфраструктуре:

- **Kafka-топики** (создаются явно kafka-init; `auto.create.topics.enable=false` — сервисы сами топики не создают): `outbox.events` — события доставки (6 партиций, replication factor 3, `min.insync.replicas=2`); `cdc-checkpoints` — служебный checkpoint-стор outbox-сервиса (1 партиция, replication factor 3, `cleanup.policy=compact`, `min.insync.replicas=2`; recommendations.md «Checkpoint Store в Kafka» и «Балансировка CDC-экземпляров»).
- Все подключения к PostgreSQL — **напрямую через HAProxy** (5432): и сервис стриминговой репликации (репликационный протокол), и Writer (INSERT-нагрузка). PgBouncer в стенде не используется (решение владельца).
- Мониторинг стенда: Prometheus + postgres-exporter + kafka-exporter. cAdvisor **не используется** (решение владельца при переносе стенда): на Docker Desktop с containerd image store он видит только VM-root, per-container серий нет. postgres-exporter подключается от роли `sys_monitor` (`pg_monitor` с `INHERIT TRUE`, `CONNECT` только на БД `outbox`, без REPLICATION) напрямую через HAProxy; кастомные запросы — `infrastructure/prometheus/postgres-exporter-queries.yaml` (WAL, слоты, репликация). Состав стенда, порты и команды — `docs/stand.md`.
- Patroni permanent replication slots для logical-слотов **не используются** — failover-слоты синхронизирует сам PostgreSQL 17.

---

## 2. Зависимости сервиса (стриминговая репликация)

Версии зафиксированы решением владельца (2026-10-21): Npgsql 9.0, OpenTelemetry 1.11, prometheus-net 5.0; Kafka-стек — KafkaFlow 3.0 + Confluent.Kafka 2.3 (явный пиннинг) + librdkafka.redist 2.3. Уточнение того же дня: интеграционный пакет — `KafkaFlow.Extensions.Hosting` 3.0 вместо исходного пина `KafkaFlow.Microsoft.DependencyInjection` 2.5 (см. строку в таблице). Точные версии патчей: KafkaFlow-тройка **3.0.10**, Npgsql **9.0.x**, prometheus-net.AspNetCore **5.0.2**, Confluent.Kafka **2.3.0**, librdkafka.redist **2.3.0**; патчи OTel-линии 1.11 зафиксированы при создании скелета сервиса: OpenTelemetry.Extensions.Hosting **1.11.2**, OpenTelemetry.Instrumentation.Runtime **1.11.1** (последние стабильные линии 1.11). Стартовые версии исследовательского стенда (Npgsql 8.0.9, OTel 1.9.0, экспорт метрик через `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0-beta.2, KafkaFlow 4.2.0 + `KafkaFlow.Extensions.Hosting`, Confluent.Kafka 2.8.0 транзитивно) заменены.

**Отказ от KafkaFlow (решение владельца 2026-10-22):** outbox-сервис использует только Confluent.Kafka напрямую. Причина: фенсинг лидера на служебном topic `cdc-checkpoints` требует транзакционного продюсера с постоянным `transactional.id` (recommendations.md «Балансировка CDC-экземпляров через Kafka»), а KafkaFlow не предоставляет доступ к Kafka-транзакциям ни в одной линии — feature request [Farfetch/kafkaflow#566](https://github.com/Farfetch/kafkaflow/issues/566) открыт с 2024-04-29 без реакции мейнтейнеров; проверено рефлексией сборки 3.0.10: в публичном API нет ни одного транзакционного члена (`IMessageProducer` — только `Produce`/`ProduceAsync`), обёрнутый `Confluent.Kafka.IProducer` наружу не экспортируется. Confluent.Kafka 2.3.0 (проверено той же рефлексией) содержит полный транзакционный набор: `InitTransactions` / `BeginTransaction` / `CommitTransaction` / `AbortTransaction` / `SendOffsetsToTransaction`. Пакеты KafkaFlow-тройки удалены из скелета `CdcOutbox.Core`. KafkaFlow 3.0.10 остаётся только в KafkaValidator (консюмер тестового контура, транзакции там не нужны). Явный пин `librdkafka.redist` при этом убран: пакет 2.3.0 тянется транзитивно от Confluent.Kafka (решение владельца 2026-10-22).

| Пакет | Версия | Назначение |
|---|---|---|
| .NET | **8.0** (`net8.0`) строго | ASP.NET Core 8; сборка/тесты в `mcr.microsoft.com/dotnet/sdk:8.0` |
| Npgsql | **9.0** | Logical Replication API: `PgOutputReplicationSlot` + `StartReplication(...)` — pgoutput proto v1, типизированные сообщения; поддерживает `net8.0` |
| AspNetCore.HealthChecks.Npgsql | **9.0** | чек готовности `/ready` Writer'а: `SELECT 1` через общий с записью `NpgsqlDataSource` (фабрика чека резолвится лениво — пул один), таймаут регистрации 3 с; 9.0.0 таргетирует net8.0 (проверено) |
| Confluent.Kafka | **2.3** | единственный Kafka-клиент outbox-сервиса: идемпотентный продюсер событий (`acks=all`), транзакционный продюсер checkpoint'ов `cdc-checkpoints`, консюмер control-группы (выбор активного экземпляра); см. «Отказ от KafkaFlow» выше |
| librdkafka.redist | **2.3** | нативные бинарники librdkafka (в т.ч. linux/arm64); приходит **транзитивно** от Confluent.Kafka 2.3 — явного пина в проектах нет (решение владельца 2026-10-22) |
| OpenTelemetry.Extensions.Hosting | **1.11** | метрики OTel (Meter/Counter/Gauge) |
| OpenTelemetry.Instrumentation.Runtime | **1.11** | runtime-метрики (.NET) |
| prometheus-net (`prometheus-net.AspNetCore`) | **5.0** | экспорт `/metrics` (pull, Prometheus) — вместо `OpenTelemetry.Exporter.Prometheus.AspNetCore`; линия 5.x (текущая у автора библиотеки — 8.x), выбрана решением владельца |
| `prometheus-net` (клиент) | **5.0** | `Metrics.CreateGauge/CreateCounter/CreateHistogram` для класса `OutboxMetrics` в `CdcOutbox.Core` (5.0.2; тот же пакет транзитивно тянет `prometheus-net.AspNetCore` в Host — версии линий совпадают) |
| `Microsoft.Extensions.Hosting.Abstractions` | **8.0** | `BackgroundService`/`IHostedService` каркаса процесса в `CdcOutbox.Core` (8.0.1; в Host входит в shared framework 8) |
| Shouldly | **4.3.0** | ассерты в тестах — все четыре тестовых проекта решения (решение владельца 2026-10-22; `[Fact]`/`[Theory]` остаются от xUnit) |

Замечания к сервису:

- Тестовый контур — Writer и KafkaValidator — самостоятельные сервисы: **общего кода с outbox-сервисом и между собой не имеют**. Зависимости Writer — Npgsql 9.0.x + AspNetCore.HealthChecks.Npgsql 9.0.0 (health checks `/ready`); KafkaValidator — KafkaFlow-стек 3.0.10; метрики обоих — через prometheus-net 5.0 (`Metrics.Create*`, без OTel). Пакеты OpenTelemetry (1.11) закреплены за outbox-сервисом (стриминговый процессор) — подключены в его скелете (`CdcOutbox.Host`).
- Скелет outbox-сервиса (имена — решение владельца): `src/CdcOutbox.Host` (хост, Web SDK: `/health`, `/ready`, `/metrics`; пакеты хостинга — AspNetCore.HealthChecks.Npgsql 9.0.0, OTel 1.11.2/1.11.1, prometheus-net.AspNetCore 5.0.2) и `src/CdcOutbox.Core` (библиотека под всю логику стриминга: Npgsql 9.0.5 + Confluent.Kafka 2.3.0 (librdkafka.redist 2.3.0 — транзитивно, без явного пина) — единственный Kafka-клиент, KafkaFlow исключён — + prometheus-net 5.0.2 + Microsoft.Extensions.Hosting.Abstractions 8.0.1) + `tests/CdcOutbox.UnitTests` (ссылается на оба). Dockerfile хоста собирается с контекстом `./src` (обе csproj в restore-слое; `src/.dockerignore` в корне контекста). env-префикс конфигурации — `CDCOUTBOX__` (одним словом: одинарные подчёркивания env-провайдер не заменяет на `:` — грабля `CDC_POLLING__` старого стенда); конфигурационные параметры — подраздел «Конфигурация outbox-сервиса» ниже. Compose (зафиксировано, docs/stand.md): **два экземпляра `cdc-outbox-1`/`cdc-outbox-2` — Active-Passive резервирование** (арбитр — Kafka Consumer Group на служебном topic `cdc-checkpoints`: активный один — читает слот и публикует, второй в standby; переключение — rebalance → fencing transactional producer → checkpoint из `cdc-checkpoints` → ожидание освобождения слота; эксклюзивность слота — последняя линия защиты; алгоритм — docs/recommendations.md «Балансировка CDC-экземпляров через Kafka», модель стенда — docs/stand.md), общий образ `streaming-outbox/cdc-outbox:net8.0`, host-порты 18084/18085; **cdc-outbox-1 — always-on, cdc-outbox-2 закрыт compose-профилем `redundancy`** (второй экземпляр включим вместе с реализацией резервирования — решение владельца: `docker compose --profile redundancy up -d --no-deps cdc-outbox-2` + возврат таргета в prometheus.yml + рестарт prometheus; wipe.sh уже чистит его с флагом профиля).
- Health checks: Writer и KafkaValidator — встроенный фреймворк ASP.NET Core (`AddHealthChecks` + `MapHealthChecks`; `Microsoft.Extensions.Diagnostics.HealthChecks` входит в shared framework 8, отдельного пина нет) c ResponseWriter-контрактом `{"status":…}`; `/health` — liveness без чеков (`Predicate = _ => false`). cdc-outbox (решение владельца): `/health` — liveness без чеков, `/ready` — postgres-чек из AspNetCore.HealthChecks.Npgsql (`SELECT 1` от `sys_replication` через общий connection string, таймаут 3 с) с **дефолтным форматтером фреймворка** (plain-text агрегированный статус: `Healthy`/`Unhealthy`; JSON-писателя в коробке нет — сниппет из документации MS остаётся кастомным кодом). Чек состояния процессора сознательно не вводится: в Active-Passive standby-экземпляр не держит цикл доставки, но экземпляр здоров — готовность ≠ лидерство (активность отслеживает метрика `outbox_processor_is_active`, metrics.md), а при падении цикла .NET 8 останавливает хост (`BackgroundServiceExceptionBehavior.StopHost`) и liveness гаснет сам. Экосистемный AspNetCore.HealthChecks.Kafka сознательно не используется: его проверка — produce-раундтрип (пишет тестовое сообщение в topic при каждом пробе), для консюмера это side-effect в кластере и путь продюсера, а не консюмера. В тестах — Microsoft.Extensions.Diagnostics.HealthChecks 8.0.11 (test-only), ассерты — Shouldly 4.3.0.
- pgoutput в Npgsql (актуально и для 8.x, и для 9.x) отдаётся **только типизированными сообщениями** (сырого `XLogDataMessage` для pgoutput в публичном API нет).
- Значения колонок pgoutput приходят текстом и размечаются как `text`: читать `Get<string>()` и парсить самому (`Guid.TryParse`, парсинг timestamp) — `Get<Guid>()`/`Get<DateTime>()` кидают `InvalidCastException`.
- Подтверждение позиции — `SetReplicationStatus(TransactionEndLsn)` + `SendStatusUpdate()` **только после Kafka delivery** (at-least-once).
- Публикация в Kafka — напрямую клиентом Confluent.Kafka из внутреннего представления `OutboxMessage` (`outbox-table.md`); обязательный интерфейс-контракт транспорта (`IOutboxMessagesProcessorTransport`) сознательно не вводится (решение владельца: абстракция стесняет реализацию — батчинг, delivery-репорты, транзакционный продюсер checkpoint'ов).

### Конфигурация outbox-сервиса (env `CDCOUTBOX__*`)

env-префикс — `CDCOUTBOX__` одним словом (двойное подчёркивание env-провайдер заменяет на `:` — грабля `CDC_POLLING__` старого стенда); параметры биндятся в секции `CdcOutbox:*` options-классов `CdcOutbox.Core`. Подключение к PostgreSQL задаётся **единой строкой подключения** (решение владельца): любые параметры Npgsql — SSL, keepalives, `Pooling`, `Application Name` (наблюдаемость в `pg_stat_replication`) и т.п. — задаются строкой без расширения конфигурации и кода; пароль живёт в самой строке (compose подставляет `${SYS_REPLICATION_PASSWORD}` интерполяцией).

| env | Свойство | Дефолт | Назначение |
|---|---|---|---|
| `CDCOUTBOX__POSTGRES__CONNECTIONSTRING` | `PostgresOptions.ConnectionString` | — (обязательно) | строка подключения Npgsql: маршрут к primary, база, роль, пароль и любые параметры драйвера — одной строкой (в стенде compose подставляет `Password=${SYS_REPLICATION_PASSWORD}`) |
| `CDCOUTBOX__REPLICATION__SLOTNAME` | `ReplicationOptions.SlotName` | — (обязательно) | failover-слот логической репликации (§5); имя — стабильный идентификатор checkpoint и `transactional.id` |
| `CDCOUTBOX__REPLICATION__PUBLICATIONNAME` | `ReplicationOptions.PublicationName` | — (обязательно) | publication (§4) |
| `CDCOUTBOX__STORAGE__SCHEMA` | `OutboxStorageOptions.Schema` | `outbox` | схема outbox-таблицы (outbox-table.md) |
| `CDCOUTBOX__STORAGE__TABLE` | `OutboxStorageOptions.Table` | `messages` | имя outbox-таблицы |
| `CDCOUTBOX__KAFKA__BOOTSTRAPSERVERS` | `KafkaOptions.BootstrapServers` | — (обязательно) | список bootstrap-серверов Kafka |
| `CDCOUTBOX__KAFKA__ACKS` | `KafkaOptions.Acks` | `all` | подтверждение записи всеми ISR |
| `CDCOUTBOX__KAFKA__ENABLEIDEMPOTENCE` | `KafkaOptions.EnableIdempotence` | `true` | идемпотентный продюсер |
| `CDCOUTBOX__KAFKA__BATCHNUMMESSAGES` | `KafkaOptions.BatchNumMessages` | `10000` | батчинг продюсера (сообщений на батч) |
| `CDCOUTBOX__KAFKA__LINGERMS` | `KafkaOptions.LingerMs` | `5` | задержка накопления батча, мс |
| `CDCOUTBOX__KAFKA__DELIVERYTIMEOUTMS` | `KafkaOptions.DeliveryTimeoutMs` | `120000` | таймаут доставки (включая ретраи), мс |
| `CDCOUTBOX__KAFKA__MESSAGESENDMAXRETRIES` | `KafkaOptions.MessageSendMaxRetries` | `2147483647` | лимит ретраев отправки (фактически ограничен delivery timeout) |
| `CDCOUTBOX__KAFKA__MESSAGEMAXBYTES` | `KafkaOptions.MessageMaxBytes` | `1000000` | максимальный размер сообщения, байт |
| `CDCOUTBOX__KAFKA__CHECKPOINTTOPIC` | `KafkaOptions.CheckpointTopic` | `cdc-checkpoints` | checkpoint-стор (recommendations.md «Checkpoint Store в Kafka») |
| `CDCOUTBOX__PROCESSOR__QUEUECAPACITY` | `ProcessorOptions.QueueCapacity` | `2048` | bounded-очередь чтение→публикация (backpressure) |
| `CDCOUTBOX__PROCESSOR__BATCHSIZE` | `ProcessorOptions.BatchSize` | `500` | пачка публикации |
| `CDCOUTBOX__PROCESSOR__CHECKPOINTINTERVALSECONDS` | `ProcessorOptions.CheckpointIntervalSeconds` | `5` | интервал периодического сохранения checkpoint |
| `CDCOUTBOX__PROCESSOR__IDLETIMEOUTSECONDS` | `ProcessorOptions.IdleTimeoutSeconds` | `30` | порог простоя потока для idle-предупреждения (WARN) |

Правила:

- **Дефолты не содержат значений контура разработки** (решение владельца): подключение PG (`ConnectionString`), `BootstrapServers` и имена слота/publication обязательны и задаются env (в стенде — compose); остальные дефолты — контракты сервиса (схема/таблица outbox, checkpoint-topic) и параметры клиента/цикла (совпадают с дефолтами librdkafka или зафиксированы архитектурно).
- Валидация — стандартный конвейер options: DataAnnotations (`[Required]`/`[Range]`, сообщения указывают путь секции и свойство) + `.ValidateDataAnnotations().ValidateOnStart()` при регистрации (`AddOptions<T>().Bind(section)` — эквивалент `Configure<T>(configuration)`, но с цепочкой валидации) — ошибка конфигурации валит старт сервиса, контейнер перезапускается рестарт-политикой. Пути секций — константы `ConfigurationSection` в самих options-классах. Строка подключения дополнительно проверяется разбором `NpgsqlConnectionStringBuilder` (неизвестные ключевые слова Npgsql отвергает сам) с контролем непустых Host/Username/Database.
- Kafka-параметры попадают в `ProducerConfig` напрямую при создании продюсера (задача 4 implementation-plan.md); `transactional.id` checkpoint-продюсера и `group.id` арбитра Active-Passive добавят задачи 5/10 — отдельные поля появятся вместе с ними.

---

## 3. Outbox-таблица: `outbox.messages`

Полное описание (назначение, особенности, связь с метриками, открытые вопросы) — `outbox-table.md`. Канонический DDL:

```sql
CREATE SCHEMA IF NOT EXISTS outbox;

CREATE TABLE IF NOT EXISTS outbox.messages (
  id               bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic            varchar(255) NOT NULL,
  key              bytea        NOT NULL,
  body             bytea        NOT NULL,
  headers          varchar      NOT NULL,
  partition        integer,
  producer_key     varchar(255),
  created_at       timestamptz  NOT NULL DEFAULT now(),
  outbox_partition integer      NOT NULL DEFAULT 0
);
```

Свойства схемы:

- `id` — `bigint` identity: генерируется базой автоматически (не клиентом), первичный ключ.
- `key` и `body` — `bytea`: ключ и тело передаются в брокер как есть; формат сериализации определяется приложением и таблицей не навязывается.
- `partition` — целевая партиция **брокера** (`NULL` — брокер выбирает сам); `producer_key` — выбор продюсера (кластера), через который выполняется публикация, в текущей реализации **не используется**; оба поля необязательные.
- `outbox_partition` — внутренняя партиция **Outbox** (разделение сообщений между экземплярами процессора; по умолчанию `0`); в текущей реализации **не используется**; label `partition` метрик процессора (`metrics.md`) соответствует этому полю.
- Индексы, алгоритм выборки и политика очистки структурой не фиксируются — определяются реализацией процессора (см. `outbox-table.md`).
- Схема и имя таблицы конфигурируются в сервисе через `OutboxStorageOptions` (контракт — `outbox-table.md`); каноническое имя `outbox.messages` — значение по умолчанию.

### Запись тестовой нагрузки (Writer)

Writer (генератор нагрузки) пишет INSERT+DELETE **в одной транзакции** — таблица всегда пустая, сообщение живёт в WAL (`publish='insert'` — слот несёт 1 change на сообщение). Идентичность сообщения для валидации — `id` таблицы, поэтому id резервируется у identity-последовательности **до** сборки body и вставляется явно:

```sql
-- резерв id до сборки body (id попадает в тело сообщения)
SELECT nextval(pg_get_serial_sequence('outbox.messages', 'id')) FROM generate_series(1, $1);

-- батч: 6 позиционных параметров на строку; OVERRIDING SYSTEM VALUE — id задаётся явно
-- в обход GENERATED ALWAYS; key = UTF8(id), headers = '{}'
INSERT INTO outbox.messages (id, topic, key, body, headers, created_at)
OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3::bytea, $4::bytea, $5, $6), ...;

-- удаление тех же строк в той же транзакции (таблица не растёт)
DELETE FROM outbox.messages WHERE id = ANY($1);
```

Ретрай батча с теми же id: `unique_violation` (23505) по PK = прошлый commit прошёл — разрешение неоднозначного коммита. Формат тестового body — `outbox-table.md`. Гранты `sys_writer`: `CONNECT` на БД, `USAGE` на схему `outbox`, `INSERT, DELETE` на таблицу, колоночный `SELECT (id)` (колонка читается в WHERE DELETE), `USAGE ON SEQUENCE` identity-последовательности таблицы.

---

## 4. Publication — канонический пример

pgoutput эмитит изменения **только таблиц из publication**, поэтому publication содержит outbox-таблицу. `publish = 'insert'` — для доставки значимы только INSERT (сообщение появляется в таблице одной вставкой); очистка отправленных строк (DELETE) в доставке не участвует и из эмиссии исключена — слот несёт 1 change на сообщение.

Каноническая форма:

```sql
CREATE PUBLICATION outbox_publication
  FOR TABLE outbox.messages
  WITH (publish = 'insert');
```

Идемпотентная форма (для init-скриптов; `SET (publish=...)` не трогает состав таблиц и безопасен при повторах):

```sql
SELECT 'CREATE PUBLICATION outbox_publication'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec

SELECT format('ALTER PUBLICATION outbox_publication ADD TABLE %I.%I', 'outbox', 'messages')
WHERE NOT EXISTS (
  SELECT FROM pg_publication_tables
  WHERE pubname = 'outbox_publication' AND schemaname = 'outbox' AND tablename = 'messages'
) \gexec

ALTER PUBLICATION outbox_publication SET (publish = 'insert');
```

Publication указывается в опциях **чтения** слота (параметры `START_REPLICATION` / peek), а не при создании слота. Publication не фильтрует logical messages — не считать её границей доступа.

Кто создаёт publication (решение по результатам эксперимента на стенде): инициализация стенда — `infrastructure/postgres/init/03-outbox-publication.sql` от postgres, а не outbox-сервис. Причина: `CREATE PUBLICATION` требует привилегию `CREATE` на текущую БД, а добавление таблицы — владения таблицей; у `sys_replication` нет ни того, ни другого (минимальные привилегии, SUPERUSER прикладным ролям запрещён) — попытка `CREATE PUBLICATION … FOR TABLE outbox.messages` от роли падает `permission denied for database outbox`. Гранты `sys_replication` выдаёт та же инициализация: `CONNECT` на БД — 01-appdb-objects.sql, `USAGE` на схему `outbox` — 03; `SELECT` на таблицу не требуется — стриминг pgoutput читает сообщения через слот (проверено исследовательским стендом `streaming-polling-outbox`: доставка 525k сообщений ролью без табличных грантов), каталоги `pg_publication`/`pg_replication_slots` роли видны. Сервис при старте только проверяет конфигурацию publication (имя, `publish='insert'`, состав таблиц) — это соответствует и части I recommendations.md §2 («миграции должны создавать … publication»). Failover-слот (§5), в отличие от publication, создаёт сам сервис: `pg_create_logical_replication_slot` требует только атрибут `REPLICATION` (есть у роли; проверено тем же экспериментом — probe-слот создавался и удалялся от неё без ошибок).

---

## 5. Failover-слот репликации — канонический пример

```sql
-- Слот — объект конкретной БД: создавать в прикладной БД (здесь — outbox).
SELECT pg_create_logical_replication_slot(
  slot_name => 'outbox_slot',
  plugin    => 'pgoutput',
  failover  => true
);
```

Идемпотентная форма:

```sql
SELECT format('SELECT pg_create_logical_replication_slot(slot_name => %L, plugin => %L, failover => true)',
              'outbox_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'outbox_slot') \gexec
```

Обязательные условия корректности:

- `failover => true` — включает нативную синхронизацию слота на standby силами PostgreSQL 17 (slotsync worker). Требует на стендбаях: `sync_replication_slots = on` и `hot_standby_feedback = on` (задаются в patroni.yml / параметрах кластера через DCS).
- **Слот в прикладной БД**: PG17 slotsync worker синхронизирует слоты только БД из `dbname` в `primary_conninfo` стендбая — прикладная БД должна быть именно там (в Patroni — официальная опция `postgresql.database`).
- `synchronized_standby_slots` на primary принимает **только физические слоты**; собирается из фактических физических слотов и обновляется после каждой смены primary (`ALTER SYSTEM SET synchronized_standby_slots = '...'` + `pg_reload_conf()`):

  ```sql
  SELECT format('ALTER SYSTEM SET synchronized_standby_slots = %L',
                (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
                 FROM pg_replication_slots WHERE slot_type = 'physical')) \gexec
  SELECT pg_reload_conf();
  ```

- Плагин — строго `pgoutput`; версия протокола при подключении — `proto_version = '1'`.
- Непотребляемый слот со временем перестаёт синхронизироваться (catalog_xmin стендбая обогнал слот) и при promotion **не сохраняется** — до появления потребителя продвигать слот вручную; реальный сервис делает это сам после Kafka delivery.
