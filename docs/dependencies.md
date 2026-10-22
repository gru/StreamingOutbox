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

- **Kafka-топик** `outbox.events`: 6 партиций, replication factor 3, `min.insync.replicas=2`; `auto.create.topics.enable=false` — топик создаётся явно отдельной init-командой, сервис сам его не создаёт.
- Все подключения к PostgreSQL — **напрямую через HAProxy** (5432): и сервис стриминговой репликации (репликационный протокол), и Writer (INSERT-нагрузка). PgBouncer в стенде не используется (решение владельца).
- Мониторинг стенда: Prometheus + postgres-exporter + kafka-exporter. cAdvisor **не используется** (решение владельца при переносе стенда): на Docker Desktop с containerd image store он видит только VM-root, per-container серий нет. postgres-exporter подключается от роли `sys_monitor` (`pg_monitor` с `INHERIT TRUE`, `CONNECT` только на БД `outbox`, без REPLICATION) напрямую через HAProxy; кастомные запросы — `infrastructure/prometheus/postgres-exporter-queries.yaml` (WAL, слоты, репликация). Состав стенда, порты и команды — `docs/stand.md`.
- Patroni permanent replication slots для logical-слотов **не используются** — failover-слоты синхронизирует сам PostgreSQL 17.

---

## 2. Зависимости сервиса (стриминговая репликация)

Версии зафиксированы решением владельца (2026-10-21): Npgsql 9.0, OpenTelemetry 1.11, prometheus-net 5.0; Kafka-стек — KafkaFlow 3.0 + Confluent.Kafka 2.3 (явный пиннинг) + librdkafka.redist 2.3. Уточнение того же дня: интеграционный пакет — `KafkaFlow.Extensions.Hosting` 3.0 вместо исходного пина `KafkaFlow.Microsoft.DependencyInjection` 2.5 (см. строку в таблице). Точные версии патчей: KafkaFlow-тройка **3.0.10**, Npgsql **9.0.x**, prometheus-net.AspNetCore **5.0.2**, Confluent.Kafka **2.3.0**, librdkafka.redist **2.3.0**; патчи OTel-линии 1.11 зафиксированы при создании скелета сервиса: OpenTelemetry.Extensions.Hosting **1.11.2**, OpenTelemetry.Instrumentation.Runtime **1.11.1** (последние стабильные линии 1.11). Стартовые версии исследовательского стенда (Npgsql 8.0.9, OTel 1.9.0, экспорт метрик через `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0-beta.2, KafkaFlow 4.2.0 + `KafkaFlow.Extensions.Hosting`, Confluent.Kafka 2.8.0 транзитивно) заменены.

| Пакет | Версия | Назначение |
|---|---|---|
| .NET | **8.0** (`net8.0`) строго | ASP.NET Core 8; сборка/тесты в `mcr.microsoft.com/dotnet/sdk:8.0` |
| Npgsql | **9.0** | Logical Replication API: `PgOutputReplicationSlot` + `StartReplication(...)` — pgoutput proto v1, типизированные сообщения; поддерживает `net8.0` |
| AspNetCore.HealthChecks.Npgsql | **9.0** | чек готовности `/ready` Writer'а: `SELECT 1` через общий с записью `NpgsqlDataSource` (фабрика чека резолвится лениво — пул один), таймаут регистрации 3 с; 9.0.0 таргетирует net8.0 (проверено) |
| KafkaFlow | **3.0** | основной API публикации в Kafka (`acks=all`, идемпотентный producer) |
| KafkaFlow.Extensions.Hosting | **3.0** | интеграция KafkaFlow с generic host: `AddKafkaFlowHostedService` (старт/стоп шины вместе с хостом). Уточнение к исходному пину `KafkaFlow.Microsoft.DependencyInjection` 2.5: тот пакет на любой линии (проверено по исходникам тегов 2.5.1 и 3.0.0) содержит только низкоуровневый `AddKafka` без хостинга и для консюмера непригоден; пакеты KafkaFlow выходят синхронными линиями — берётся одна линия 3.0.10 |
| KafkaFlow.LogHandler.Console | **3.0** | логирование KafkaFlow (в решении владельца не указана — выровнена по линии ядра 3.0) |
| Confluent.Kafka | **2.3** | носитель `ProducerConfig`/нативного клиента; пиннится явно (в стенде шла транзитивно); напрямую не использовать — основной API KafkaFlow |
| librdkafka.redist | **2.3** | нативные бинарники librdkafka (в т.ч. linux/arm64); пиннится в паре с Confluent.Kafka 2.3 для детерминизма рантайма |
| OpenTelemetry.Extensions.Hosting | **1.11** | метрики OTel (Meter/Counter/Gauge) |
| OpenTelemetry.Instrumentation.Runtime | **1.11** | runtime-метрики (.NET) |
| prometheus-net (`prometheus-net.AspNetCore`) | **5.0** | экспорт `/metrics` (pull, Prometheus) — вместо `OpenTelemetry.Exporter.Prometheus.AspNetCore`; линия 5.x (текущая у автора библиотеки — 8.x), выбрана решением владельца |

Замечания к сервису:

- Тестовый контур — Writer и KafkaValidator — самостоятельные сервисы: **общего кода с outbox-сервисом и между собой не имеют**. Зависимости Writer — Npgsql 9.0.x + AspNetCore.HealthChecks.Npgsql 9.0.0 (health checks `/ready`); KafkaValidator — KafkaFlow-стек 3.0.10; метрики обоих — через prometheus-net 5.0 (`Metrics.Create*`, без OTel). Пакеты OpenTelemetry (1.11) закреплены за outbox-сервисом (стриминговый процессор) — подключены в его скелете (`CdcOutbox.Host`).
- Скелет outbox-сервиса (имена — решение владельца; кода нет, зависимости расставлены): `src/CdcOutbox.Host` (хост, Web SDK) + `src/CdcOutbox.Core` (библиотека под всю логику стриминга) + `tests/CdcOutbox.UnitTests` (ссылается на оба). Раскладка пакетов: Core — Npgsql 9.0.5, KafkaFlow-тройка 3.0.10, явные пины Confluent.Kafka/librdkafka.redist 2.3.0; Host — AspNetCore.HealthChecks.Npgsql 9.0.0, OpenTelemetry.Extensions.Hosting 1.11.2, OpenTelemetry.Instrumentation.Runtime 1.11.1, prometheus-net.AspNetCore 5.0.2, ссылка на Core. Dockerfile хоста собирается с контекстом `./src` (обе csproj в restore-слое; `src/.dockerignore` в корне контекста). env-префикс конфигурации — `CDCOUTBOX__` (одним словом: одинарные подчёркивания env-провайдер не заменяет на `:` — грабля `CDC_POLLING__` старого стенда). Compose (зафиксировано): два одинаковых экземпляра `cdc-outbox-1`/`cdc-outbox-2` — резервирование, арбитр — эксклюзивность слота `outbox_slot` (проигравший получает 55006 и ретраит с backoff; модель — `docs/stand.md` «Резервирование cdc-outbox»), образ `streaming-outbox/cdc-outbox:net8.0`, host-порты 18084/18085, always-on без профилей; конфигурационные env (`CDCOUTBOX__*`) добавит этап реализации.
- Health checks обоих сервисов — встроенный фреймворк ASP.NET Core (`AddHealthChecks` + `MapHealthChecks`; `Microsoft.Extensions.Diagnostics.HealthChecks` входит в shared framework 8, отдельного пина нет) c ResponseWriter-контрактом `{"status":…}`. `/health` — liveness без чеков (`Predicate = _ => false`). `/ready`: Writer — postgres-чек из AspNetCore.HealthChecks.Npgsql (строка в таблице); KafkaValidator — собственный `ValidatorEngineHealthCheck` (готовность = KafkaFlow-шина running). Экосистемный AspNetCore.HealthChecks.Kafka сознательно не используется: его проверка — produce-раундтрип (пишет тестовое сообщение в topic при каждом пробе), для консюмера это side-effect в кластере и путь продюсера, а не консюмера. В тестах — Microsoft.Extensions.Diagnostics.HealthChecks 8.0.11 (test-only).
- pgoutput в Npgsql (актуально и для 8.x, и для 9.x) отдаётся **только типизированными сообщениями** (сырого `XLogDataMessage` для pgoutput в публичном API нет).
- Значения колонок pgoutput приходят текстом и размечаются как `text`: читать `Get<string>()` и парсить самому (`Guid.TryParse`, парсинг timestamp) — `Get<Guid>()`/`Get<DateTime>()` кидают `InvalidCastException`.
- Подтверждение позиции — `SetReplicationStatus(TransactionEndLsn)` + `SendStatusUpdate()` **только после Kafka delivery** (at-least-once).
- Свой enum `KafkaFlow.Acks` конфликтует с `Confluent.Kafka.Acks` — квалифицировать (замечание проверено на линии 4.2; на линии 3.0 сигнатуру уточнить при реализации).
- Публикация в Kafka осуществляется строго через интерфейс `IOutboxMessagesProcessorTransport` (контракт и внутреннее представление `OutboxMessage` — `outbox-table.md`); реализация транспорта — поверх KafkaFlow.

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
