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
| PgBouncer | **1.24.1** | `edoburu/pgbouncer:v1.24.1-p1` (pool_mode = transaction) |
| Apache Kafka (KRaft, без ZooKeeper) | **3.9.0** | `apache/kafka:3.9.0`, кластер ×3 |
| Prometheus | **2.54.1** | `prom/prometheus:v2.54.1` |
| postgres-exporter | **0.15.0** | `prometheuscommunity/postgres-exporter:v0.15.0` (имя с **дефисом**; вариант с подчёркиванием не существует) |
| kafka-exporter | **1.8.0** | `danielqsj/kafka-exporter:v1.8.0` |
| cAdvisor | **0.49.1** | `gcr.io/cadvisor/cadvisor:v0.49.1` |
| Оркестрация | Docker Compose | всё строго в Docker Compose |

Замечания к инфраструктуре:

- **Kafka-топик** `outbox.events`: 6 партиций, replication factor 3, `min.insync.replicas=2`; `auto.create.topics.enable=false` — топик создаётся явно отдельной init-командой, сервис сам его не создаёт.
- **PgBouncer не проксирует replication-протокол**: сервис стриминговой репликации подключается к PostgreSQL **напрямую через HAProxy** (роль с привилегией `REPLICATION`); PgBouncer — только для прикладных INSERT-подключений (Writer).
- Patroni permanent replication slots для logical-слотов **не используются** — failover-слоты синхронизирует сам PostgreSQL 17.

---

## 2. Зависимости сервиса (стриминговая репликация)

Версии зафиксированы решением владельца (2026-10-21): Npgsql 9.0, OpenTelemetry 1.11, prometheus-net 5.0; Kafka-стек — KafkaFlow 3.0 + `KafkaFlow.Microsoft.DependencyInjection` 2.5 + Confluent.Kafka 2.3 (явный пиннинг) + librdkafka.redist 2.3. Стартовые версии исследовательского стенда (Npgsql 8.0.9, OTel 1.9.0, экспорт метрик через `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0-beta.2, KafkaFlow 4.2.0 + `KafkaFlow.Extensions.Hosting`, Confluent.Kafka 2.8.0 транзитивно) заменены.

| Пакет | Версия | Назначение |
|---|---|---|
| .NET | **8.0** (`net8.0`) строго | ASP.NET Core 8; сборка/тесты в `mcr.microsoft.com/dotnet/sdk:8.0` |
| Npgsql | **9.0** | Logical Replication API: `PgOutputReplicationSlot` + `StartReplication(...)` — pgoutput proto v1, типизированные сообщения; поддерживает `net8.0` |
| KafkaFlow | **3.0** | основной API публикации в Kafka (`acks=all`, идемпотентный producer) |
| KafkaFlow.Microsoft.DependencyInjection | **2.5** | интеграция KafkaFlow с Microsoft.Extensions.DependencyInjection — вместо `KafkaFlow.Extensions.Hosting`; пакеты KafkaFlow выходят синхронными линиями (пара к ядру 3.0 — DI 3.0; линия 2.5 на NuGet видна как prerelease) |
| KafkaFlow.LogHandler.Console | **3.0** | логирование KafkaFlow (в решении владельца не указана — выровнена по линии ядра 3.0) |
| Confluent.Kafka | **2.3** | носитель `ProducerConfig`/нативного клиента; пиннится явно (в стенде шла транзитивно); напрямую не использовать — основной API KafkaFlow |
| librdkafka.redist | **2.3** | нативные бинарники librdkafka (в т.ч. linux/arm64); пиннится в паре с Confluent.Kafka 2.3 для детерминизма рантайма |
| OpenTelemetry.Extensions.Hosting | **1.11** | метрики OTel (Meter/Counter/Gauge) |
| OpenTelemetry.Instrumentation.Runtime | **1.11** | runtime-метрики (.NET) |
| prometheus-net (`prometheus-net.AspNetCore`) | **5.0** | экспорт `/metrics` (pull, Prometheus) — вместо `OpenTelemetry.Exporter.Prometheus.AspNetCore`; линия 5.x (текущая у автора библиотеки — 8.x), выбрана решением владельца |

Замечания к сервису:

- pgoutput в Npgsql (актуально и для 8.x, и для 9.x) отдаётся **только типизированными сообщениями** (сырого `XLogDataMessage` для pgoutput в публичном API нет).
- Значения колонок pgoutput приходят текстом и размечаются как `text`: читать `Get<string>()` и парсить самому (`Guid.TryParse`, парсинг timestamp) — `Get<Guid>()`/`Get<DateTime>()` кидают `InvalidCastException`.
- Подтверждение позиции — `SetReplicationStatus(TransactionEndLsn)` + `SendStatusUpdate()` **только после Kafka delivery** (at-least-once).
- Свой enum `KafkaFlow.Acks` конфликтует с `Confluent.Kafka.Acks` — квалифицировать (замечание проверено на линии 4.2; на линии 3.0 сигнатуру уточнить при реализации).

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
- `outbox_partition` — внутренняя партиция **Outbox**: распределяет сообщения между экземплярами процессора; label `partition` метрик процессора (`metrics.md`) соответствует именно этому полю.
- Индексы, алгоритм выборки и политика очистки структурой не фиксируются — определяются реализацией процессора (см. `outbox-table.md`).

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
