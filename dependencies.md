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

| Пакет | Версия | Назначение |
|---|---|---|
| .NET | **8.0** (`net8.0`) строго | ASP.NET Core 8; сборка/тесты в `mcr.microsoft.com/dotnet/sdk:8.0` |
| Npgsql | **8.0.9** | Logical Replication API: `PgOutputReplicationSlot` + `StartReplication(...)` — pgoutput proto v1, типизированные сообщения |
| KafkaFlow | **4.2.0** | основной API публикации в Kafka (`acks=all`, идемпотентный producer) |
| KafkaFlow.Extensions.Hosting | **4.2.0** | интеграция KafkaFlow с Generic Host |
| KafkaFlow.LogHandler.Console | **4.2.0** | логирование KafkaFlow |
| Confluent.Kafka | 2.8.0 (транзитивно) | только носитель `ProducerConfig`; напрямую не использовать — основной API KafkaFlow |
| OpenTelemetry.Extensions.Hosting | **1.9.0** | метрики OTel |
| OpenTelemetry.Exporter.Prometheus.AspNetCore | **1.9.0-beta.2** | экспорт `/metrics` (pull, Prometheus) |
| OpenTelemetry.Instrumentation.Runtime | **1.9.0** | runtime-метрики (.NET) |

Замечания к сервису:

- pgoutput в Npgsql 8.0.x отдаётся **только типизированными сообщениями** (сырого `XLogDataMessage` для pgoutput в публичном API нет).
- Значения колонок pgoutput приходят текстом и размечаются как `text`: читать `Get<string>()` и парсить самому (`Guid.TryParse`, парсинг timestamp) — `Get<Guid>()`/`Get<DateTime>()` кидают `InvalidCastException`.
- Подтверждение позиции — `SetReplicationStatus(TransactionEndLsn)` + `SendStatusUpdate()` **только после Kafka delivery** (at-least-once).
- Свой enum `KafkaFlow.Acks` конфликтует с `Confluent.Kafka.Acks` — квалифицировать.

---

## 3. Outbox-таблица: `public.kafka_outbox_messages`

```sql
CREATE TABLE IF NOT EXISTS public.kafka_outbox_messages (
  id         uuid PRIMARY KEY,
  topic      text NOT NULL CHECK (topic = 'outbox.events'),
  body       jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

Назначение колонок:

| Колонка | Тип | Роль |
|---|---|---|
| `id` | `uuid` PK | уникальный ключ сообщения (UUID v7, задаёт клиент-Writer); key сообщения в Kafka |
| `topic` | `text` | целевой topic; `CHECK (topic = 'outbox.events')` запрещает произвольную маршрутизацию |
| `body` | `jsonb` | тело сообщения (JSON; Avro/Schema Registry — возможное будущее расширение) |
| `created_at` | `timestamptz` | время создания, задаёт клиент (`DEFAULT now()` — страховка) |

Свойства схемы:

- Таблица **сознательно минимальна** (PK без лишних индексов): лишние индексы увеличивают объём WAL на INSERT и искажают замеры.
- **Жизненный цикл строки**: Writer выполняет INSERT и DELETE тех же id **в одной транзакции** — таблица всегда пустая, сообщение живёт только в WAL. Поэтому publication ниже эмитит только INSERT (1 change на сообщение), а CDC в DELETE-записях не нуждается вовсе.
- `id` генерирует клиент: у роли записи нет прав, порождающих id на стороне БД; ретрай после неоднозначного commit разрешается PK-конфликтом (`unique_violation` = прошлый commit прошёл).

---

## 4. Publication — канонический пример

pgoutput эмитит изменения **только таблиц из publication**, поэтому publication содержит outbox-таблицу. `publish = 'insert'` — из жизненного цикла строки (см. §3): DELETE исключён из эмиссии, слот несёт 1 change на сообщение.

Каноническая форма:

```sql
CREATE PUBLICATION outbox_publication
  FOR TABLE public.kafka_outbox_messages
  WITH (publish = 'insert');
```

Идемпотентная форма (для init-скриптов; `SET (publish=...)` не трогает состав таблиц и безопасен при повторах):

```sql
SELECT 'CREATE PUBLICATION outbox_publication'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec

SELECT format('ALTER PUBLICATION outbox_publication ADD TABLE %I.%I', 'public', 'kafka_outbox_messages')
WHERE NOT EXISTS (
  SELECT FROM pg_publication_tables
  WHERE pubname = 'outbox_publication' AND schemaname = 'public' AND tablename = 'kafka_outbox_messages'
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
