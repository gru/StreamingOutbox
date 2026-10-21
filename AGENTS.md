# AGENTS.md

Проект `streaming-outbox`: сервис, читающий outbox-сообщения из PostgreSQL 17 через слот логической репликации (плагин pgoutput, Npgsql Logical Replication API — стриминговая репликация, без polling) и публикующий их в Kafka через KafkaFlow.

## Главное правило

**При реализации сервиса опираться на `docs/dependencies.md`, `docs/recommendations.md`, `docs/metrics.md` и `docs/outbox-table.md`.**

`docs/dependencies.md` — единый источник правды о:

- версиях инфраструктуры (PostgreSQL, Patroni, etcd, HAProxy, PgBouncer, Kafka, observability-стек);
- версиях NuGet-зависимостей сервиса (Npgsql, KafkaFlow, OpenTelemetry);
- формате outbox-таблицы `outbox.messages` (DDL);
- канонических примерах создания publication и failover-слота репликации.

`docs/recommendations.md` — архитектурные требования к реализации CDC-сервиса:

- управление replication slot (создаётся сервисом при первом запуске, идемпотентно) и устойчивые checkpoint'ы;
- конвейер `WAL Read → Kafka Send → Kafka ACK → Durable Checkpoint → LSN Flush` — ни checkpoint, ни LSN не опережают подтверждённую Kafka-доставку;
- at-least-once доставка (дедупликация — Inbox потребителя), backpressure на ограниченных очередях, конфигурируемые пачки/in-flight;
- восстановление после сбоев, монотонность подтверждаемой позиции, явные ошибки вместо тихих пропусков.

`docs/metrics.md` — обязательные метрики сервиса (prometheus-net, класс `OutboxMetrics`): активность и итерации процессора, отправленные/прочитанные/сохранённые сообщения, ошибки, возраст сообщений (`outbox_message_age_seconds`); сводная таблица и приоритетные показатели мониторинга.

`docs/outbox-table.md` — описание таблицы `outbox.messages`: назначение (Transactional Outbox), структура полей, партиционирование (`partition` брокера / `outbox_partition` процессора), контракты сервиса (`OutboxMessage`, обязательный транспорт публикации `IOutboxMessagesProcessorTransport`, настройки `OutboxStorageOptions` — схема/таблица), связь с метриками.

Любое изменение версии, схемы или архитектурного решения **сначала** фиксируется в этих файлах, **затем** в коде и инфраструктуре.

## Текущее состояние

Из кода реализованы только сервисы тестового контура (перенесены из исследовательского стенда `streaming-polling-outbox` и адаптированы к `outbox.messages`): `src/Writer` (генератор нагрузки, HTTP `:18080` в старом стенде) и `src/KafkaValidator` (валидатор доставки, HTTP `:18083`). Решение — `StreamingOutbox.sln`; сборка/тесты — в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (68 тестов: 28 Writer + 35 KafkaValidator + 5 Integration-контрактов). Инфраструктуры (compose, init SQL, Kafka) в репозитории пока нет. Сам outbox-сервис не реализован.

- **Writer и KafkaValidator — самостоятельные проекты: общего кода с outbox-сервисом и между собой НЕ имеют.** Контракт тестового body/key зафиксирован в `docs/outbox-table.md` и сознательно продублирован в коде обоих сервисов.
- Writer: INSERT+DELETE одной транзакцией (таблица всегда пустая); id — `bigint` identity, резервируется `nextval` ДО сборки body и вставляется с `OVERRIDING SYSTEM VALUE` (детали и гранты sys_writer — `docs/dependencies.md` §3, «Запись тестовой нагрузки»); повтор батча теми же id → `unique_violation` = детект AlreadyCommitted.
- KafkaValidator: идентичность сообщения — `body.id` (= id строки, `long`), группировка — `body.runId`; консюмер KafkaFlow 3.0.10 (группа `kafka-validator`, Earliest, `WithoutStoringOffsets` — рестарт всегда перематывает topic с начала); обогащение снапшотом CDC удалено до появления outbox-сервиса (`verdict.Cdc` всегда null).
- Метрики обоих сервисов — prometheus-net 5.0 (`Metrics.Create*`, `MapMetrics`), без OTel; OTel-пины 1.11 — для outbox-сервиса. Грабли: внутри namespace `*.Metrics` имя `Metrics` резолвится в namespace — квалифицировать `Prometheus.Metrics`; `UseMetrics` в 5.0 нет — достаточно `MapMetrics`; labels — `WithLabels(...).Inc(...)`.

## Ключевые ограничения

- Чтение outbox — только через logical replication slot; SELECT-polling таблицы запрещён.
- Механика CDC — outbox-таблица + pgoutput (pgoutput не передаёт logical messages ни в одной версии PostgreSQL — доставка строится на INSERT).
- Подтверждение LSN слота — только после Kafka delivery (`acks=all`): семантика at-least-once, дубликаты допустимы, потери нет.
- KafkaFlow — основной API публикации; Confluent.Kafka — только транзитивная зависимость.
- .NET 8 (`net8.0`) и PostgreSQL 17 — строго; всё в Docker Compose.
