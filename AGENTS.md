# AGENTS.md

Проект `streaming-outbox`: сервис, читающий outbox-сообщения из PostgreSQL 17 через слот логической репликации (плагин pgoutput, Npgsql Logical Replication API — стриминговая репликация, без polling) и публикующий их в Kafka через KafkaFlow.

## Главное правило

**При реализации сервиса опираться на `dependencies.md`, `recommendations.md`, `metrics.md` и `outbox-table.md`.**

`dependencies.md` — единый источник правды о:

- версиях инфраструктуры (PostgreSQL, Patroni, etcd, HAProxy, PgBouncer, Kafka, observability-стек);
- версиях NuGet-зависимостей сервиса (Npgsql, KafkaFlow, OpenTelemetry);
- формате outbox-таблицы `outbox.messages` (DDL);
- канонических примерах создания publication и failover-слота репликации.

`recommendations.md` — архитектурные требования к реализации CDC-сервиса:

- управление replication slot (создаётся сервисом при первом запуске, идемпотентно) и устойчивые checkpoint'ы;
- конвейер `WAL Read → Kafka Send → Kafka ACK → Durable Checkpoint → LSN Flush` — ни checkpoint, ни LSN не опережают подтверждённую Kafka-доставку;
- at-least-once доставка (дедупликация — Inbox потребителя), backpressure на ограниченных очередях, конфигурируемые пачки/in-flight;
- восстановление после сбоев, монотонность подтверждаемой позиции, явные ошибки вместо тихих пропусков.

`metrics.md` — обязательные метрики сервиса (prometheus-net, класс `OutboxMetrics`): активность и итерации процессора, отправленные/прочитанные/сохранённые сообщения, ошибки, возраст сообщений (`outbox_message_age_seconds`); сводная таблица и приоритетные показатели мониторинга.

`outbox-table.md` — описание таблицы `outbox.messages`: назначение (Transactional Outbox), структура полей, партиционирование (`partition` брокера / `outbox_partition` процессора), контракты сервиса (`OutboxMessage`, обязательный транспорт публикации `IOutboxMessagesProcessorTransport`, настройки `OutboxStorageOptions` — схема/таблица), связь с метриками.

Любое изменение версии, схемы или архитектурного решения **сначала** фиксируется в этих файлах, **затем** в коде и инфраструктуре.

## Ключевые ограничения

- Чтение outbox — только через logical replication slot; SELECT-polling таблицы запрещён.
- Механика CDC — outbox-таблица + pgoutput (pgoutput не передаёт logical messages ни в одной версии PostgreSQL — доставка строится на INSERT).
- Подтверждение LSN слота — только после Kafka delivery (`acks=all`): семантика at-least-once, дубликаты допустимы, потери нет.
- KafkaFlow — основной API публикации; Confluent.Kafka — только транзитивная зависимость.
- .NET 8 (`net8.0`) и PostgreSQL 17 — строго; всё в Docker Compose.
