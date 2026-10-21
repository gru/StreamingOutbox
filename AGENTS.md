# AGENTS.md

Проект `streaming-outbox`: сервис, читающий outbox-сообщения из PostgreSQL 17 через слот логической репликации (плагин pgoutput, Npgsql Logical Replication API — стриминговая репликация, без polling) и публикующий их в Kafka через KafkaFlow.

## Главное правило

**При реализации сервиса опираться на `dependencies.md`.** Это единый источник правды о:

- версиях инфраструктуры (PostgreSQL, Patroni, etcd, HAProxy, PgBouncer, Kafka, observability-стек);
- версиях NuGet-зависимостей сервиса (Npgsql, KafkaFlow, OpenTelemetry);
- формате outbox-таблицы `public.kafka_outbox_messages`;
- канонических примерах создания publication и failover-слота репликации.

Любое изменение версии или схемы **сначала** фиксируется в `dependencies.md`, **затем** в коде и инфраструктуре.

## Ключевые ограничения

- Чтение outbox — только через logical replication slot; SELECT-polling таблицы запрещён.
- Механика CDC — outbox-таблица + pgoutput (pgoutput не передаёт logical messages ни в одной версии PostgreSQL — доставка строится на INSERT).
- Подтверждение LSN слота — только после Kafka delivery (`acks=all`): семантика at-least-once, дубликаты допустимы, потери нет.
- KafkaFlow — основной API публикации; Confluent.Kafka — только транзитивная зависимость.
- .NET 8 (`net8.0`) и PostgreSQL 17 — строго; всё в Docker Compose.
