# streaming-outbox

Сервис доставки outbox-сообщений из PostgreSQL 17 в Kafka: чтение через слот логической репликации (плагин pgoutput, Npgsql Logical Replication API — стриминг, без polling), публикация продюсером Confluent.Kafka, checkpoint позиции в служебном Kafka-топике `cdc-checkpoints` и подтверждение LSN только после подтверждённой доставки — at-least-once без потерь.

Архитектура и контракты — в `docs/` (главное: `docs/dependencies.md`, `docs/recommendations.md`, `docs/outbox-table.md`, `docs/metrics.md`); описание стенда и команд — `docs/stand.md`.

## Статус реализации

Информационная сводка по `docs/implementation-plan.md` — авторитетный чек-лист статуса находится там и обновляется вместе с задачами. Выполненные задачи закрываются живой проверкой на стенде: сборка и все тесты решения зелёные, `scripts/stand.sh check` 58/58.

| № | Задача | Статус |
|---|--------|--------|
| 1 | Конфигурация и каркас процесса | готово |
| 2 | Failover-слот сервисом + проверка объектов (publication и topic — от init) | готово |
| 3 | Чтение слота → `OutboxMessage` | готово |
| 4 | Публикация в Kafka: продюсер Confluent.Kafka | готово |
| 5 | Checkpoint Store в Kafka + подтверждение LSN | готово |
| 6 | Устойчивость: переподключения, backpressure, graceful stop | в плане |
| 7 | Recovery Mode | в плане |
| 8 | Idle-предупреждение | в плане |
| 9 | Метрики: полный аудит | в плане |
| 10 | Active-Passive: consumer group на `cdc-checkpoints` (профиль redundancy) | в плане |
| 11 | Приёмочные сценарии и синхронизация документации | в плане |
| 12 | Бинарный протокол pgoutput (опция `binary`) | в плане |
| 13 | Политика для неисправимых ошибок данных (битые `headers`) | в плане |
| 14 | Старт с ограниченными правами в Kafka (ACL по сертификату) | в плане |
| 15 | Общий checkpoint-топик на несколько слотов и сервисов (многопартиционность) | в плане |

Конвейер доставки (задачи 1–5) замкнут: `чтение WAL → отправка в Kafka → ACK → durable checkpoint → подтверждение LSN`. Следующая задача — 6 (устойчивость: переподключения, backpressure, graceful stop).
