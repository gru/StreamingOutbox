# streaming-outbox

Сервис доставки outbox-сообщений из PostgreSQL 17 в Kafka: чтение через слот логической репликации (плагин pgoutput, Npgsql Logical Replication API — стриминг, без polling), публикация продюсером Confluent.Kafka, checkpoint позиции в служебном Kafka-топике `cdc-checkpoints` и подтверждение LSN только после подтверждённой доставки — at-least-once без потерь.

Архитектура и контракты — в `docs/` (главное: `docs/dependencies.md`, `docs/recommendations.md`, `docs/outbox-table.md`, `docs/metrics.md`); описание стенда и команд — `docs/stand.md`.

## Статус реализации

Информационная сводка по `docs/implementation-plan.md` — авторитетный чек-лист статуса находится там и обновляется вместе с задачами. Выполненные задачи закрываются живой проверкой на стенде: сборка и все тесты решения зелёные, `scripts/stand.sh check` 59/59 (62/62 с профилем `redundancy`).

| № | Задача | Статус |
|---|--------|--------|
| 1 | Конфигурация и каркас процесса | готово |
| 2 | Failover-слот сервисом + проверка объектов (publication и topic — от init) | готово |
| 3 | Чтение слота → `OutboxMessage` | готово |
| 4 | Публикация в Kafka: продюсер Confluent.Kafka | готово |
| 5 | Checkpoint Store в Kafka + подтверждение LSN | готово |
| 6 | Устойчивость: переподключения, backpressure, graceful stop | готово |
| 7 | Recovery Mode | готово |
| 8 | Idle-предупреждение | готово |
| 9 | Метрики: полный аудит | готово |
| 10 | Active-Passive: consumer group на `cdc-checkpoints` (профиль redundancy) | готово |
| 11 | Приёмочные сценарии и синхронизация документации | готово |
| 12 | Бинарный протокол pgoutput (опция `binary`) | готово |
| 13 | Политика для неисправимых ошибок данных (битые `headers`) | в плане |
| 14 | Старт с ограниченными правами в Kafka (ACL по сертификату) | в плане |
| 15 | Общий checkpoint-топик на несколько слотов и сервисов (многопартиционность) | в плане |
| 16 | OTel: трассировка (activity/span) и runtime-метрики | в плане |
| 17 | Комментарии кода: единый стиль | в плане |

Конвейер доставки (задачи 1–5) замкнут: `чтение WAL → отправка в Kafka → ACK → durable checkpoint → подтверждение LSN`; устойчивость (задача 6) — переподключения с настраиваемым backoff, терминальные ошибки, корректная остановка в порядке «ACK → checkpoint → подтверждение → закрытие»; Recovery Mode (задача 7) — ручное аварийное восстановление на один старт (env `CDCOUTBOX__RECOVERYMODE=true`): старт от текущей позиции слота или пересоздание отсутствующего, новый checkpoint до начала чтения, пропуск окна — осознанный; idle-предупреждение (задача 8) — WARN в лог не чаще одного раза за `IdleTimeoutSeconds` (дефолт 30 с) при отсутствии новых outbox-сообщений, новое событие сбрасывает таймер; сигнал — риск накопления WAL: слот без событий не подтверждается, heartbeat-механизм отсутствует (сознательное решение — сервис рассчитан на постоянно пишущиеся outbox); метрики (задача 9) — полный аудит: 14 метрик metrics.md и кода (`outbox_failovers_total` реализована задачей 10), единый label `slot` = имя слота у всех, критерий нейминга `outbox_<подсистема>_<показатель>` зафиксирован в metrics.md; `outbox_saved_messages_total` из исходных условий не переносится — это метрика пишущей стороны (в контуре её закрывает `writer_messages_written_total` Writer). Резервирование (задача 10) — Active-Passive: арбитр — контрольная consumer group на `cdc-checkpoints` (назначение партиции = лидерство; CooperativeSticky — вернувшийся экземпляр не крадёт лидерство; standby держит членство poll'ом), транзакционный продюсер `cdc-outbox-<slot>` с fencing (InitTransactions только при получении лидерства; термин — recommendations.md «Fencing»), батч событий и checkpoint-запись фиксируются одной транзакцией по каденсу 5 с (простой/финал/recovery — checkpoint-only транзакции), чтение checkpoint — read_committed; стартовые проверки и Recovery Mode — только у лидера; потеря лидерства/fencing — graceful остановка доставки, хост живёт в standby (`is_active` 0, оба таргета Prometheus up); `outbox_failovers_total` — +1 на каждое получение лидерства; переключение при kill ≈ 15–20 с (session timeout + fencing + проверки), graceful stop — мгновенно (LeaveGroup); контракт произвольного Kafka-окружения (предусловия → вывод параметров → права принципала) — dependencies.md §2b; валидатор читает `outbox.events` read_committed. Второй экземпляр по умолчанию за профилем `redundancy` (включение — stand.md «Резервирование cdc-outbox»). Приёмка (задача 11) — два харнесса в `scripts/test/`: `integration-delivery.sh` (запись → вердикт → живая проба контракта `headers` → рестарт-продолжение → отказы PG/Kafka: failover лидера Patroni, минорный отказ брокера, полный outage → Active-Passive при поднятой паре → финальный `check`) и `upgrade-rehearsal.sh` (репетиция обновления PostgreSQL по пути dump/restore: dump → DROP/CREATE DATABASE → restore → recovery-старт; разрушительный для БД outbox); контракт значений `headers` зафиксирован приёмкой (`docs/outbox-table.md` «Заголовки»). Следующая задача — 12 (бинарный протокол pgoutput).
