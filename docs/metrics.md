# metrics.md — обязательные метрики сервиса (Outbox)

Требования к метрикам, которые **обязательно должны быть в сервисе**. Метрики создаются через prometheus-net API — `Metrics.CreateGauge`, `Metrics.CreateCounter`, `Metrics.CreateHistogram` (см. `dependencies.md` §2, prometheus-net 5.0); класс-носитель — `OutboxMetrics`.

---

## 1. Список метрик Outbox

Все метрики несут единый label `slot` — имя replication slot (`ReplicationOptions.SlotName`, в стенде `outbox_slot`). Процессор обслуживает один слот, а слот-ориентированные серии готовы к мульти-слотовой модели (общий checkpoint-топик на несколько слотов и сервисов — задача 15 плана; решение владельца 2026-11-04: до неё без-label метрики смешали бы наблюдения разных слотов в одном процессе). Прикладная партиция outbox (`outbox_partition` строки) в метриках не отражается: в модели «один слот = одна партиция checkpoint-топика» она не добавляет измерения.

Имена строятся как `outbox_<подсистема-источник>_<показатель>`: `processor` — цикл обработки процессора доставки, `checkpoint` — checkpoint-стор, `slot` — соединение с replication slot. Без подсистемы — общесервисные события и свойства сообщения: `outbox_message_age_seconds` — свойство сообщения (сквозная задержка доставки), а не работа процессора, поэтому без `processor_` — в паре с `outbox_processor_publish_latency_seconds`, которая характеризует приём со стороны брокера.

### 1. `outbox_processor_is_active`

Тип: Gauge

Показывает, работает ли экземпляр Outbox-процессора.

- `1` — процессор работает.
- `0` — процессор не работает.

В Active-Passive паре (recommendations.md «Балансировка CDC-экземпляров через Kafka») `1` — только у активного экземпляра-лидера (держит partition consumer-группы `cdc-checkpoints`); standby-экземпляр жив, но его процессор доставки не работает — `0`. Этой метрикой закрывается и требование «активен ли экземпляр CDC» (лидерство) — отдельная метрика не вводится.

Labels: `slot`

### 2. `outbox_processor_last_iteration_timestamp_seconds`

Тип: Gauge

Unix timestamp (в секундах) времени запуска последней итерации цикла отправки сообщений Outbox-процессором.

Labels: `slot`

Используется для определения того, насколько давно процессор выполнял обработку сообщений.

### 3. `outbox_processor_sent_messages_total`

Тип: Counter

Общее количество сообщений, отправленных экземпляром Outbox-процессора за всё время его работы.

Labels: `slot`

Используется для мониторинга активности отправки сообщений и расчёта скорости отправки.

### 4. `outbox_processor_read_messages_current`

Тип: Gauge

Количество outbox-сообщений, декодированных экземпляром outbox-процессора из потока слота (INSERT outbox-таблицы в логической репликации) с момента запуска экземпляра. Gauge хранится в памяти процесса: при рестарте обнуляется. При переподключении без подтверждённого LSN перечитанный WAL декодируется повторно и тоже увеличивает метрику (норма at-least-once).

Labels: `slot`

Скорость обработки — скорость роста метрики (rate); фиксация значения на границе `QueueCapacity` — канал чтения заполнен, чтение остановлено backpressure'ом.

### 5. `outbox_processor_errors_total`

Тип: Counter

Общее количество ошибок, возникших при работе экземпляра Outbox-процессора.

Labels: `slot`

Используется для мониторинга ошибок и обнаружения проблем при обработке сообщений.

### 6. `outbox_message_age_seconds`

Тип: Histogram

Время, прошедшее между сохранением сообщения в Outbox и его отправкой, в секундах.

Наблюдается в момент получения подтверждения брокера (ACK) — фиксирует сквозную задержку доставки: сообщение ждёт и в канале чтения, и в очереди повторов, и в подтверждении брокером.

Labels: `slot`

Используется для оценки задержек доставки сообщений и анализа распределения времени обработки.

### 7. `outbox_processor_publish_latency_seconds`

Тип: Histogram

Время от отправки события в Kafka до получения подтверждения (delivery-репорт / ACK), в секундах.

Labels: `slot`

Характеризует приём со стороны брокера; сознательно отделена от сквозной задержки `outbox_message_age_seconds` (сохранение → отправка).

### 8. `outbox_processor_pending_events_current`

Тип: Gauge

Количество событий, отправленных в Kafka, но ещё не подтверждённых ACK (in-flight).

Labels: `slot`

До подтверждения непрерывного диапазона событий checkpoint не продвигается; рост метрики — индикатор backpressure (брокер не успевает подтверждать).

### 9. `outbox_checkpoint_age_seconds`

Тип: Gauge

Время (в секундах) с последнего подтверждённого сохранения checkpoint в `cdc-checkpoints` (запись получила ACK брокера; транзакционным сохранение станет в задаче 10 плана).

Labels: `slot`

Устойчивый рост сверх интервала периодического сохранения — признак проблем checkpoint-стора или полного отсутствия прогресса доставки.

### 10. `outbox_checkpoint_lag_bytes`

Тип: Gauge

Разница в байтах между прочитанной позицией WAL (последний WalEnd потока репликации) и позицией подтверждённо сохранённого checkpoint.

Labels: `slot`

Оценивает объём WAL, который придётся переобработать (и повторно доставить) при переключении активного экземпляра.

### 11. `outbox_checkpoint_errors_total`

Тип: Counter

Общее количество ошибок сохранения checkpoint в `cdc-checkpoints`.

Labels: `slot`

### 12. `outbox_slot_reconnects_total`

Тип: Counter

Количество переподключений к replication slot (разрывы walsender: failover/switchover PostgreSQL, сетевые сбои, backpressure-таймауты). Инкремент — перед каждой сессией чтения слота, кроме первой (задача 6): при устойчивом отказе растёт по одной на попытку переподключения.

Labels: `slot`

### 13. `outbox_failovers_total`

Тип: Counter

Количество переключений активного экземпляра — смен лидера в consumer group `cdc-checkpoints` (rebalance при отказе или потере лидерства).

Labels: `slot`

Инкремент появляется в задаче 10 плана (Active-Passive, consumer group на `cdc-checkpoints`); метрика создаётся сразу с label `slot`.

### 14. `outbox_recovery_mode`

Тип: Gauge

1 — процесс запущен с включенным Recovery Mode (env `CDCOUTBOX__RECOVERYMODE=true`, задача 7): аварийное восстановление со переписыванием checkpoint по текущей позиции слота; 0 — обычный режим. Устанавливается один раз при старте и живёт до перезапуска процесса. Старая и новая позиции репликации фиксируются WARN-логом старта (`Recovery Mode: checkpoint X → Y`), метрика отражает только факт режима.

Labels: `slot`

Соответствие исходному списку CDC-метрик (сверка с исходными условиями 2026-11-04, дословно): шесть действующих имён, относящихся к читающей стороне, перенесены без изменений — `outbox_processor_is_active`, `outbox_processor_last_iteration_timestamp_seconds`, `outbox_processor_sent_messages_total`, `outbox_processor_read_messages_current`, `outbox_processor_errors_total`, `outbox_message_age_seconds`. `outbox_saved_messages_total` — метрика пишущей стороны (сервис, вставляющий сообщения в outbox-таблицу): наш сервис в outbox не пишет, факт сохранения — не его событие, метрика не переносится (решение владельца 2026-11-04; в тестовом контуре роль закрывает `writer_messages_written_total` Writer). `partitioning_outbox_saved_messages_total` — `[Obsolete]` в исходнике, не переносится. Единственное осознанное отклонение — labels: исходный `partition` унифицирован в `slot` (решение владельца 2026-11-04). Остальные метрики списка (publish-latency, pending, checkpoint age/lag/errors, reconnects, failovers, recovery) добавлены задачами 4–10 с неймингом проекта: показатель «активен ли экземпляр CDC» закрыт `outbox_processor_is_active` (в Active-Passive паре 1 — только у лидера), «ошибки публикации» — `outbox_processor_errors_total` (считает все ошибки процессора, включая публикацию).

---

## 2. Сводная таблица

| Метрика                                             | Тип       | Назначение                                            |
| --------------------------------------------------- | --------- | ----------------------------------------------------- |
| `outbox_processor_is_active`                        | Gauge     | Активность процессора                                 |
| `outbox_processor_last_iteration_timestamp_seconds` | Gauge     | Время последней итерации                              |
| `outbox_processor_sent_messages_total`              | Counter   | Количество отправленных сообщений                     |
| `outbox_processor_read_messages_current`            | Gauge     | Декодировано из слота с момента запуска               |
| `outbox_processor_errors_total`                     | Counter   | Общее количество ошибок                               |
| `outbox_message_age_seconds`                        | Histogram | Задержка между сохранением и отправкой                |
| `outbox_processor_publish_latency_seconds`          | Histogram | Время получения Kafka ACK                             |
| `outbox_processor_pending_events_current`           | Gauge     | События, ожидающие подтверждения (in-flight)         |
| `outbox_checkpoint_age_seconds`                     | Gauge     | Время с последнего сохранения checkpoint              |
| `outbox_checkpoint_lag_bytes`                       | Gauge     | Прочитанный LSN минус устойчивый checkpoint           |
| `outbox_checkpoint_errors_total`                    | Counter   | Ошибки сохранения checkpoint                          |
| `outbox_slot_reconnects_total`                      | Counter   | Переподключения к replication slot                    |
| `outbox_failovers_total`                            | Counter   | Переключения активного экземпляра (задача 10)         |
| `outbox_recovery_mode`                              | Gauge     | Recovery Mode процесса                                |

Все метрики несут label `slot` = имя replication slot (в стенде `outbox_slot`).

---

## 3. Что важно мониторить

Для контроля состояния Outbox выделяются пять основных показателей:

1. Доступность процессора — `outbox_processor_is_active`. Позволяет обнаружить остановку обработки.
2. Регулярность обработки — `outbox_processor_last_iteration_timestamp_seconds`. Помогает выявить зависание процессора.
3. Скорость отправки — `rate(outbox_processor_sent_messages_total[5m])`. Показывает количество отправляемых сообщений в секунду.
4. Количество ошибок — `increase(outbox_processor_errors_total[5m])`. Показывает прирост ошибок за последние пять минут.
5. Задержка доставки — `outbox_message_age_seconds`. Позволяет оценивать, насколько долго сообщения ожидают отправки.

Дополнительно полезно сравнивать скорость записи пишущего сервиса (в тестовом контуре — `writer_messages_written_total` Writer; в проде — метрика сохранения бизнес-приложения) со скоростью отправки `outbox_processor_sent_messages_total` и скоростью чтения `outbox_processor_read_messages_current`: если сообщения сохраняются быстрее, чем отправляются, растёт очередь. Размер потенциальной повторной доставки при переключении экземпляра показывает `outbox_checkpoint_lag_bytes`.

Для пары Active-Passive (`cdc-outbox-1`/`cdc-outbox-2`, recommendations.md «Балансировка CDC-экземпляров через Kafka»): у лидера `outbox_processor_is_active=1`, у standby-экземпляра — `0`; оба таргета Prometheus при этом up (это liveness пары; таргет cdc-outbox-2 вернётся в Prometheus вместе с профилем `redundancy` — сейчас экземпляр выключен до реализации резервирования). `increase(outbox_failovers_total[15m])` фиксирует переключения активного экземпляра, `outbox_checkpoint_age_seconds` и `outbox_checkpoint_lag_bytes` — здоровье checkpoint-стора и объём возможной повторной доставки при переключении, `outbox_slot_reconnects_total` — нестабильность соединения к PostgreSQL.
