# metrics.md — обязательные метрики сервиса (Outbox)

Требования к метрикам, которые **обязательно должны быть в сервисе**. Метрики создаются через prometheus-net API — `Metrics.CreateGauge`, `Metrics.CreateCounter`, `Metrics.CreateHistogram` (см. `dependencies.md` §2, prometheus-net 5.0); класс-носитель — `OutboxMetrics`.

---

## 1. Список метрик Outbox

### 1. `outbox_processor_is_active`

Тип: Gauge

Показывает, работает ли экземпляр Outbox-процессора.

- `1` — процессор работает.
- `0` — процессор не работает.

В Active-Passive паре (recommendations.md «Балансировка CDC-экземпляров через Kafka») `1` — только у активного экземпляра-лидера (держит partition consumer-группы `cdc-checkpoints`); standby-экземпляр жив, но его процессор доставки не работает — `0`. Этой метрикой закрывается и требование «активен ли экземпляр CDC» (лидерство) — отдельная метрика не вводится.

Labels: `partition`

### 2. `outbox_processor_last_iteration_timestamp_seconds`

Тип: Gauge

Unix timestamp (в секундах) времени запуска последней итерации цикла отправки сообщений Outbox-процессором.

Labels: `partition`

Используется для определения того, насколько давно процессор выполнял обработку сообщений.

### 3. `outbox_processor_sent_messages_total`

Тип: Counter

Общее количество сообщений, отправленных экземпляром Outbox-процессора за всё время его работы.

Labels: `partition`

Используется для мониторинга активности отправки сообщений и расчёта скорости отправки.

### 4. `outbox_processor_read_messages_current`

Тип: Gauge

Количество outbox-сообщений, декодированных экземпляром outbox-процессора из потока слота (INSERT outbox-таблицы в логической репликации) с момента запуска экземпляра. Gauge хранится в памяти процесса: при рестарте обнуляется. При переподключении без подтверждённого LSN перечитанный WAL декодируется повторно и тоже увеличивает метрику (норма at-least-once).

Labels: `partition`

Скорость обработки — скорость роста метрики (rate); фиксация значения на границе `QueueCapacity` — канал чтения заполнен, чтение остановлено backpressure'ом.

### 5. `outbox_processor_errors_total`

Тип: Counter

Общее количество ошибок, возникших при работе экземпляра Outbox-процессора.

Labels: `partition`

Используется для мониторинга ошибок и обнаружения проблем при обработке сообщений.

### 6. `outbox_saved_messages_total`

Тип: Counter

Общее количество сообщений, сохранённых в Outbox за время работы сервиса.

Labels: `partition`, `topic`

Устаревшая метрика — вместо неё использовать `partitioning_outbox_saved_messages_total`.

### 7. `partitioning_outbox_saved_messages_total`

Тип: Counter

Количество сообщений, сохранённых в Outbox за время работы сервиса.

Labels: `partition`

Это новая метрика, предназначенная для замены `outbox_saved_messages_total`.

### 8. `outbox_message_age_seconds`

Тип: Histogram

Время, прошедшее между сохранением сообщения в Outbox и его отправкой, в секундах.

Используется для оценки задержек доставки сообщений и анализа распределения времени обработки.

### 9. `outbox_processor_publish_latency_seconds`

Тип: Histogram

Время от отправки события в Kafka до получения подтверждения (delivery-репорт / ACK), в секундах.

Labels: —

Характеризует приём со стороны брокера; сознательно отделена от сквозной задержки `outbox_message_age_seconds` (сохранение → отправка).

### 10. `outbox_processor_pending_events_current`

Тип: Gauge

Количество событий, отправленных в Kafka, но ещё не подтверждённых ACK (in-flight).

Labels: —

До подтверждения непрерывного диапазона событий checkpoint не продвигается; рост метрики — индикатор backpressure (брокер не успевает подтверждать).

### 11. `outbox_checkpoint_age_seconds`

Тип: Gauge

Время (в секундах) с последнего успешного транзакционного сохранения checkpoint в `cdc-checkpoints`.

Labels: —

Устойчивый рост сверх интервала периодического сохранения — признак проблем checkpoint-стора или полного отсутствия прогресса доставки.

### 12. `outbox_checkpoint_lag_bytes`

Тип: Gauge

Разница в байтах между прочитанной позицией WAL и LSN устойчиво сохранённого checkpoint.

Labels: —

Оценивает объём WAL, который придётся переобработать (и повторно доставить) при переключении активного экземпляра.

### 13. `outbox_checkpoint_errors_total`

Тип: Counter

Общее количество ошибок транзакционного сохранения checkpoint в `cdc-checkpoints`.

Labels: —

### 14. `outbox_slot_reconnects_total`

Тип: Counter

Количество переподключений к replication slot (разрывы walsender: failover/switchover PostgreSQL, сетевые сбои, backpressure-таймауты).

Labels: —

### 15. `outbox_failovers_total`

Тип: Counter

Количество переключений активного экземпляра — смен лидера в consumer group `cdc-checkpoints` (rebalance при отказе или потере лидерства).

Labels: —

Соответствие исходному списку CDC-метрик (решение владельца 2026-10-22): «активен ли экземпляр CDC» закрыт существующей `outbox_processor_is_active` (в Active-Passive паре 1 — только у лидера, см. выше), «ошибки публикации» — существующей `outbox_processor_errors_total` (считает все ошибки процессора, включая публикацию). Остальные семь показателей добавлены выше с неймингом проекта.

---

## 2. Сводная таблица

| Метрика                                             | Тип       | Назначение                                            |
| --------------------------------------------------- | --------- | ----------------------------------------------------- |
| `outbox_processor_is_active`                        | Gauge     | Активность процессора                                 |
| `outbox_processor_last_iteration_timestamp_seconds` | Gauge     | Время последней итерации                              |
| `outbox_processor_sent_messages_total`              | Counter   | Количество отправленных сообщений                     |
| `outbox_processor_read_messages_current`            | Gauge     | Декодировано из слота с момента запуска               |
| `outbox_processor_errors_total`                     | Counter   | Общее количество ошибок                               |
| `outbox_saved_messages_total`                       | Counter   | Количество сохранённых сообщений (устаревшая метрика) |
| `partitioning_outbox_saved_messages_total`          | Counter   | Количество сохранённых сообщений (новая метрика)      |
| `outbox_message_age_seconds`                        | Histogram | Задержка между сохранением и отправкой                |
| `outbox_processor_publish_latency_seconds`          | Histogram | Время получения Kafka ACK                             |
| `outbox_processor_pending_events_current`           | Gauge     | События, ожидающие подтверждения (in-flight)         |
| `outbox_checkpoint_age_seconds`                     | Gauge     | Время с последнего сохранения checkpoint              |
| `outbox_checkpoint_lag_bytes`                       | Gauge     | Прочитанный LSN минус устойчивый checkpoint           |
| `outbox_checkpoint_errors_total`                    | Counter   | Ошибки сохранения checkpoint                          |
| `outbox_slot_reconnects_total`                      | Counter   | Переподключения к replication slot                    |
| `outbox_failovers_total`                            | Counter   | Переключения активного экземпляра                     |

---

## 3. Что важно мониторить

Для контроля состояния Outbox выделяются пять основных показателей:

1. Доступность процессора — `outbox_processor_is_active`. Позволяет обнаружить остановку обработки.
2. Регулярность обработки — `outbox_processor_last_iteration_timestamp_seconds`. Помогает выявить зависание процессора.
3. Скорость отправки — `rate(outbox_processor_sent_messages_total[5m])`. Показывает количество отправляемых сообщений в секунду.
4. Количество ошибок — `increase(outbox_processor_errors_total[5m])`. Показывает прирост ошибок за последние пять минут.
5. Задержка доставки — `outbox_message_age_seconds`. Позволяет оценивать, насколько долго сообщения ожидают отправки.

Дополнительно полезно сравнивать скорость сохранения сообщений в Outbox со скоростью их отправки. Если сообщения сохраняются быстрее, чем отправляются, это может свидетельствовать о росте очереди. Однако для точного определения размера очереди нужна отдельная метрика количества необработанных сообщений.

Для пары Active-Passive (`cdc-outbox-1`/`cdc-outbox-2`, recommendations.md «Балансировка CDC-экземпляров через Kafka»): у лидера `outbox_processor_is_active=1`, у standby-экземпляра — `0`; оба таргета Prometheus при этом up (это liveness пары; таргет cdc-outbox-2 вернётся в Prometheus вместе с профилем `redundancy` — сейчас экземпляр выключен до реализации резервирования). `increase(outbox_failovers_total[15m])` фиксирует переключения активного экземпляра, `outbox_checkpoint_age_seconds` и `outbox_checkpoint_lag_bytes` — здоровье checkpoint-стора и объём возможной повторной доставки при переключении, `outbox_slot_reconnects_total` — нестабильность соединения к PostgreSQL.
