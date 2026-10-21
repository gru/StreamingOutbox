# metrics.md — обязательные метрики сервиса (Outbox)

Требования к метрикам, которые **обязательно должны быть в сервисе**. Метрики создаются через prometheus-net API — `Metrics.CreateGauge`, `Metrics.CreateCounter`, `Metrics.CreateHistogram` (см. `dependencies.md` §2, prometheus-net 5.0); класс-носитель — `OutboxMetrics`.

---

## 1. Список метрик Outbox

### 1. `outbox_processor_is_active`

Тип: Gauge

Показывает, работает ли экземпляр Outbox-процессора.

- `1` — процессор работает.
- `0` — процессор не работает.

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

Количество сообщений, прочитанных экземпляром Outbox-процессора за один проход обработки.

Labels: `partition`

Позволяет оценивать объём сообщений, обрабатываемых за одну итерацию.

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

---

## 2. Сводная таблица

| Метрика                                             | Тип       | Назначение                                            |
| --------------------------------------------------- | --------- | ----------------------------------------------------- |
| `outbox_processor_is_active`                        | Gauge     | Активность процессора                                 |
| `outbox_processor_last_iteration_timestamp_seconds` | Gauge     | Время последней итерации                              |
| `outbox_processor_sent_messages_total`              | Counter   | Количество отправленных сообщений                     |
| `outbox_processor_read_messages_current`            | Gauge     | Количество прочитанных сообщений за проход            |
| `outbox_processor_errors_total`                     | Counter   | Общее количество ошибок                               |
| `outbox_saved_messages_total`                       | Counter   | Количество сохранённых сообщений (устаревшая метрика) |
| `partitioning_outbox_saved_messages_total`          | Counter   | Количество сохранённых сообщений (новая метрика)      |
| `outbox_message_age_seconds`                        | Histogram | Задержка между сохранением и отправкой                |

---

## 3. Что важно мониторить

Для контроля состояния Outbox выделяются пять основных показателей:

1. Доступность процессора — `outbox_processor_is_active`. Позволяет обнаружить остановку обработки.
2. Регулярность обработки — `outbox_processor_last_iteration_timestamp_seconds`. Помогает выявить зависание процессора.
3. Скорость отправки — `rate(outbox_processor_sent_messages_total[5m])`. Показывает количество отправляемых сообщений в секунду.
4. Количество ошибок — `increase(outbox_processor_errors_total[5m])`. Показывает прирост ошибок за последние пять минут.
5. Задержка доставки — `outbox_message_age_seconds`. Позволяет оценивать, насколько долго сообщения ожидают отправки.

Дополнительно полезно сравнивать скорость сохранения сообщений в Outbox со скоростью их отправки. Если сообщения сохраняются быстрее, чем отправляются, это может свидетельствовать о росте очереди. Однако для точного определения размера очереди нужна отдельная метрика количества необработанных сообщений.
