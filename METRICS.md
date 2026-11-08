# Метрики outbox-сервиса

Экспорт — Prometheus, эндпоинт `/metrics`. Все метрики несут единый label `slot` — имя replication slot: процесс обслуживает один слот, label сводит наблюдения пары Active-Passive и нескольких слотов общего мониторинга. Имена строятся как `outbox_<подсистема>_<показатель>`: `processor` — цикл доставки, `checkpoint` — сохранение контрольных точек позиции, `slot` — соединение с слотом; без подсистемы — свойства сообщения. Важные для мониторинга отмечены словом «важная».

## Процессор доставки

- `outbox_processor_is_active` — **важная**. Gauge. Активность экземпляра: `1` — лидер пары Active-Passive (доставляет), `0` — standby (экземпляр жив, доставки нет). В паре ровно один со значением `1`.
- `outbox_processor_last_iteration_timestamp_seconds` — **важная**. Gauge. Unix time последней итерации цикла отправки — регулярность обработки, детектор зависания.
- `outbox_processor_sent_messages_total` — **важная**. Counter. Подтверждённо отправленные (ACK брокера) сообщения за время работы экземпляра; скорость доставки — `rate(...[5m])`.
- `outbox_processor_errors_total` — **важная**. Counter. Ошибки процессора (чтение, публикация, checkpoint); мониторить прирост `increase(...[5m])`.
- `outbox_processor_read_messages_current` — Gauge. Декодировано из слота с момента запуска; обнуляется при рестарте, повторная декодировка при переподключении — норма at-least-once. Фиксация значения на границе `QueueCapacity` — чтение остановлено backpressure'ом.
- `outbox_processor_pending_events_current` — Gauge. События, отправленные в Kafka, но ещё не зафиксированные commit'ом транзакции (in-flight). Рост — брокер не успевает подтверждать.
- `outbox_processor_publish_latency_seconds` — Histogram. Время от отправки события в Kafka до подтверждения (ACK) — приём со стороны брокера.

## Сообщение

- `outbox_message_age_seconds` — **важная**. Histogram. Сквозная задержка доставки: сохранение в outbox → ACK брокера; наблюдается в момент ACK, включает ожидание в очереди чтения и повторах.

## Контрольные точки (checkpoint)

- `outbox_checkpoint_age_seconds` — **важная**. Gauge. Время с последнего подтверждённо сохранённого checkpoint. Рекомендация по алертингу: рост при `checkpoint_lag_bytes > 0` — доставка идёт, но позиция не фиксируется (проблема сохранения), алерт при возрасте > `2 × CheckpointIntervalSeconds`; рост при `lag = 0` и `pending = 0` — здоровый простой без событий (heartbeat отсутствует сознательно), не алертить.
- `outbox_checkpoint_lag_bytes` — **важная**. Gauge. Прочитанная позиция WAL минус подтверждённый checkpoint, байты — объём повторной доставки при переключении активного экземпляра.
- `outbox_checkpoint_errors_total` — Counter. Ошибки сохранения checkpoint.

## Слот и переключения

- `outbox_slot_reconnects_total` — Counter. Переподключения к replication slot (failover/switchover PostgreSQL, сетевые сбои) — нестабильность соединения к PostgreSQL.
- `outbox_failovers_total` — Counter. Получения лидерства в контрольной группе (первое избрание = 1, каждая смена лидера +1); перезапуск процесса обнуляет, у standby серия отсутствует до первого избрания.
- `outbox_recovery_mode` — Gauge. `1` — процесс запущен в Recovery Mode (аварийное восстановление позиции), `0` — обычный режим; выставляется на старте и живёт до перезапуска.

## Базовый набор мониторинга

Пять показателей здоровья: активность (`is_active`), регулярность (`last_iteration`), скорость доставки (`rate(sent_messages_total[5m])`), ошибки (`increase(errors_total[5m])`), задержка доставки (`message_age_seconds`). Для пары Active-Passive дополнительно: ровно один `is_active=1` при живых обоих таргетах, переключения — `increase(failovers_total[15m])`, своевременность сохранения позиции и объём повторной доставки — `checkpoint_age_seconds`/`checkpoint_lag_bytes`.
