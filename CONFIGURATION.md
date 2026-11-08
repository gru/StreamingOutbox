# Конфигурация outbox-сервиса

## PostgreSQL и репликация

- `CdcOutbox:Postgres:ConnectionString` — обязательная, **важная**. Строка подключения к PostgreSQL: маршрут к primary, прикладная БД, роль, пароль и любые параметры драйвера (SSL, keepalives, `Application Name`) — одной строкой. Рекомендация: в HA-кластере подключаться через маршрутизатор к текущему primary; задать `Application Name` — наблюдаемость в `pg_stat_replication`.
- `CdcOutbox:Replication:SlotName` — обязательная, **важная**. Имя слота логической репликации; создаётся сервисом идемпотентно. Из имени выводятся `transactional.id` и контрольная группа арбитража — смена имени на работающей системе означает новую линию доставки с нуля.
- `CdcOutbox:Replication:PublicationName` — обязательная, **важная**. Publication с outbox-таблицей (`publish='insert'`); создаётся инфраструктурой/миграциями, сервис при старте проверяет.
- `CdcOutbox:Replication:Binary` — дефолт `false`. Бинарный формат значений pgoutput (PostgreSQL 14+): `bytea` идёт сырыми байтами без hex-надбавки. На производительность не влияет — выбор вопрос надёжности декодирования; переключение между рестартами безопасно.
- `CdcOutbox:Storage:Schema` — дефолт `outbox`. Схема outbox-таблицы.
- `CdcOutbox:Storage:Table` — дефолт `messages`. Имя outbox-таблицы.

## Kafka — подключение

- `CdcOutbox:Kafka:BootstrapServers` — обязательная, **важная**. Список bootstrap-серверов кластера.
- `CdcOutbox:Kafka:CheckpointTopic` — **важная**. Дефолт `cdc-checkpoints`. Служебный топик checkpoint-стора и арбитража пары. Должен существовать до запуска: 1 партиция — обязательно, `cleanup.policy=compact` — рекомендуется. Один топик общий на несколько слотов и сервисов.
- `CdcOutbox:Kafka:ControlSessionTimeoutSeconds` — **важная**. Дефолт `15`. Таймаут сессии контрольной группы — время обнаружения смерти активного экземпляра, задаёт скорость переключения пары. Рекомендация: в границах `group.min/max.session.timeout.ms` брокеров; уменьшение ускоряет переключение, но повышает риск ложных ребалансов.
- `CdcOutbox:Kafka:CheckpointReadTimeoutSeconds` — дефолт `30`. Бюджет чтения последнего checkpoint при старте, с.
- `CdcOutbox:Kafka:CheckpointPollTimeoutSeconds` — дефолт `2`. Такт опроса консюмера при чтении checkpoint, с.

## Kafka — продюсер

- `CdcOutbox:Kafka:EnableIdempotence` — дефолт `true`. Идемпотентный продюсер; обязательное требование транзакций — не отключать.
- `CdcOutbox:Kafka:BatchNumMessages` — дефолт `10000`. Сообщений на батч клиента.
- `CdcOutbox:Kafka:LingerMs` — дефолт `5`. Задержка накопления батча клиента, мс. Рекомендация: при низких темпах записи снизить — уменьшает латентность публикации.
- `CdcOutbox:Kafka:DeliveryTimeoutMs` — дефолт `120000`. Таймаут доставки сообщения, включая ретраи, мс. Рекомендация: `transaction.max.timeout.ms` окружения должен быть ≥ `DeliveryTimeoutMs + 60 000` (транзакционный таймаут выводится этой формулой), иначе инициализация продюсера упадёт.
- `CdcOutbox:Kafka:MessageSendMaxRetries` — дефолт `2147483647`. Лимит ретраев отправки; фактически ограничен delivery timeout.
- `CdcOutbox:Kafka:MessageMaxBytes` — дефолт `1000000`. Максимальный размер сообщения, байт. Рекомендация: не больше минимального из `message.max.bytes` брокера и `max.message.bytes` топиков — иначе крупные сообщения будут ретраиться бесконечно.

## Kafka — безопасность

- `CdcOutbox:Kafka:SecurityProtocol` — дефолт `plaintext`; `ssl` — mTLS с авторизацией клиентским сертификатом. Применяется ко всем клиентам сервиса.
- `CdcOutbox:Kafka:SslCaLocation` — PEM-файл CA для проверки брокеров (режим `ssl`).
- `CdcOutbox:Kafka:SslCertificateLocation` и `CdcOutbox:Kafka:SslKeyLocation` — файловая пара сертификат/ключ (режим `ssl`); `CdcOutbox:Kafka:SslKeyPassword` — пароль ключа, если зашифрован.
- `CdcOutbox:Kafka:SslCertificateThumbprint` — SHA-1 thumbprint сертификата из хранилища пользователя (`CdcOutbox:Kafka:SslStoreName`, дефолт `My`; `CdcOutbox:Kafka:SslStoreLocation`, дефолт `CurrentUser`); приватный ключ должен быть экспортируемым.
- Рекомендация: при `ssl` — ровно один источник сертификата: файловая пара ИЛИ thumbprint. Оба источника или ни одного — ошибка валидации старта.

## Процессор — конвейер

- `CdcOutbox:Processor:BatchSize` — **важная**. Дефолт `500`. Пачка публикации = одна транзакция Kafka; главный регулятор пропускной способности — потолок растёт с батчем (рост затухающий: на стенде 500 → ~25 000 msg/s, 1000 → ~41 000, 2000 → ~65 000). Значение `1` — строгий последовательный режим без окна перестановки порядка (~40 msg/s на стенде).
- `CdcOutbox:Processor:QueueCapacity` — дефолт `2048`. Ёмкость bounded-очереди чтение→публикация (backpressure). Рекомендация: не меньше `BatchSize` — иначе очередь срезает батч; при росте батча расти пропорционально.
- `CdcOutbox:Processor:CheckpointIntervalSeconds` — дефолт `5`. Каденс периодического checkpoint; определяет окно повторной доставки при переключении пары.

## Процессор — устойчивость и остановка

- `CdcOutbox:Processor:IdleTimeoutSeconds` — дефолт `30`. Порог простоя потока для предупреждения WARN (сигнал риска накопления WAL: без событий слот не подтверждается).
- `CdcOutbox:Processor:RetryDelaySeconds` — дефолт `5`. База backoff восстановлений (чтение слота, публикация, checkpoint) и пауза повторной отправки после провала доставки, с.
- `CdcOutbox:Processor:MaxRetryDelaySeconds` — дефолт `60`. Потолок backoff: пауза удваивается от базы до этого предела.
- `CdcOutbox:Processor:BackoffResetSessionSeconds` — дефолт `30`. Сессия чтения, прожившая столько, считается стабильной и сбрасывает счётчик backoff.
- `CdcOutbox:Processor:FinalCheckpointWaitSeconds` — дефолт `15`. Бюджет ожидания финального checkpoint при штатной остановке, с.
- `CdcOutbox:Processor:ShutdownTimeoutSeconds` — **важная**. Дефолт `30`. Бюджет штатной остановки: дрейн публикации, финальный checkpoint, подтверждение позиции слота. Рекомендация: таймаут принудительного завершения средой (compose `stop_grace_period`, k8s `terminationGracePeriodSeconds`) должен быть больше — иначе SIGKILL обрежет финальный checkpoint.

## Режимы

- `CdcOutbox:RecoveryMode` — дефолт `false`. Recovery Mode на один старт: аварийное восстановление позиции (утрата слота, сброс после миграции) — включается осознанно оператором, после восстановления сервис перезапускается без флага.

## Вычисляемые и зафиксированные параметры Kafka

Клиенты Kafka (продюсер, контрольный консюмер арбитража) конфигурируются сервисом автоматически — отдельные ключи для них не задаются: параметры либо вычисляются из настроек сервиса, либо зафиксированы дизайном:

| Параметр Kafka                  | Формула / значение                    | Комментарий                                                                                    |
|---------------------------------|---------------------------------------|------------------------------------------------------------------------------------------------|
| `transactional.id`              | `cdc-outbox-<SlotName>`               | транзакционный продюсер: fencing лидера пары                                                   |
| `group.id`                      | `cdc-outbox-<SlotName>`               | контрольная consumer group арбитража на checkpoint-топике: назначение партиции = лидерство     |
| `session.timeout.ms`            | `ControlSessionTimeoutSeconds × 1000` | должен попадать в границы `group.min/max.session.timeout.ms` окружения                         |
| `heartbeat.interval.ms`         | `session.timeout.ms / 3`              | правило трёх heartbeat на сессию                                                               |
| `transaction.timeout.ms`        | `DeliveryTimeoutMs + 60 000`          | окружение: `transaction.max.timeout.ms` ≥ этого значения, иначе инициализация продюсера упадёт |
| `acks`                          | `all` (зафиксировано)                 | единственное допустимое для транзакций значение                                                |
| `enable.idempotence`            | `true` (зафиксировано)                | обязательное требование транзакций                                                             |
| `partition.assignment.strategy` | `cooperative-sticky` (зафиксировано)  | вернувшийся экземпляр не крадёт лидерство                                                      |
| `enable.auto.commit`            | `false` (зафиксировано)               | офсеты группы не источник истины — checkpoint хранится по ключу                                |
| `auto.offset.reset`             | `latest` (зафиксировано)              | записи группы не используются                                                                  |
