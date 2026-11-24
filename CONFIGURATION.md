# Конфигурация outbox-сервиса

## PostgreSQL и репликация

- `CdcOutbox:Postgres:ConnectionString` — обязательная, **важная**. Строка подключения к PostgreSQL: маршрут к primary, прикладная БД, роль, пароль и любые параметры драйвера (SSL, keepalives, `Application Name`) — одной строкой. Рекомендация: в HA-кластере подключаться через маршрутизатор к текущему primary; задать `Application Name` — наблюдаемость в `pg_stat_replication`.
- `CdcOutbox:Replication:SlotName` — обязательная, **важная**. Имя слота логической репликации; создаётся сервисом идемпотентно. Из имени выводятся `transactional.id` и контрольная группа арбитража — смена имени на работающей системе означает новую линию доставки с нуля.
- `CdcOutbox:Replication:PublicationName` — обязательная, **важная**. Publication с outbox-таблицей (`publish='insert'`); создаётся инфраструктурой/миграциями, сервис при старте проверяет.
- `CdcOutbox:Replication:Binary` — дефолт `false`. Бинарный формат значений pgoutput (PostgreSQL 14+): `bytea` идёт сырыми байтами без hex-надбавки. На производительность не влияет — выбор вопрос надёжности декодирования; переключение между рестартами безопасно.
- `CdcOutbox:Replication:FlushSlotOnIdle` — дефолт `false` (для `WalMessages` и `PartitionedTable` включается автоматически). Безопасное продвижение `confirmed_flush_lsn` слота и checkpoint в периоды простоя, когда нет новых outbox-сообщений — предотвращает рост WAL при фоновой активности других таблиц или БД; позиция двигается только после доставки всех сообщений из канала и стабильной паузы потока.
- `CdcOutbox:Storage:Schema` — дефолт `outbox`. Схема outbox-таблицы.
- `CdcOutbox:Storage:Table` — дефолт `messages`. Имя outbox-таблицы (для секционированного варианта из `04-wal-and-partitions.sql` — `messages_partitioned`).
- `CdcOutbox:Storage:Mode` — дефолт `Table`. Выбор способа доставки рядом со штатным режимом:
  - `Table` — штатная таблица `outbox.messages` (`INSERT + DELETE` писателем, поведение по умолчанию без изменений).
  - `WalMessages` — целевой безтабличный режим: прямая запись в WAL через `pg_logical_emit_message` (бинарный кадр `OBX1`), пустая publication (`outbox_wal_publication`), нулевой bloat/vacuum на таблице.
  - `PartitionedTable` — страхующая почасовая секционированная таблица (`INSERT`-only без `DELETE`, publication с `publish_via_partition_root = true`, фоновая нарезка на 48 ч вперёд, безопасное удаление доставленных часов и 24/7 авто-восстановление при утрате слота).
- `CdcOutbox:Storage:EnableWalMessages` — дефолт `false` (для `WalMessages` включается автоматически). Включает `pgoutput messages=true` одновременно с табличными `INSERT` — позволяет переключать на стороне писателя запись `INSERT` ↔ прямой `WAL` (`outbox.publish_message(..., p_write_to_table => true/false)`) без пересоздания слота и остановки доставки.
- `CdcOutbox:Storage:WalMessagePrefix` — дефолт `outbox`. Префикс сообщений `pg_logical_emit_message`, принадлежащих данному потоку (сообщения с чужим префиксом безопасно пропускаются).

## Почасовые партиции и 24/7 авто-восстановление (`PartitionedTable`)

Секция `CdcOutbox:Partitions` активна автоматически при `CdcOutbox:Storage:Mode = PartitionedTable` (или при явном `Enabled = true`) и работает **только на активном лидере** пары Active-Passive:

- `CdcOutbox:Partitions:Enabled` — дефолт `false` (автоматически `true` при `Mode = PartitionedTable`). Включает фоновый менеджер почасовых партиций.
- `CdcOutbox:Partitions:PrecreateHours` — дефолт `48` (2 дня вперёд, диапазон `[1..720]`). На каждом запуске (сразу при получении лидерства и далее раз в час) проверяет наличие текущего часа и всех `PrecreateHours` будущих часов и идемпотентно досоздаёт недостающие.
- `CdcOutbox:Partitions:RunAtMinuteOfHour` — дефолт `5` (`HH:05:00 UTC`, диапазон `[0..59]`). Фиксированная минута каждого часа UTC без дрейфа расписания: запечатывает закрытые часы и удаляет доставленные после сдвига подтверждённого LSN.
- `CdcOutbox:Partitions:SealGraceMinutes` — дефолт `5` (диапазон `[0..59]`). Задержка после окончания часа перед запечатыванием верхней границы `sealed_lsn = pg_current_wal_lsn()`.
- `CdcOutbox:Partitions:RetentionHours` — дефолт `0` (диапазон `[0..720]`). Дополнительный срок хранения закрытой и полностью доставленной партиции перед удалением, ч (`0` — удалять в ближайший плановый тик после сдвига LSN).
- `CdcOutbox:Partitions:AutoRecoverFromPartitions` — дефолт `true`. 24/7 автоматическое восстановление без ручного вмешательства при потере, инвалидации (`wal_status = 'lost'`) или скачке слота: блокирует уборщик партиций (`recovery_in_progress = true`), пересоздаёт `failover=true` слот, вычитывает пропущенное окно из партиций в `REPEATABLE READ` снапшоте, доставляет в Kafka, сохраняет checkpoint и снимает блокировку уборщика.
- `CdcOutbox:Partitions:RecoveryBatchSize` — дефолт `1000` (диапазон `[1..50000]`). Размер пачки при вычитке пропущенных строк из партиций.
- `CdcOutbox:Partitions:RecoveryOverlapMinutes` — дефолт `5` (диапазон `[0..1440]`). Запас перекрытия назад по `created_at` при авто-восстановлении из партиций.
- `CdcOutbox:Partitions:UseSecurityDefinerFunctions` — дефолт `true`. `true` — вызов `SECURITY DEFINER`-функций из `04-wal-and-partitions.sql` (роли `sys_replication` не нужны права владельца/DDL, только `EXECUTE`); `false` — прямой DDL от владельца таблицы с `ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY`.
- `CdcOutbox:Partitions:RegistryTable` — дефолт `partition_registry`. Имя таблицы реестра партиций и `sealed_lsn` в схеме outbox.
- `CdcOutbox:Partitions:GuardTable` — дефолт `partition_guard`. Имя таблицы защитного курсора (`system_identifier`, `contiguous_delivered_lsn`, `recovery_in_progress`) в схеме outbox.

## Kafka — подключение

- `CdcOutbox:Kafka:BootstrapServers` — обязательная, **важная**. Список bootstrap-серверов кластера.
- `CdcOutbox:Kafka:CheckpointTopic` — **важная**. Дефолт `cdc-checkpoints`. Служебный топик хранения контрольных точек (checkpoint) и арбитража пары. Должен существовать до запуска: 1 партиция — обязательно, `cleanup.policy=compact` — рекомендуется. Один топик общий на несколько слотов и сервисов.
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
