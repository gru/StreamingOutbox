# Карта чтения кода outbox-сервиса

Порядок чтения: сверху вниз по управлению, затем по потоку данных — точка входа и DI-инвентаризация → оболочка лидерства → конвейер → публикация и контрольные точки → жизненный цикл старта.

**Главный инвариант:** ни checkpoint, ни подтверждение LSN слота никогда не опережают подтверждённую Kafka-доставку; всё остальное — следствия этого.

## 1. Точка входа и состав

- `src/CdcOutbox.Host/Program.cs` — ASP-часть (health checks, `/metrics`), `ShutdownTimeout`, вызов `AddCdcOutbox`.
- `src/CdcOutbox.Core/CdcOutboxServiceCollectionExtensions.cs` — **главная карта кода**: вся DI-регистрация в одном методе; выписать, кто singleton, кто scoped (период лидерства), какие интерфейсы во что резолвятся — инвентарь для всех следующих пунктов.
- `src/CdcOutbox.Core/Configuration/` — options-классы: пути секций, валидация, нормализация идентификаторов.

## 2. Оболочка лидерства

Порядок: `LeadershipArbiter` → `LeadershipCoordinator` → `Fencing` → `LeadershipLostException`.

- `src/CdcOutbox.Core/Redundancy/LeadershipArbiter.cs` — единственный hosted-сервис; контрольная consumer group `cdc-outbox-<slot>` на `cdc-checkpoints`; session 15 с / poll 1 с всегда.
- `src/CdcOutbox.Core/Redundancy/LeadershipCoordinator.cs` — машина состояний: назначение партиции → `RegisterFailover` → `InitTransactions` **только** при лидерстве → стартовые проверки → цикл доставки в собственном DI-скоупе → `is_active=1`; кто создаёт и dispose'ит скоуп.
- `src/CdcOutbox.Core/Redundancy/Fencing.cs` — распознавание `ProducerFenced`/`InvalidProducerEpoch`/`InvalidProducerIdMapping`: в .NET-клиенте это всё `KafkaException` без отдельного типа, матчинг по кодам — самое хрупкое место.
- Пути выхода: отзыв партиций vs терминальная ошибка vs восстановимая с backoff — три разные судьбы хоста.

## 3. Ядро конвейера

Порядок: `OutboxProcessor` → `ReplicationProgress` → `IOutboxSlotReader`/`OutboxSlotReader` → `OutboxRowParser` + `OutboxColumnPlan` → `OutboxMessage`.

- `src/CdcOutbox.Core/Processing/OutboxProcessor.cs` — bounded-канал (`QueueCapacity`) как backpressure; сбой публикации отменяет чтение связанным CTS — смотреть на утечку незаблюдаемых задач.
- `src/CdcOutbox.Core/Processing/ReplicationProgress.cs` — монотонность `MarkRead`/`MarkDelivered`/`MarkCheckpointed` (Interlocked-max); `MarkDelivered` только при полном успехе батча.
- `src/CdcOutbox.Core/Replication/OutboxSlotReader.cs` — буферизация до `CommitMessage` (LSN коммита известен только там); graceful stop — разрыв стрима **после** завершающего handshake (грабля Npgsql: `SendStatusUpdate` работает только пока операция репликации активна).
- `src/CdcOutbox.Core/Replication/ReadingIssue.cs` — классификация ошибок чтения: терминальная только инвалидированный слот, остальное транзиентное.
- `src/CdcOutbox.Core/Replication/OutboxRowParser.cs` + `OutboxColumnPlan.cs` — план колонок строится один раз на relation; спан-парсинг; `ParseBinary` по OID; на сообщение аллоцируются только `OutboxMessage` и байты Key/Body.
- `src/CdcOutbox.Core/OutboxMessage.cs` — внутреннее представление строки outbox.

## 4. Публикация и контрольные точки

Порядок: `OutboxPublisher` → `OutboxProducerConfig` → `CheckpointStore` → `LsnText`.

- `src/CdcOutbox.Core/Kafka/OutboxPublisher.cs` — батч ≤ `BatchSize`, один батч in-flight; delivery-репорт — момент метрик; провал → упорядоченная in-memory retry-очередь, дренируется первой; headers — JSON-объект строк, битые → консервативная остановка публикации.
- Единый последовательный цикл транзакций: `BeginTransaction` → produce → delivery-репорты → (каденс истёк) checkpoint-запись в той же транзакции → `Commit`; частичный сбой — доставленная часть commit'ится.
- `src/CdcOutbox.Core/Kafka/OutboxProducerConfig.cs` — `transactional.id = cdc-outbox-<slot>`, `transaction.timeout.ms = DeliveryTimeoutMs + 60 000`, `HeartbeatIntervalMs = SessionTimeoutMs / 3`, mTLS из X509Store по thumbprint.
- `src/CdcOutbox.Core/Kafka/CheckpointStore.cs` — ключ = слот, партиция 0; `ReadLatest` — assign-only консюмер с `read_committed`: записи прерванных транзакций невидимы.
- `src/CdcOutbox.Core/Replication/LsnText.cs` — Format/TryParse «X/Y».

## 5. Жизненный цикл старта

Порядок: `IOutboxStartupChecks` → `OutboxStartupChecks` → `OutboxSlotProvisioner` → `PublicationVerifier` + `CheckpointTopicVerifier` → `CheckpointReconciler` → `RecoveryPlanner`.

- `src/CdcOutbox.Core/Processing/OutboxStartupChecks.cs` — порядок в `RunAllAsync` (выполняет только лидер): верификация topic → чтение checkpoint → согласование **до** создания слота → предохранитель `pg_is_in_recovery` → идемпотентное обеспечение слота → publication.
- `src/CdcOutbox.Core/Replication/OutboxSlotProvisioner.cs` — создание (pgoutput, failover=true) vs существующий (плагин жёстко, failover → WARN); гонка параллельного создания 42710 → перевалидация.
- `src/CdcOutbox.Core/Replication/PublicationVerifier.cs` + `src/CdcOutbox.Core/Kafka/CheckpointTopicVerifier.cs` — pubinsert и вхождение outbox-таблицы; наличие topic по имени.
- `src/CdcOutbox.Core/Processing/CheckpointReconciler.cs` — «пересоздание запрещено», пустой `confirmed_flush`, опережение checkpoint'ом → WARN и продолжение.
- `src/CdcOutbox.Core/Processing/RecoveryPlanner.cs` + `RecoveryDecision.cs` — NotNeeded / AdoptSlotPosition / CreateSlot / Reject.
- Терминальные расхождения → `StartupCheckException` → остановка хоста — пройти каждый путь.
