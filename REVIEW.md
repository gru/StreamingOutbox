# Карта ревью outbox-сервиса

Порядок: сверху вниз по управлению, затем по потоку данных — точка входа и DI-инвентаризация → оболочка лидерства → конвейер → публикация/checkpoint → жизненный цикл старта → поперечные срезы → тесты.

Отмечай `[x]` по ходу ревью.

**Главный инвариант всего ревью:** ни checkpoint, ни подтверждение LSN слота никогда не опережают подтверждённую Kafka-доставку; всё остальное — следствия этого.

## Этап 0. Контекст (~30 минут, обязательно)

Код вторичен относительно документов — без них ревью будет поверхностным.

- [ ] `docs/implementation-plan.md` — принятые задачи (чек-лист в начале) и что проверялось на живом стенде
- [ ] `docs/recommendations.md` — конвейер `WAL Read → Kafka Send → Kafka ACK → Durable Checkpoint → LSN Flush`, Active-Passive через Kafka Consumer Group, fencing, Recovery Mode, согласование checkpoint со слотом
- [ ] `docs/outbox-table.md` — контракт `OutboxMessage`, заголовки, «Подтверждение LSN и checkpoint»
- [ ] `docs/dependencies.md` §2 — конфигурация сервиса, грабли Npgsql/Confluent.Kafka

## Этап 1. Точка входа и состав (~15 минут)

- [x] `src/CdcOutbox.Host/Program.cs` — ASP-часть (health checks, `/metrics`), `ShutdownTimeout`, вызов `AddCdcOutbox`
- [x] `src/CdcOutbox.Core/CdcOutboxServiceCollectionExtensions.cs` — **главная карта кода**: вся DI-регистрация в одном методе; выписать: кто singleton, кто scoped (период лидерства), какие интерфейсы во что резолвятся — инвентарь для всех следующих этапов
- [x] `src/CdcOutbox.Core/Configuration/` — 7 классов; сверить пути секций с env-префиксом `CDCOUTBOX__`

## Этап 2. Внешняя оболочка — лидерство (~45 минут)

Порядок: `LeadershipArbiter` → `LeadershipCoordinator` → `Fencing` → `LeadershipLostException`.

- [x] `src/CdcOutbox.Core/Redundancy/LeadershipArbiter.cs` — единственный hosted-сервис; контрольная consumer group `cdc-outbox-<slot>` на `cdc-checkpoints`; формула группы; session 15 с / poll 1 с всегда
- [x] `src/CdcOutbox.Core/Redundancy/LeadershipCoordinator.cs` — машина состояний: назначение партиции → `RegisterFailover` → `InitTransactions` **только** при лидерстве → стартовые проверки → цикл доставки в своём DI-скоупе → `is_active=1`; кто создаёт/dispose'ит скоуп
- [x] `src/CdcOutbox.Core/Redundancy/Fencing.cs` — распознавание `ProducerFenced`/`InvalidProducerEpoch`/`InvalidProducerIdMapping` (в .NET-клиенте это всё `KafkaException` без отдельного типа — матчить по кодам, самое хрупкое место)
- [x] `src/CdcOutbox.Core/Redundancy/LeadershipLostException.cs`
- [x] Пути выхода: отзыв партиций vs терминальная ошибка vs восстановимая с backoff — три разных судьбы хоста

## Этап 3. Ядро конвейера (~1 час)

Порядок: `OutboxProcessor` → `ReplicationProgress` → `IOutboxSlotReader` → `OutboxSlotReader` → `OutboxRowParser` + `OutboxColumnPlan` → `OutboxMessage`.

- [ ] `src/CdcOutbox.Core/Processing/OutboxProcessor.cs` — bounded-канал (`QueueCapacity`) как backpressure; сбой публикации отменяет чтение связанным CTS (проверить на утечку незаблюдаемых задач)
- [ ] `src/CdcOutbox.Core/Processing/ReplicationProgress.cs` — монотонность `MarkRead/MarkDelivered/MarkCheckpointed` (Interlocked-max); `MarkDelivered` только при полном успехе батча
- [ ] `src/CdcOutbox.Core/Replication/IOutboxSlotReader.cs` + `OutboxSlotReader.cs` — буферизация до `CommitMessage` (LSN коммита известен только там); graceful stop — разрыв стрима **после** завершающего handshake (грабля Npgsql: `SendStatusUpdate` работает только пока операция репликации активна); классификация ошибок — терминальная только инвалидированный слот
- [ ] `src/CdcOutbox.Core/Replication/ReadingIssue.cs` — классификация терминальных/транзиентных ошибок чтения
- [ ] `src/CdcOutbox.Core/Replication/OutboxRowParser.cs` + `OutboxColumnPlan.cs` — план колонок один раз на relation (кэш), спан-парсинг, `ParseBinary` по OID; на сообщение аллоцируются только `OutboxMessage` + байты Key/Body
- [ ] `src/CdcOutbox.Core/OutboxMessage.cs` — сверить контракт с outbox-table.md дословно

## Этап 4. Публикация и checkpoint (~1 час)

Порядок: `OutboxPublisher` → `OutboxProducerConfig` → `CheckpointStore` → `LsnText`.

- [ ] `src/CdcOutbox.Core/Kafka/OutboxPublisher.cs` — батч ≤ `BatchSize`, один батч in-flight; delivery-репорт = момент метрик; провал → упорядоченная in-memory retry-очередь (дренируется первой); headers — JSON-объект строк, битые → консервативная остановка
- [ ] Единый последовательный цикл транзакций: Begin → produce → delivery → (каденс истёк) checkpoint-запись в той же транзакции → Commit; частичный сбой — доставленная часть commit'ится
- [ ] `src/CdcOutbox.Core/Kafka/OutboxProducerConfig.cs` — `transactional.id = cdc-outbox-<slot>`, `transaction.timeout.ms = DeliveryTimeoutMs+60 000`, `HeartbeatIntervalMs = SessionTimeoutMs/3`, `ParseAcks`, mTLS из X509Store по thumbprint
- [ ] `src/CdcOutbox.Core/Kafka/CheckpointStore.cs` — ключ=слот/партиция 0; `ReadLatest` — assign-only консюмер с `read_committed` (записи прерванных транзакций невидимы)
- [ ] `src/CdcOutbox.Core/Replication/LsnText.cs` — Format/TryParse «X/Y»

## Этап 5. Жизненный цикл старта (~45 минут)

Порядок: `IOutboxStartupChecks` → `OutboxStartupChecks` → `OutboxSlotProvisioner` → `PublicationVerifier` + `CheckpointTopicVerifier` → `CheckpointReconciler` → `RecoveryPlanner`.

- [ ] `src/CdcOutbox.Core/Processing/OutboxStartupChecks.cs` — порядок в `RunAllAsync` (только лидер): верификация topic → чтение checkpoint → reconciler **до** создания слота → предохранитель `pg_is_in_recovery` → идемпотентное обеспечение слота → publication
- [ ] `src/CdcOutbox.Core/Replication/OutboxSlotProvisioner.cs` — создание (pgoutput, failover=true) vs существующий (плагин жёстко, failover → WARN); гонка 42710 → перевалидация
- [ ] `src/CdcOutbox.Core/Replication/PublicationVerifier.cs` + `src/CdcOutbox.Core/Kafka/CheckpointTopicVerifier.cs` — pubinsert + вхождение таблицы; наличие topic по имени
- [ ] `src/CdcOutbox.Core/Processing/CheckpointReconciler.cs` — «пересоздание запрещено», пустой confirmed_flush, опережение checkpoint'ом → WARN + продолжение
- [ ] `src/CdcOutbox.Core/Processing/RecoveryPlanner.cs` + `RecoveryDecision.cs` — NotNeeded/AdoptSlotPosition/CreateSlot/Reject
- [ ] Терминальные расхождения → `StartupCheckException` → остановка хоста (проверить каждый путь)

## Этап 6. Поперечные срезы (~30 минут)

- [ ] `src/CdcOutbox.Core/Processing/IdleActivityTracker.cs` — WARN не чаще раза за `IdleTimeoutSeconds`; сбрасывается только outbox-транзакциями
- [ ] `src/CdcOutbox.Core/Processing/RetryBackoff.cs` — база → удвоение → потолок; сброс по длительной сессии
- [ ] `src/CdcOutbox.Core/Metrics/OutboxMetrics.cs` — сверить с таблицей `docs/metrics.md`: единый label `slot`, нейминг `outbox_<подсистема>_<показатель>`, все 14 метрик

## Этап 7. Тесты как карта поведения

Читать параллельно с этапами 3–5 — исполняемая спецификация.

- [ ] `tests/CdcOutbox.UnitTests/CheckpointReconcilerTests.cs`, `RecoveryPlannerTests.cs`, `StartupChecksTests.cs` — таблицы решений
- [ ] `tests/CdcOutbox.UnitTests/OptionsTests.cs` — валидация конфигурации
- [ ] `tests/CdcOutbox.UnitTests/LeadershipCoordinatorTests.cs` — машина состояний лидерства
- [ ] `tests/CdcOutbox.UnitTests/OutboxRowParserTests.cs` — в т.ч. аллокационный guard
- [ ] `tests/CdcOutbox.IntegrationTests/CheckpointPipelineTests.cs` — порядок конвейера
- [ ] `tests/CdcOutbox.IntegrationTests/GracefulStopTests.cs` — confirmed_flush == checkpoint
- [ ] `tests/CdcOutbox.IntegrationTests/RecoveryModeTests.cs` — окно пропуска, WARN, gauge
- [ ] `tests/CdcOutbox.IntegrationTests/TransactionalProducerTests.cs` — fencing
- [ ] `tests/CdcOutbox.IntegrationTests/ArbiterTests.cs` — выборы лидера
- [ ] Прогон: `scripts/test.sh` (283 теста)
- [ ] Приёмочный: `scripts/test/integration-delivery.sh` (end-to-end с отказами)

## Чек-лист ревьюера (сквозные инварианты)

- [ ] **Инвариант порядка**: ACK → checkpoint → LSN — нигде не нарушен (особенно пути ошибок и graceful stop)
- [ ] **Монотонность** всех водяных знаков на любых пересечениях потоков
- [ ] **Fencing-коды** — матчинг по строкам/кодам `KafkaException`, нет отдельного типа
- [ ] **Отмена/dispose**: связанный CTS чтения, скоупы лидерства, незаблюдаемые задачи
- [ ] **`TimeProvider`**: ни одного прямого `DateTime.UtcNow`/`DateTimeOffset.UtcNow` в src
- [ ] **Терминальность vs транзиентность**: каждая ошибка где-то классифицирована; «тихих пропусков» нет
