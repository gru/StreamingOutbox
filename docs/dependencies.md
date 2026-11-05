# dependencies.md — стек, схема и канонические SQL-примеры

Единый источник правды о версиях и схеме для сервиса `streaming-outbox` (чтение outbox-сообщений из PostgreSQL через слот логической репликации, публикация в Kafka).

Версии взяты из проверенной реализации исследовательского стенда `streaming-polling-outbox` как стартовая точка — дальше правим здесь, затем в коде/инфраструктуре.

---

## 1. Инфраструктурные зависимости

| Компонент | Версия | Образ / источник |
|---|---|---|
| PostgreSQL | **17.2** | `postgres:17.2` — базовый образ собственной сборки Patroni |
| Patroni | **4.1.5** | собственная сборка: `pip3 install "patroni[etcd3]==4.1.5" "psycopg2-binary==2.9.10"` поверх `postgres:17.2` (нативный arm64, прозрачный patroni.yml) |
| etcd (DCS) | **3.5.17** | `gcr.io/etcd-development/etcd:v3.5.17`, кластер ×3 |
| HAProxy | **3.0.30** | `haproxy:3.0.30-alpine` — маршрутизация на текущий primary (5432) |
| Apache Kafka (KRaft, без ZooKeeper) | **3.9.0** | `apache/kafka:3.9.0`, кластер ×3 |
| Prometheus | **2.54.1** | `prom/prometheus:v2.54.1` |
| postgres-exporter | **0.15.0** | `prometheuscommunity/postgres-exporter:v0.15.0` (имя с **дефисом**; вариант с подчёркиванием не существует) |
| kafka-exporter | **1.8.0** | `danielqsj/kafka-exporter:v1.8.0` |
| Оркестрация | Docker Compose | всё строго в Docker Compose |

Замечания к инфраструктуре:

- **Kafka-топики** (создаются явно kafka-init; `auto.create.topics.enable=false` — сервисы сами топики не создают): `outbox.events` — события доставки (6 партиций, replication factor 3, `min.insync.replicas=2`); `cdc-checkpoints` — служебный checkpoint-стор outbox-сервиса (контракт создания и записи — подраздел «Топик `cdc-checkpoints`» ниже).
- Все подключения к PostgreSQL — **напрямую через HAProxy** (5432): и сервис стриминговой репликации (репликационный протокол), и Writer (INSERT-нагрузка). PgBouncer в стенде не используется (решение владельца).
- Мониторинг стенда: Prometheus + postgres-exporter + kafka-exporter. cAdvisor **не используется** (решение владельца при переносе стенда): на Docker Desktop с containerd image store он видит только VM-root, per-container серий нет. postgres-exporter подключается от роли `sys_monitor` (`pg_monitor` с `INHERIT TRUE`, `CONNECT` только на БД `outbox`, без REPLICATION) напрямую через HAProxy; кастомные запросы — `infrastructure/prometheus/postgres-exporter-queries.yaml` (WAL, слоты, репликация). Состав стенда, порты и команды — `docs/stand.md`.
- Patroni permanent replication slots для logical-слотов **не используются** — failover-слоты синхронизирует сам PostgreSQL 17.

### Топик `cdc-checkpoints`: контракт создания и записи

Создаётся **kafka-init** при подъёме стенда (`--create --if-not-exists`; автосоздание топиков на брокерах отключено — сам сервис топик не создаёт, только проверяет наличие при старте). Параметры создания обязательны — каждый обоснован контрактом checkpoint-стора:

| параметр | значение | зачем |
|---|---|---|
| `partitions` | **1** | все записи одного слота — в одной партиции: код пишет и читает явно партицию 0, чтение последнего checkpoint — скан партиции с начала. Однопартиционность — текущее допущение одного слота на топик; общий топик на несколько слотов/сервисов и несколько партиций — задача 15 implementation-plan.md |
| `replication-factor` | **3** | пережить отказ одного брокера из трёх без потери подтверждённых записей |
| `min.insync.replicas` | **2** | вместе с `acks=all` продюсера сервиса запись подтверждается только при попадании в две синхронные реплики — подтверждённый checkpoint не теряется при отказе одного брокера |
| `cleanup.policy` | **`compact`** (только compact) | хранение по ключу, а не по времени/размеру — топик не растёт при каденции сохранения (по умолчанию 5 с). Смешанный `compact,delete` или retention по времени запрещены: удаление по времени молча стёрло бы ключ слота вместе с последним checkpoint |

Контракт записи (код `CheckpointStore`, контракт `outbox-table.md` «Подтверждение LSN и checkpoint»):

- **ключ** — имя слота (UTF-8), обязателен: по ключу идёт очистка журнала (compaction) и поиск последнего значения при старте; запись без ключа не очищаетсялась бы и не находилась;
- **партиция** — явно 0 (см. `partitions`);
- **значение** — JSON camelCase `{"slotName","lsn","savedAtUtc"}` (чтение безразлично к регистру имён); битое значение — явная ошибка старта с offset записи;
- **checkpoint считается сохранённым после ACK записи** (`acks=all` + `min.insync.replicas=2`).

Правила эксплуатации:

- чтение **не полагается на очистку журнала**: при старте партиция сканируется с начала и берётся последняя запись ключа слота; очистка журнала — только экономия места (несжатые дубликаты могут существовать до отработки log cleaner'а — это норма, корректности не влияет);
- пересоздание топика (сброс checkpoint-стора) безопасно для корректности: позиция доставки хранится в `confirmed_flush` слота, сервис продолжит с неё (пере-доставка окна неподтверждённого — норма at-least-once); процедура сброса — `docs/stand.md`;
- сегменты и прочие настройки — дефолты брокера, отдельных требований нет.

---

## 2. Зависимости сервиса (стриминговая репликация)

Версии зафиксированы решением владельца (2026-10-21): Npgsql 9.0, OpenTelemetry 1.11, prometheus-net 5.0, Kafka-клиент — Confluent.Kafka 2.3 (единый для всего решения). Точные версии патчей: Npgsql **9.0.x**, prometheus-net.AspNetCore **5.0.2**, Confluent.Kafka **2.3.0** (librdkafka.redist 2.3.0 — транзитивно); патчи OTel-линии 1.11 зафиксированы при создании скелета сервиса: OpenTelemetry.Extensions.Hosting **1.11.2**, OpenTelemetry.Instrumentation.Runtime **1.11.1** (последние стабильные линии 1.11). Стартовые версии исследовательского стенда (Npgsql 8.0.9, OTel 1.9.0, экспорт метрик через `OpenTelemetry.Exporter.Prometheus.AspNetCore` 1.9.0-beta.2, KafkaFlow 4.2.0 + `KafkaFlow.Extensions.Hosting`, Confluent.Kafka 2.8.0 транзитивно) заменены.

**Отказ от KafkaFlow (решение владельца 2026-10-22):** outbox-сервис использует только Confluent.Kafka напрямую. Причина: fencing лидера на служебном topic `cdc-checkpoints` требует транзакционного продюсера с постоянным `transactional.id` (recommendations.md «Балансировка CDC-экземпляров через Kafka»), а KafkaFlow не предоставляет доступ к Kafka-транзакциям ни в одной линии — feature request [Farfetch/kafkaflow#566](https://github.com/Farfetch/kafkaflow/issues/566) открыт с 2024-04-29 без реакции мейнтейнеров; проверено рефлексией сборки 3.0.10: в публичном API нет ни одного транзакционного члена (`IMessageProducer` — только `Produce`/`ProduceAsync`), обёрнутый `Confluent.Kafka.IProducer` наружу не экспортируется. Confluent.Kafka 2.3.0 (проверено той же рефлексией) содержит полный транзакционный набор: `InitTransactions` / `BeginTransaction` / `CommitTransaction` / `AbortTransaction` / `SendOffsetsToTransaction`. Пакеты KafkaFlow-тройки удалены из скелета `CdcOutbox.Core`.

**KafkaFlow удалён и из KafkaValidator (решение владельца 2026-10-27):** консюмер валидатора на KafkaFlow 3.0.10 поверх Confluent.Kafka 2.3.0 назначал все партиции группы и не доставлял ни одной записи — ни накопленных, ни новых (наблюдалось на стенде: `records_consumed=0` при 505 записях в topic; воспроизведено минимальным пробником на «чистом» KafkaFlow 3.0.10 с идентичным ConsumerConfig — Earliest + `EnableAutoCommit=false` + `EnableAutoOffsetStore=false`; при этом сырой Confluent.Kafka 2.3.0 с той же конфигурацией в четырёх вариантах, включая `Consume(CancellationToken)` и хендлеры назначения партиций, потребляет исправно). Валидатор переведён на сырой `IConsumer<byte[], byte[]>` (`ValidatorConsumerService`), KafkaFlow-пакеты удалены из решения полностью; семантика потребления сохранена: группа `kafka-validator`, `AutoOffsetReset=Earliest`, оффсеты никогда не коммитятся — каждый старт перематывает topic с начала.

| Пакет | Версия | Назначение |
|---|---|---|
| .NET | **8.0** (`net8.0`) строго | ASP.NET Core 8; сборка/тесты в `mcr.microsoft.com/dotnet/sdk:8.0` |
| Npgsql | **9.0** | Logical Replication API: `PgOutputReplicationSlot` + `StartReplication(...)` — pgoutput proto v1, типизированные сообщения; поддерживает `net8.0` |
| AspNetCore.HealthChecks.Npgsql | **9.0** | чек готовности `/ready` Writer'а: `SELECT 1` через общий с записью `NpgsqlDataSource` (фабрика чека резолвится лениво — пул один), таймаут регистрации 3 с; 9.0.0 таргетирует net8.0 (проверено) |
| Confluent.Kafka | **2.3** | единственный Kafka-клиент решения: outbox-сервис (идемпотентный продюсер событий `acks=all`, будущие транзакционный продюсер checkpoint'ов `cdc-checkpoints` и консюмер control-группы) и консюмер валидатора; см. «Отказ от KafkaFlow» выше |
| librdkafka.redist | **2.3** | нативные бинарники librdkafka (в т.ч. linux/arm64); приходит **транзитивно** от Confluent.Kafka 2.3 — явного пина в проектах нет (решение владельца 2026-10-22) |
| OpenTelemetry.Extensions.Hosting | **1.11** | метрики OTel (Meter/Counter/Gauge) |
| OpenTelemetry.Instrumentation.Runtime | **1.11** | runtime-метрики (.NET) |
| prometheus-net (`prometheus-net.AspNetCore`) | **5.0** | экспорт `/metrics` (pull, Prometheus) — вместо `OpenTelemetry.Exporter.Prometheus.AspNetCore`; линия 5.x (текущая у автора библиотеки — 8.x), выбрана решением владельца |
| `prometheus-net` (клиент) | **5.0** | `Metrics.CreateGauge/CreateCounter/CreateHistogram` для класса `OutboxMetrics` в `CdcOutbox.Core` (5.0.2; тот же пакет транзитивно тянет `prometheus-net.AspNetCore` в Host — версии линий совпадают) |
| `Microsoft.Extensions.Hosting.Abstractions` | **8.0** | `BackgroundService`/`IHostedService` каркаса процесса в `CdcOutbox.Core` (8.0.1; в Host входит в shared framework 8) |
| `Microsoft.Extensions.Options` | **8.0** | потребление `IOptions<T>` классами `CdcOutbox.Core` (8.0.2; в Host входит в shared framework 8) |
| Shouldly | **4.3.0** | ассерты в тестах — все четыре тестовых проекта решения (решение владельца 2026-10-22; `[Fact]`/`[Theory]` остаются от xUnit) |

Замечания к сервису:

- Тестовый контур — Writer и KafkaValidator — самостоятельные сервисы: **общего кода с outbox-сервисом и между собой не имеют**. Зависимости Writer — Npgsql 9.0.x + AspNetCore.HealthChecks.Npgsql 9.0.0 (health checks `/ready`); KafkaValidator — Confluent.Kafka 2.3.0 (сырой консюмер, см. «Отказ от KafkaFlow»); метрики обоих — через prometheus-net 5.0 (`Metrics.Create*`, без OTel). Пакеты OpenTelemetry (1.11) закреплены за outbox-сервисом (стриминговый процессор) — подключены в его скелете (`CdcOutbox.Host`).
- Скелет outbox-сервиса (имена — решение владельца): `src/CdcOutbox.Host` (хост, Web SDK: `/health`, `/ready`, `/metrics`; пакеты хостинга — AspNetCore.HealthChecks.Npgsql 9.0.0, OTel 1.11.2/1.11.1, prometheus-net.AspNetCore 5.0.2) и `src/CdcOutbox.Core` (библиотека под всю логику стриминга: Npgsql 9.0.5 + Confluent.Kafka 2.3.0 (librdkafka.redist 2.3.0 — транзитивно, без явного пина) — единственный Kafka-клиент, KafkaFlow исключён — + prometheus-net 5.0.2 + Microsoft.Extensions.Hosting.Abstractions 8.0.1) + `tests/CdcOutbox.UnitTests` (ссылается на оба). Dockerfile хоста собирается с контекстом `./src` (обе csproj в restore-слое; `src/.dockerignore` в корне контекста). env-префикс конфигурации — `CDCOUTBOX__` (одним словом: одинарные подчёркивания env-провайдер не заменяет на `:` — грабля `CDC_POLLING__` старого стенда); конфигурационные параметры — подраздел «Конфигурация outbox-сервиса» ниже. Compose (зафиксировано, docs/stand.md): **два экземпляра `cdc-outbox-1`/`cdc-outbox-2` — Active-Passive резервирование** (арбитр — Kafka Consumer Group на служебном topic `cdc-checkpoints`: активный один — читает слот и публикует, второй в standby; переключение — rebalance → fencing transactional producer → checkpoint из `cdc-checkpoints` → ожидание освобождения слота; эксклюзивность слота — последняя линия защиты; алгоритм — docs/recommendations.md «Балансировка CDC-экземпляров через Kafka», модель стенда — docs/stand.md), общий образ `streaming-outbox/cdc-outbox:net8.0`, host-порты 18084/18085; **cdc-outbox-1 — always-on, cdc-outbox-2 закрыт compose-профилем `redundancy`** (второй экземпляр включим вместе с реализацией резервирования — решение владельца: `docker compose --profile redundancy up -d --no-deps cdc-outbox-2` + возврат таргета в prometheus.yml + рестарт prometheus; wipe.sh уже чистит его с флагом профиля).
- Health checks: Writer и KafkaValidator — встроенный фреймворк ASP.NET Core (`AddHealthChecks` + `MapHealthChecks`; `Microsoft.Extensions.Diagnostics.HealthChecks` входит в shared framework 8, отдельного пина нет) c ResponseWriter-контрактом `{"status":…}`; `/health` — liveness без чеков (`Predicate = _ => false`). cdc-outbox (решение владельца): `/health` — liveness без чеков, `/ready` — postgres-чек из AspNetCore.HealthChecks.Npgsql (`SELECT 1` от `sys_replication` через общий connection string, таймаут 3 с) с **дефолтным форматтером фреймворка** (plain-text агрегированный статус: `Healthy`/`Unhealthy`; JSON-писателя в коробке нет — сниппет из документации MS остаётся кастомным кодом). Чек состояния процессора сознательно не вводится: в Active-Passive standby-экземпляр не держит цикл доставки, но экземпляр здоров — готовность ≠ лидерство (активность отслеживает метрика `outbox_processor_is_active`, metrics.md), а при падении цикла .NET 8 останавливает хост (`BackgroundServiceExceptionBehavior.StopHost`) и liveness гаснет сам. Экосистемный AspNetCore.HealthChecks.Kafka сознательно не используется: его проверка — produce-раундтрип (пишет тестовое сообщение в topic при каждом пробе), для консюмера это side-effect в кластере и путь продюсера, а не консюмера. В тестах — Microsoft.Extensions.Diagnostics.HealthChecks 8.0.11 (test-only), ассерты — Shouldly 4.3.0.
- pgoutput в Npgsql (актуально и для 8.x, и для 9.x) отдаётся **только типизированными сообщениями** (сырого `XLogDataMessage` для pgoutput в публичном API нет).
- Значения колонок pgoutput приходят текстом и размечаются как `text`: читать `Get<string>()` и парсить самому (`Guid.TryParse`, парсинг timestamp) — `Get<Guid>()`/`Get<DateTime>()` кидают `InvalidCastException`.
- Подтверждение позиции — `SetReplicationStatus(TransactionEndLsn)` + `SendStatusUpdate()` **только после Kafka delivery и подтверждённо сохранённого checkpoint** в `cdc-checkpoints` (at-least-once; контракт — `outbox-table.md` «Подтверждение LSN и checkpoint»). Механика Npgsql: автоматически серверу отправляется только received-позиция (таймер ~10 с и keepalive-ответы); flush-позиция — та, которой движется `confirmed_flush` логического слота, — регистрируется только явным `SetReplicationStatus`, `SendStatusUpdate` выталкивает её немедленно.
- Checkpoint-запись (задача 5): ключ — имя слота, партиция 0 (однопартиционное допущение — общий топик на несколько слотов/сервисов и многопартиционность выделены в задачу 15 плана), значение — JSON `{"slotName","lsn","savedAtUtc"}`; продюсер общий с событиями; сохранена = ACK записи. Чтение при старте — консюмером с явным назначением партиции 0 с начала до границы (очистка журнала по ключу хранит последнюю запись на ключ; группы и офсеты не используются, фиктивный group.id требуется клиенту). Согласование при старте сравнивает checkpoint с `confirmed_flush` слота **на сервере** (`pg_wal_lsn_diff`) — сервис не парсит серверный текст pg_lsn (формат вывода зависит от версии/реализации: PG ≤17 `%X/%X`, PG 18+ `%X/%08X`; собственный формат `LsnText` сервис пишет и читает только сам, парсер толерантен к ведущим нулям).
- Публикация в Kafka — напрямую клиентом Confluent.Kafka из внутреннего представления `OutboxMessage` (`outbox-table.md`); обязательный интерфейс-контракт транспорта (`IOutboxMessagesProcessorTransport`) сознательно не вводится (решение владельца: абстракция стесняет реализацию — батчинг, delivery-репорты, транзакционный продюсер checkpoint'ов).
- Стартовый блок проверки объектов (задача 2 implementation-plan.md): hosted-служба `OutboxStartupChecks` стартует до цикла процессора (порядок регистрации hosted-служб). Порядок проверок: (1) `pg_is_in_recovery()` — соединение не с primary → явная ошибка (слот создаётся и потребляется только на primary; в PG17 вызов `pg_create_logical_replication_slot` на standby **не завершается** — сессия виснет, слот при этом виден другим сессиям существующим и active, проверено на стенде); (2) идемпотентное обеспечение слота `OutboxSlotProvisioner` — отсутствующий создаётся `pg_create_logical_replication_slot(..., failover => true)` (достаточно атрибута REPLICATION роли), существующий проверяется по плагину `pgoutput` (расхождение — ошибка, молчаливое пересоздание исключено); слот без failover принимается с WARN (решение владельца: failover нужен только чтобы пережить promotion, чтению не требуется; цена — после смены primary слота нет, продолжение с текущей позиции), гонка параллельного создания вторым экземпляром (профиль redundancy) — SQLSTATE 42710 → перечитать и провалидировать; ошибка создания 55000+«wal_level» (wal_level < logical) транслируется в понятное сообщение; (3) publication — имя, публикация INSERT (без INSERT события не попадут в слот) и вхождение outbox-таблицы; публикация других операций — WARN без блокировки (решение владельца: лишние записи декодируются, но не обрабатываются); publication создаёт init (§4); (4) checkpoint-topic — наличие по метадате кластера `IAdminClient.GetMetadata` (создаёт kafka-init — §1); (5) согласование checkpoint со слотом (задача 5) — **до** создания слота: живой checkpoint + отсутствие слота → ошибка (пересоздание запрещено — новый слот начал бы с текущей позиции WAL), опережение checkpoint'ом `confirmed_flush` при живом слоте — WARN и продолжение с позиции слота (фикс 2026-11-04; обоснование всех вариантов — recommendations.md «Согласование checkpoint-стора с replication slot при старте»), иначе продолжение с позиции слота. Проверки выполняет лидер при получении лидерства (задача 10, `LeadershipCoordinator`). Классификация ошибок запуска: терминальные нарушения (нет слота при живом checkpoint, пустой `confirmed_flush`, чужой плагин слота, отсутствие publication/checkpoint-topic, `wal_level` < logical, битая запись стора) — исключение `StartupCheckException`, хост останавливается (рестарт-политики нет — подъём явным `docker compose start`, группа перебалансируется, доставку принимает standby); восстановимые ошибки (сеть к PG/Kafka, недоступность метадаты кластера, чтение checkpoint-стора, окно PG failover — `pg_is_in_recovery`) — повторы с backoff `RetryDelaySeconds`/`MaxRetryDelaySeconds` с удержанием лидерства, каждая попытка со свежим DI-скоупом (2026-11-05, заказ владельца: хост не останавливается на ошибках, восстановимых в перспективе; контракт — outbox-table.md «Параллельные экземпляры»).

### Конфигурация outbox-сервиса (env `CDCOUTBOX__*`)

env-префикс — `CDCOUTBOX__` одним словом (двойное подчёркивание env-провайдер заменяет на `:` — грабля `CDC_POLLING__` старого стенда); параметры биндятся в секции `CdcOutbox:*` options-классов `CdcOutbox.Core`. Подключение к PostgreSQL задаётся **единой строкой подключения** (решение владельца): любые параметры Npgsql — SSL, keepalives, `Pooling`, `Application Name` (наблюдаемость в `pg_stat_replication`) и т.п. — задаются строкой без расширения конфигурации и кода; пароль живёт в самой строке (compose подставляет `${SYS_REPLICATION_PASSWORD}` интерполяцией).

| env | Свойство | Дефолт | Назначение |
|---|---|---|---|
| `CDCOUTBOX__RECOVERYMODE` | `CdcOutboxOptions.RecoveryMode` | `false` | Recovery Mode на один старт: аварийное восстановление — старт от текущей позиции слота (отсутствующий слот создаётся с текущей позиции WAL), новый checkpoint сохраняется до начала чтения; события между позициями пропускаются осознанно (outbox-table.md «Recovery Mode»; ключ одним словом — одинарное подчёркивание binder не превращает в разделитель) |
| `CDCOUTBOX__POSTGRES__CONNECTIONSTRING` | `PostgresOptions.ConnectionString` | — (обязательно) | строка подключения Npgsql: маршрут к primary, база, роль, пароль и любые параметры драйвера — одной строкой (в стенде compose подставляет `Password=${SYS_REPLICATION_PASSWORD}`) |
| `CDCOUTBOX__REPLICATION__SLOTNAME` | `ReplicationOptions.SlotName` | — (обязательно) | failover-слот логической репликации (§5); имя — стабильный идентификатор checkpoint и `transactional.id` |
| `CDCOUTBOX__REPLICATION__PUBLICATIONNAME` | `ReplicationOptions.PublicationName` | — (обязательно) | publication (§4) |
| `CDCOUTBOX__STORAGE__SCHEMA` | `OutboxStorageOptions.Schema` | `outbox` | схема outbox-таблицы (outbox-table.md) |
| `CDCOUTBOX__STORAGE__TABLE` | `OutboxStorageOptions.Table` | `messages` | имя outbox-таблицы |
| `CDCOUTBOX__KAFKA__BOOTSTRAPSERVERS` | `KafkaOptions.BootstrapServers` | — (обязательно) | список bootstrap-серверов Kafka |
| `CDCOUTBOX__KAFKA__ENABLEIDEMPOTENCE` | `KafkaOptions.EnableIdempotence` | `true` | идемпотентный продюсер |
| `CDCOUTBOX__KAFKA__BATCHNUMMESSAGES` | `KafkaOptions.BatchNumMessages` | `10000` | батчинг продюсера (сообщений на батч) |
| `CDCOUTBOX__KAFKA__LINGERMS` | `KafkaOptions.LingerMs` | `5` | задержка накопления батча, мс |
| `CDCOUTBOX__KAFKA__DELIVERYTIMEOUTMS` | `KafkaOptions.DeliveryTimeoutMs` | `120000` | таймаут доставки (включая ретраи), мс |
| `CDCOUTBOX__KAFKA__MESSAGESENDMAXRETRIES` | `KafkaOptions.MessageSendMaxRetries` | `2147483647` | лимит ретраев отправки (фактически ограничен delivery timeout) |
| `CDCOUTBOX__KAFKA__MESSAGEMAXBYTES` | `KafkaOptions.MessageMaxBytes` | `1000000` | максимальный размер сообщения, байт |
| `CDCOUTBOX__KAFKA__CHECKPOINTTOPIC` | `KafkaOptions.CheckpointTopic` | `cdc-checkpoints` | checkpoint-стор (recommendations.md «Checkpoint Store в Kafka»); контрольная группа арбитража подписана на него же |
| `CDCOUTBOX__KAFKA__CONTROLGROUPID` | `KafkaOptions.ControlGroupId` | `cdc-outbox` | consumer-группа арбитража Active-Passive (задача 10): назначение партиции = лидерство; одинакова у всех экземпляров пары, уникальна в кластере; офсеты не читаются и не коммитятся |
| `CDCOUTBOX__KAFKA__CONTROLSESSIONTIMEOUTSECONDS` | `KafkaOptions.ControlSessionTimeoutSeconds` | `15` | таймаут сессии контрольной группы, с — время обнаружения внезапной смерти лидера (kill); graceful stop идёт через LeaveGroup мгновенно; должен попадать в границы окружения `group.min/max.session.timeout.ms` (§2b) |
| `CDCOUTBOX__KAFKA__CHECKPOINTREADTIMEOUTSECONDS` | `KafkaOptions.CheckpointReadTimeoutSeconds` | 30 | бюджет чтения последнего checkpoint при старте, с (резервная опция: нигде не задаётся, дефолт — контракт) |
| `CDCOUTBOX__KAFKA__CHECKPOINTPOLLTIMEOUTSECONDS` | `KafkaOptions.CheckpointPollTimeoutSeconds` | 2 | такт опроса консюмера при чтении checkpoint, с (резервная опция: нигде не задаётся, дефолт — контракт) |
| `CDCOUTBOX__KAFKA__SECURITYPROTOCOL` | `KafkaOptions.SecurityProtocol` | `plaintext` | протокол безопасности Kafka: `plaintext` (стенд) или `ssl` — mTLS, авторизация клиентским сертификатом |
| `CDCOUTBOX__KAFKA__SSLCALOCATION` | `KafkaOptions.SslCaLocation` | — | PEM-файл CA для проверки брокеров (режим `ssl`; на Windows допустимы `root`/`probe` — системное хранилище) |
| `CDCOUTBOX__KAFKA__SSLCERTIFICATELOCATION` | `KafkaOptions.SslCertificateLocation` | — | PEM-файл клиентского сертификата (режим `ssl`, источник «из файлов»; задаётся вместе с ключом) |
| `CDCOUTBOX__KAFKA__SSLKEYLOCATION` | `KafkaOptions.SslKeyLocation` | — | PEM-файл приватного ключа (режим `ssl`, источник «из файлов») |
| `CDCOUTBOX__KAFKA__SSLKEYPASSWORD` | `KafkaOptions.SslKeyPassword` | — | пароль приватного ключа, если зашифрован (источник «из файлов») |
| `CDCOUTBOX__KAFKA__SSLCERTIFICATETHUMBPRINT` | `KafkaOptions.SslCertificateThumbprint` | — | SHA-1 thumbprint клиентского сертификата в хранилище сертификатов (режим `ssl`, источник «из хранилища») |
| `CDCOUTBOX__KAFKA__SSLSTORENAME` | `KafkaOptions.SslStoreName` | `My` | имя хранилища сертификатов (источник «из хранилища») |
| `CDCOUTBOX__KAFKA__SSLSTORELOCATION` | `KafkaOptions.SslStoreLocation` | `CurrentUser` | расположение хранилища сертификатов: `CurrentUser` или `LocalMachine` |
| `CDCOUTBOX__PROCESSOR__QUEUECAPACITY` | `ProcessorOptions.QueueCapacity` | `2048` | bounded-очередь чтение→публикация (backpressure) |
| `CDCOUTBOX__PROCESSOR__BATCHSIZE` | `ProcessorOptions.BatchSize` | `500` | пачка публикации |
| `CDCOUTBOX__PROCESSOR__CHECKPOINTINTERVALSECONDS` | `ProcessorOptions.CheckpointIntervalSeconds` | `5` | интервал периодического сохранения checkpoint |
| `CDCOUTBOX__PROCESSOR__IDLETIMEOUTSECONDS` | `ProcessorOptions.IdleTimeoutSeconds` | `30` | порог простоя потока для idle-предупреждения (WARN) |
| `CDCOUTBOX__PROCESSOR__RETRYDELAYSECONDS` | `ProcessorOptions.RetryDelaySeconds` | `5` | база backoff повторных попыток восстановления (чтение слота, публикация, checkpoint, запуск доставки при получении лидерства) |
| `CDCOUTBOX__PROCESSOR__MAXRETRYDELAYSECONDS` | `ProcessorOptions.MaxRetryDelaySeconds` | `60` | потолок backoff: пауза удваивается от базы до этого предела |
| `CDCOUTBOX__PROCESSOR__BACKOFFRESETSESSIONSECONDS` | `ProcessorOptions.BackoffResetSessionSeconds` | `30` | сессия чтения, прожившая столько, считается стабильной и сбрасывает backoff повторных подключений |
| `CDCOUTBOX__PROCESSOR__FINALCHECKPOINTWAITSECONDS` | `ProcessorOptions.FinalCheckpointWaitSeconds` | `15` | бюджет ожидания финального checkpoint при штатной остановке (до подтверждения позиции слоту), с |
| `CDCOUTBOX__PROCESSOR__SHUTDOWNTIMEOUTSECONDS` | `ProcessorOptions.ShutdownTimeoutSeconds` | `30` | бюджет штатной остановки хоста (`HostOptions.ShutdownTimeout`): дрейн публикации, финальный checkpoint, подтверждение позиции, с |

Правила:

- **Дефолты не содержат значений контура разработки** (решение владельца): подключение PG (`ConnectionString`), `BootstrapServers` и имена слота/publication обязательны и задаются env (в стенде — compose); остальные дефолты — контракты сервиса (схема/таблица outbox, checkpoint-topic) и параметры клиента/цикла (совпадают с дефолтами librdkafka или зафиксированы архитектурно).
- Валидация — стандартный конвейер options: DataAnnotations (`[Required]`/`[Range]`, сообщения указывают путь секции и свойство) + `.ValidateDataAnnotations().ValidateOnStart()` при регистрации (`AddOptions<T>().Bind(section)` — эквивалент `Configure<T>(configuration)`, но с цепочкой валидации) — ошибка конфигурации валит старт сервиса, контейнер перезапускается рестарт-политикой. Пути секций — константы `ConfigurationSection` в самих options-классах. Строка подключения дополнительно проверяется разбором `NpgsqlConnectionStringBuilder` (неизвестные ключевые слова Npgsql отвергает сам) с контролем непустых Host/Username/Database.
- Kafka-параметры попадают в `ProducerConfig` напрямую при создании продюсера (задача 4 implementation-plan.md); с задачей 10 продюсер транзакционный: `transactional.id = "cdc-outbox-" + SlotName` (формула — стабильность линии владения, см. §2b и recommendations.md «Fencing»), `transaction.timeout.ms = DeliveryTimeoutMs + 60 000` (производная в коде), `InitTransactions` вызывается **только при получении лидерства** (`LeadershipCoordinator`) — инициализация standby отфенсила бы действующего лидера; настройка `Acks` удалена — значение `all` зашито в коде (решение владельца 2026-11-04).
- **Безопасность Kafka (mTLS, решение владельца)** задаётся в той же секции `CdcOutbox:Kafka` и применяется ко **всем** клиентам сервиса — продюсеру событий/checkpoint (один транзакционный продюсер), AdminClient стартовых проверок, консюмеру чтения checkpoint и консюмеру control-группы (одна удостоверяющая личность). `SecurityProtocol=ssl` требует ровно один источник клиентского сертификата: файловая пара `SslCertificateLocation`+`SslKeyLocation` (пароль `SslKeyPassword` — при зашифрованном ключе) **или** `SslCertificateThumbprint` — сертификат из хранилища пользователя (`SslStoreName`/`SslStoreLocation`); оба источника или ни одного, а также security-поля при `plaintext` — ошибка валидации (молчаливое игнорирование сертификатов опасно). Сертификат из хранилища передаётся librdkafka PEM-строками (экспорт сертификата и PKCS#8-ключа): приватный ключ должен быть **экспортируемым** — неэкспортируемый ключ (токен/HSM) даёт явную ошибку старта. Стенд работает по `plaintext` — mTLS покрыт конфигурацией и юнит-тестами маппинга.

### §2b. Контракт с Kafka-окружением (задача 10)

Приложение рассчитано на **произвольное Kafka-окружение**: параметры окружения — вход, параметры сервиса выводятся из них, стенд — проверяемый частный случай (не причина). Контракт провижининга (RF, `min.insync.replicas`, `cleanup.policy`, партиции, `message.max.bytes`) сервис в коде **не проверяет** — он документирован здесь и выполняется при провижининге (в стенде — kafka-init); код проверяет только существование топиков и классифицирует ошибки протокола.

**(а) Предусловия окружения** — что произвольный Kafka обязан предоставлять:

| Параметр окружения | Условие | Зачем |
|---|---|---|
| `group.min.session.timeout.ms` / `group.max.session.timeout.ms` | допускать нашу session: `min ≤ SessionTimeoutMs ≤ max` (дефолты Kafka: [6000, 1800000] мс; наш 15 с = 15000 попадает) | иначе координатор отвергнет JoinGroup (`INVALID_SESSION_TIMEOUT`) |
| `transaction.max.timeout.ms` | `≥ DeliveryTimeoutMs + 60 000` (дефолт 900000; наш 180000) | иначе `InitTransactions` упадёт (`INVALID_TRANSACTION_TIMEOUT`) |
| `message.max.bytes` (брокер) и `max.message.bytes` топиков, если задан | `≥ MessageMaxBytes` (наш 1000000 < дефолта брокера 1048588) | иначе крупные записи вечно ретраятся `MSG_SIZE_TOO_LARGE` |
| `transaction.state.log.replication.factor` / `transaction.state.log.min.isr` | RF ≤ числа брокеров, minISR ≤ RF | `__transaction_state` создаётся при первом `InitTransactions`; на 1-брочном кластере нужны RF=1/minISR=1 (так настроен IT-стенд `kafka-it`) |
| Топик `cdc-checkpoints` (создаёт эксплуатация) | **1 партиция** (до задачи 15), `cleanup.policy=compact` **только** (без `delete` — retention стёр бы ключ слота), рекомендуются RF ≥ 2 и `min.insync.replicas ≥ 2` | 1 партиция = один лидер (модель арбитража); minISR ≥ 2 + acks=all — подтверждённый checkpoint переживает отказ брокера |
| Топик `outbox.events` | существует; рекомендуются RF ≥ 2, `min.insync.replicas ≥ 2` | события доставки |
| `group.initial.rebalance.delay.ms` | без условия (влияет только на скорость первичного избрания пары) | |
| Развёртывание (compose `stop_grace_period` / k8s `terminationGracePeriodSeconds`) | `> ShutdownTimeoutSeconds` (30 с) + запас | иначе SIGKILL обрежет финальный checkpoint |

**(б) Наши параметры** — вывод из окружения, три категории:

- *Фиксированы протоколом/дизайном (не настраиваются)*: `Acks=all` (зашито в коде, настройки нет — единственное допустимое транзакциям значение; решение владельца 2026-11-04) и `EnableIdempotence=true` (требования транзакций), `PartitionAssignmentStrategy=CooperativeSticky` (вернувшийся экземпляр не крадёт партицию — приемка «restore → standby»), `EnableAutoCommit=false` без коммитов (checkpoint по ключу, офсеты группы не источник истины; следствие — lag-метрик группы нет), `AutoOffsetReset=Latest` (записи группы не используются), `MaxPollIntervalMs=300000` — дефолт, обязательство нашего кода: единственная блокировка poll-потока — хендлер отзыва партиций ≤ ~45 с ≪ 300 с; он же — бюджет группы на ре-join, `TransactionalId="cdc-outbox-"+SlotName` (формула), `ControlGroupId="cdc-outbox"`.
- *Выводимые формулами в коде (не опции)*: `HeartbeatIntervalMs = SessionTimeoutMs / 3` (правило «3 heartbeat на сессию» при любом значении session); `TransactionTimeoutMs = DeliveryTimeoutMs + 60 000` — покрывает самую долгую легитимную транзакцию (доставка ≤ `MessageTimeoutMs` + commit), ≤ `transaction.max.timeout.ms`; это же потолок зависания LSO (слепота `read_committed`-читателей, включая валидатор) при лидере, умершем посреди транзакции без преемника — «с запасом на полчаса» его делать нельзя.
- *Опции, ограниченные окружением*: `ControlSessionTimeoutSeconds` (15) ∈ [env min, env max]; `DeliveryTimeoutMs` (120000) ≤ env `transaction.max.timeout.ms − 60 000`; `MessageMaxBytes` (1000000) ≤ min(брокер, топики). Внутренние, окружением не ограниченные: `CheckpointIntervalSeconds` (5 — окно повторной доставки при failover и частота записей в compacted-топик; со всеми Kafka-таймаутами не связано), батчинг, ретраи, бюджеты остановки.

Система неравенств (env → наши):

```
env.group.min.session.timeout.ms ≤ SessionTimeoutMs ≤ env.group.max.session.timeout.ms
HeartbeatIntervalMs = SessionTimeoutMs / 3                                   (производная)
TransactionTimeoutMs = DeliveryTimeoutMs + 60 000 ≤ env.transaction.max.timeout.ms
MessageMaxBytes ≤ min(env.message.max.bytes, topic max.message.bytes…)
бюджет отзыва партиций (~45 с) < MaxPollIntervalMs (300 с)                             (обязательство кода)
ShutdownTimeoutSeconds(30) < grace-period развёртывания
цепочки остановки: финальный checkpoint ≤ 15 с ≤ ShutdownTimeout 30 с < compose stop_grace_period 45 с
```

**(в) Права принципала при включённой авторизации** (в стенде авторизации нет — PLAINTEXT без authorizer; живая проверка — задача 14):

| Ресурс | Операции | Зачем |
|---|---|---|
| `TRANSACTIONAL_ID` = `cdc-outbox-<slot>` | **WRITE** (`InitProducerId` — акт fencing) + **DESCRIBE** (`FindCoordinator`) | транзакционный продюсер — новое относительно задачи 5 |
| `GROUP` = `cdc-outbox` | **READ** (+ DESCRIBE) | контроль-консюмер арбитра: JoinGroup/SyncGroup/Heartbeat/LeaveGroup |
| `TOPIC` `cdc-checkpoints` | WRITE + DESCRIBE + **READ** | запись checkpoint; READ — арбитр (fetch для членства) и чтец `ReadLatest` |
| `TOPIC` `outbox.events` | WRITE + DESCRIBE | транзакционный продюсер событий |
| `CLUSTER` | — | **не требуется**: `IdempotentWrite` нужен только безтранзакционным идемпотентным продюсерам |
| `TOPIC` `__transaction_state` | — | **права не выдаются и не проверяются**: внутренний топик трогают только брокеры; клиент общается с координатором RPC (`InitProducerId`/`AddPartitionsToTxn`/`EndTxn`), авторизация — по `TRANSACTIONAL_ID` и топикам данных; топик создастся при первом `InitTransactions` |

**(г) Что проверяет код**: существование топиков (существующий `CheckpointTopicVerifier`, без изменений) и терминальная классификация ошибок протокола, которые всё равно случаются: JoinGroup `INVALID_SESSION_TIMEOUT` в арбитре (диагностика о границах окружения), `InitTransactions` `INVALID_TRANSACTION_TIMEOUT` при лидерстве, отказы авторизации (`GroupAuthorizationFailed` и др.). Отдельных статических валидаций транзакционных параметров нет: `Acks` зашито в коде, прочие ограничения среда сама отвергает перечисленными ошибками протокола.

**(д) Стенд — проверка условий** (compose + документированные дефолты Kafka 3.9; фактический снимок `kafka-configs --describe` — в отчёте задачи 10): session 15000 ∈ [6000, 1800000] ✓; TransactionTimeoutMs 180000 ≤ 900000 ✓; MessageMaxBytes 1000000 ≤ 1048588 ✓; transaction.state.log RF3/minISR2 при 3 брокерах ✓; `cdc-checkpoints` 1 партиция/compact/RF3/minISR2 (kafka-init) ✓; `outbox.events` 6/RF3/minISR2 ✓; `stop_grace_period` 45 с > 30 с ✓; `group.initial.rebalance.delay.ms=0` — оптимально для пары ✓. **Стенд всем условиям удовлетворяет — изменений не потребовалось.**

### Интеграционные тесты (мини-стенд pg-it/kafka-it)

Интеграционные тесты решения — `tests/CdcOutbox.IntegrationTests` — исполняются на реальных PostgreSQL и Kafka: одноразовый мини-стенд `docker-compose.tests.yml` (compose-проект `streaming-outbox-it`, отдельный от основного стенда) из двух сервисов: `pg-it` (postgres:17.2, `wal_level=logical`, БД `outbox`, литеральный пароль одноразового локального стенда — без Patroni/HAProxy/прикладных ролей: одиночный primary, предохранитель `pg_is_in_recovery` тривиально проходит) и `kafka-it` (apache/kafka:3.9.0, KRaft один узел, `auto.create.topics.enable=false` — детерминизм теста «topic отсутствует»; topic для позитивного теста создаёт сам тест через `IAdminClient.CreateTopicsAsync`, kafka-init не нужен). **Порты наружу не публикуются** — тесты исполняются в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` внутри сети стенда.

Правила:

- **Прогоны всегда требуют поднятого стенда, skip-логики нет** (решение владельца): канонический прогон — `scripts/test.sh` — поднимает мини-стенд (`up -d --wait`), передаёт подключение стандартными env `CDCOUTBOX__*` (`CDCOUTBOX__POSTGRES__CONNECTIONSTRING`, `CDCOUTBOX__KAFKA__BOOTSTRAPSERVERS` — те же имена, что у сервиса), исполняет `dotnet test StreamingOutbox.sln`, сносит стенд (`down -v`) в trap. Запуск интеграционных тестов без стенда/env — падение по Required-валидации конфигурации или подключению, не пропуск. Быстрые unit-прогоны без стенда — `dotnet test tests/<проект>` в том же sdk-контейнере.
- Тестируемые классы настраиваются **стандартным конвейером конфигурации** (констант подключения в коде тестов нет): фикстура xUnit-коллекции `IntegrationFixture` строит env-`IConfiguration` один раз на прогон и даёт `BuildOptions<TOptions>(секция, overrides)` — `AddConfiguration(env)` + in-memory переопределения per-test → `AddOptions<T>().Bind(...).ValidateDataAnnotations()`; дословно путь Program.cs, все тест-классы в одной коллекции `integration`: фикстура переиспользуется между тестами (общие `IAdminClient` и хелперы вызовов БД — `ExecuteAsync`/`QueryAsync<T>` с позиционными параметрами и доменный `GetSlotsAsync`, возвращающий список слотов для assert'ов), исполнение последовательное — мягче к одноузловому стенду. Фикстура — точка расширения интеграционных тестов задач 3+.
- Имена объектов (слоты, publication, схемы, topic'и) уникальны на тест — `it_{Guid:N}` через in-memory overrides; стенд одноразовый (`down -v` после прогона), очистки внутри тестов нет. WARN/INFO-поведение утверждается записывающим ILogger тестового проекта (инстанс per-test — записи не смешиваются).
- Покрытие задачи 2: provisioner (создание pgoutput/failover=t, идемпотентность, чужой плагин через test_decoding-слот, WARN на слот без failover), publication (нет / publish='delete' / без outbox-таблицы / лишние операции WARN / норма), checkpoint-topic (создан / отсутствует). Ошибка wal_level (SQLSTATE 55000) на живом сервере не воспроизводится — контейнер pg-it-nological исключён решением владельца, путь закрыт юнит-тестом маппера `MapSlotCreationIssue`.
- test-only пакеты проекта: `Microsoft.Extensions.Configuration` 8.0.0, `Microsoft.Extensions.Configuration.EnvironmentVariables` 8.0.0, `Microsoft.Extensions.DependencyInjection` 8.0.0, `Microsoft.Extensions.Options.ConfigurationExtensions` 8.0.0, `Microsoft.Extensions.Options.DataAnnotations` 8.0.0; прод-код `CdcOutbox.Core` не меняется. Пин `Microsoft.Extensions.DependencyInjection` 8.0.0 — вынужденный: транзитивный DiffEngine (от Shouldly) тянет пакет 3.1.9, а в 8.0 `ServiceCollection` переехал в DI.Abstractions — без пина конфликт типов CS0433.
- **Testcontainers не используется**: новые пакеты и точка входа ryuk (pull — по правилу владельца стоп при неудаче), доступ к docker.sock из контейнера тестов; compose-мини-стенд детерминирован, обозрим и в стиле репозитория. Мини-стенд и фикстура — основа интеграционных тестов задач 3+ (чтение слота, публикация, checkpoint).

---

## 3. Outbox-таблица: `outbox.messages`

Полное описание (назначение, особенности, связь с метриками, открытые вопросы) — `outbox-table.md`. Канонический DDL:

```sql
CREATE SCHEMA IF NOT EXISTS outbox;

CREATE TABLE IF NOT EXISTS outbox.messages (
  id               bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic            varchar(255) NOT NULL,
  key              bytea        NOT NULL,
  body             bytea        NOT NULL,
  headers          varchar      NOT NULL,
  partition        integer,
  producer_key     varchar(255),
  created_at       timestamptz  NOT NULL DEFAULT now(),
  outbox_partition integer      NOT NULL DEFAULT 0
);
```

Свойства схемы:

- `id` — `bigint` identity: генерируется базой автоматически (не клиентом), первичный ключ.
- `key` и `body` — `bytea`: ключ и тело передаются в брокер как есть; формат сериализации определяется приложением и таблицей не навязывается.
- `partition` — целевая партиция **брокера** (`NULL` — брокер выбирает сам); `producer_key` — выбор продюсера (кластера), через который выполняется публикация, в текущей реализации **не используется**; оба поля необязательные.
- `outbox_partition` — внутренняя партиция **Outbox** (разделение сообщений между экземплярами процессора; по умолчанию `0`); в текущей реализации **не используется**; label `partition` метрик процессора (`metrics.md`) соответствует этому полю.
- Индексы, алгоритм выборки и политика очистки структурой не фиксируются — определяются реализацией процессора (см. `outbox-table.md`).
- Схема и имя таблицы конфигурируются в сервисе через `OutboxStorageOptions` (контракт — `outbox-table.md`); каноническое имя `outbox.messages` — значение по умолчанию.

### Запись тестовой нагрузки (Writer)

Writer (генератор нагрузки) пишет INSERT+DELETE **в одной транзакции** — таблица всегда пустая, сообщение живёт в WAL (`publish='insert'` — слот несёт 1 change на сообщение). Идентичность сообщения для валидации — `id` таблицы, поэтому id резервируется у identity-последовательности **до** сборки body и вставляется явно:

```sql
-- резерв id до сборки body (id попадает в тело сообщения)
SELECT nextval(pg_get_serial_sequence('outbox.messages', 'id')) FROM generate_series(1, $1);

-- батч: 6 позиционных параметров на строку; OVERRIDING SYSTEM VALUE — id задаётся явно
-- в обход GENERATED ALWAYS; key = UTF8(id), headers = '{}'
INSERT INTO outbox.messages (id, topic, key, body, headers, created_at)
OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3::bytea, $4::bytea, $5, $6), ...;

-- удаление тех же строк в той же транзакции (таблица не растёт)
DELETE FROM outbox.messages WHERE id = ANY($1);
```

Ретрай батча с теми же id: `unique_violation` (23505) по PK = прошлый commit прошёл — разрешение неоднозначного коммита. Формат тестового body — `outbox-table.md`. Гранты `sys_writer`: `CONNECT` на БД, `USAGE` на схему `outbox`, `INSERT, DELETE` на таблицу, колоночный `SELECT (id)` (колонка читается в WHERE DELETE), `USAGE ON SEQUENCE` identity-последовательности таблицы.

---

## 4. Publication — канонический пример

pgoutput эмитит изменения **только таблиц из publication**, поэтому publication содержит outbox-таблицу. `publish = 'insert'` — для доставки значимы только INSERT (сообщение появляется в таблице одной вставкой); очистка отправленных строк (DELETE) в доставке не участвует и из эмиссии исключена — слот несёт 1 change на сообщение.

Каноническая форма:

```sql
CREATE PUBLICATION outbox_publication
  FOR TABLE outbox.messages
  WITH (publish = 'insert');
```

Идемпотентная форма (для init-скриптов; `SET (publish=...)` не трогает состав таблиц и безопасен при повторах):

```sql
SELECT 'CREATE PUBLICATION outbox_publication'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec

SELECT format('ALTER PUBLICATION outbox_publication ADD TABLE %I.%I', 'outbox', 'messages')
WHERE NOT EXISTS (
  SELECT FROM pg_publication_tables
  WHERE pubname = 'outbox_publication' AND schemaname = 'outbox' AND tablename = 'messages'
) \gexec

ALTER PUBLICATION outbox_publication SET (publish = 'insert');
```

Publication указывается в опциях **чтения** слота (параметры `START_REPLICATION` / peek), а не при создании слота. Publication не фильтрует logical messages — не считать её границей доступа.

Кто создаёт publication (решение по результатам эксперимента на стенде): инициализация стенда — `infrastructure/postgres/init/03-outbox-publication.sql` от postgres, а не outbox-сервис. Причина: `CREATE PUBLICATION` требует привилегию `CREATE` на текущую БД, а добавление таблицы — владения таблицей; у `sys_replication` нет ни того, ни другого (минимальные привилегии, SUPERUSER прикладным ролям запрещён) — попытка `CREATE PUBLICATION … FOR TABLE outbox.messages` от роли падает `permission denied for database outbox`. Гранты `sys_replication` выдаёт та же инициализация: `CONNECT` на БД — 01-appdb-objects.sql, `USAGE` на схему `outbox` — 03; `SELECT` на таблицу не требуется — стриминг pgoutput читает сообщения через слот (проверено исследовательским стендом `streaming-polling-outbox`: доставка 525k сообщений ролью без табличных грантов), каталоги `pg_publication`/`pg_replication_slots` роли видны. Сервис при старте только проверяет конфигурацию publication (имя, публикация INSERT, вхождение outbox-таблицы; иные операции — WARN) — это соответствует и части I recommendations.md §2 («миграции должны создавать … publication»). Failover-слот (§5), в отличие от publication, создаёт сам сервис: `pg_create_logical_replication_slot` требует только атрибут `REPLICATION` (есть у роли; проверено тем же экспериментом — probe-слот создавался и удалялся от неё без ошибок).

---

## 5. Failover-слот репликации — канонический пример

```sql
-- Слот — объект конкретной БД: создавать в прикладной БД (здесь — outbox).
SELECT pg_create_logical_replication_slot(
  slot_name => 'outbox_slot',
  plugin    => 'pgoutput',
  failover  => true
);
```

Идемпотентная форма:

```sql
SELECT format('SELECT pg_create_logical_replication_slot(slot_name => %L, plugin => %L, failover => true)',
              'outbox_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'outbox_slot') \gexec
```

Обязательные условия корректности:

- `failover => true` — включает нативную синхронизацию слота на standby силами PostgreSQL 17 (slotsync worker). Требует на стендбаях: `sync_replication_slots = on` и `hot_standby_feedback = on` (задаются в patroni.yml / параметрах кластера через DCS).
- **Слот в прикладной БД**: PG17 slotsync worker синхронизирует слоты только БД из `dbname` в `primary_conninfo` стендбая — прикладная БД должна быть именно там (в Patroni — официальная опция `postgresql.database`).
- `synchronized_standby_slots` на primary принимает **только физические слоты**; собирается из фактических физических слотов и обновляется после каждой смены primary (`ALTER SYSTEM SET synchronized_standby_slots = '...'` + `pg_reload_conf()`):

  ```sql
  SELECT format('ALTER SYSTEM SET synchronized_standby_slots = %L',
                (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
                 FROM pg_replication_slots WHERE slot_type = 'physical')) \gexec
  SELECT pg_reload_conf();
  ```

- Плагин — строго `pgoutput`; версия протокола при подключении — `proto_version = '1'`.
- Создание слота — только на primary: PG17 позволяет вызвать `pg_create_logical_replication_slot` и на standby, но вызов не завершается (сессия виснет; слот виден другим сессиям существующим и active — проверено на стенде, артефакт убивается `pg_terminate_backend` создателя), поэтому сервис перед созданием явно проверяет `pg_is_in_recovery()`. Ошибка wal_level < logical (SQLSTATE 55000) транслируется в понятное сообщение.
- Существующий слот без failover сервис принимает с WARN (решение владельца): failover — инфраструктурное решение о переживании promotion, чтению флаг не требуется; слот, созданный самим сервисом, всегда failover=true.
- Непотребляемый слот со временем перестаёт синхронизироваться (catalog_xmin стендбая обогнал слот) и при promotion **не сохраняется** — до появления потребителя продвигать слот вручную; реальный сервис делает это сам после Kafka delivery.
