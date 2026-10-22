# AGENTS.md

Проект `streaming-outbox`: сервис, читающий outbox-сообщения из PostgreSQL 17 через слот логической репликации (плагин pgoutput, Npgsql Logical Replication API — стриминговая репликация, без polling) и публикующий их в Kafka через Confluent.Kafka.

## Главное правило

**При реализации сервиса опираться на `docs/dependencies.md`, `docs/recommendations.md`, `docs/metrics.md` и `docs/outbox-table.md`.**

`docs/dependencies.md` — единый источник правды о:

- версиях инфраструктуры (PostgreSQL, Patroni, etcd, HAProxy, Kafka, observability-стек);
- версиях NuGet-зависимостей сервиса (Npgsql, Confluent.Kafka, OpenTelemetry);
- формате outbox-таблицы `outbox.messages` (DDL);
- канонических примерах создания publication и failover-слота репликации.

`docs/recommendations.md` — архитектурные требования к реализации CDC-сервиса:

- управление replication slot (создаётся сервисом при первом запуске, идемпотентно) и устойчивые checkpoint'ы;
- конвейер `WAL Read → Kafka Send → Kafka ACK → Durable Checkpoint → LSN Flush` — ни checkpoint, ни LSN не опережают подтверждённую Kafka-доставку;
- at-least-once доставка (дедупликация — Inbox потребителя), backpressure на ограниченных очередях, конфигурируемые пачки/in-flight;
- восстановление после сбоев, монотонность подтверждаемой позиции, явные ошибки вместо тихих пропусков;
- Checkpoint Store — отдельный Kafka topic с log compaction (ключ — имя replication slot), Recovery Mode для ручного аварийного восстановления;
- балансировка экземпляров Active-Passive через Kafka Consumer Group (служебный topic `cdc-control`): partition группы определяет активного, transactional producer (постоянный `transactional.id` от слота) фенсит устаревших лидеров, эксклюзивность слота — последняя линия защиты.

`docs/metrics.md` — обязательные метрики сервиса (prometheus-net, класс `OutboxMetrics`): активность и итерации процессора, отправленные/прочитанные/сохранённые сообщения, ошибки, возраст сообщений (`outbox_message_age_seconds`), латентность публикации, in-flight события, checkpoint-стор (age/lag/ошибки), переподключения к слоту и переключения активного экземпляра; сводная таблица и приоритетные показатели мониторинга.

`docs/outbox-table.md` — описание таблицы `outbox.messages`: назначение (Transactional Outbox), структура полей, партиционирование (`partition` брокера / `outbox_partition` процессора), контракты сервиса (`OutboxMessage`, обязательный транспорт публикации `IOutboxMessagesProcessorTransport`, настройки `OutboxStorageOptions` — схема/таблица), связь с метриками.

`docs/writer.md` — особенности реализации Writer (запись батча, ретраи и неоднозначный commit, диспетчер, подключения, формат тестового сообщения, метрики): комментарии в коде Writer сознательно минимальны, пояснения по реализации живут здесь.

Любое изменение версии, схемы или архитектурного решения **сначала** фиксируется в этих файлах, **затем** в коде и инфраструктуре.

## Текущее состояние

Из кода реализованы только сервисы тестового контура (перенесены из исследовательского стенда `streaming-polling-outbox` и адаптированы к `outbox.messages`): `src/Writer` (генератор нагрузки, HTTP `:18080`) и `src/KafkaValidator` (валидатор доставки, HTTP `:18083`). Решение — `StreamingOutbox.sln`; сборка/тесты — в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (75 тестов: 30 Writer + 39 KafkaValidator + 5 Integration-контрактов + 1 sanity CdcOutbox). Сам outbox-сервис не реализован — в решении есть его скелет (решение владельца, кода нет): `src/CdcOutbox.Host` (хост, Web SDK: `/health`, `/ready`, `/metrics`; пакеты хостинга — AspNetCore.HealthChecks.Npgsql 9.0.0, OTel 1.11.2/1.11.1, prometheus-net.AspNetCore 5.0.2) и `src/CdcOutbox.Core` (библиотека под всю логику стриминга: Npgsql 9.0.5 + Confluent.Kafka/librdkafka.redist 2.3.0 — единственный Kafka-клиент, KafkaFlow исключён) + `tests/CdcOutbox.UnitTests`. Build-контекст Dockerfile хоста — `./src` (обе csproj в restore-слое; `src/.dockerignore`). env-префикс конфигурации сервиса — `CDCOUTBOX__` (одним словом — грабля `CDC_POLLING__` старого стенда). Compose (зафиксировано, docs/stand.md): **два экземпляра `cdc-outbox-1`/`cdc-outbox-2` — Active-Passive резервирование** (арбитр — Kafka Consumer Group на служебном topic `cdc-control`: активный один — читает слот и публикует, второй в standby; переключение — rebalance → fencing transactional producer → checkpoint из `cdc-control` → ожидание освобождения слота; эксклюзивность слота — последняя линия защиты; алгоритм — docs/recommendations.md «Балансировка CDC-экземпляров через Kafka»), общий образ `streaming-outbox/cdc-outbox:net8.0`, host-порты 18084/18085, always-on без профилей; конфигурационные env (`CDCOUTBOX__*`) добавит этап реализации.

**Инфраструктура (стенд) перенесена из старого стенда и поднята**: `docker-compose.yml` + `infrastructure/` — Patroni/PostgreSQL 17.2 ×3 (etcd ×3, HAProxy :5432, синхронная репликация, БД `outbox` через bootstrap-with-appdb), Kafka KRaft 3.9.0 ×3 (topic `outbox.events`), Prometheus + postgres-exporter + kafka-exporter, init-db (роли sys_writer/sys_replication/sys_monitor, схема `outbox`, таблица `outbox.messages`). Состав минимальный по решению владельца: без PgBouncer, без polling-слоя (sys_app/cdc-функции), без cAdvisor и без харнессов старого проекта. Publication `outbox_publication` и failover-слот `outbox_slot` init НЕ создаёт — их создаёт сам outbox-сервис при первом старте (dependencies.md §4–§5). Полное описание стенда: состав, порты, команды, грабли — **`docs/stand.md`**.

Ключевые правила стенда (подробности и проверки — `docs/stand.md`):

- **Управление стендом — `scripts/stand.sh`**: `up [--build] | down | wipe | bootstrap | status | check | primary | restart | rebuild | logs`, инъекции отказов `fail pg|pgN|pg-all|kafkaN|kafka-all|haproxy|outbox-1|outbox-2|outbox-all` + `restore [target]` (target `outbox-N` стартует `cdc-outbox-N`), `switchover [candidate]`, тестовый контур `load <total> [size] [rate]` + `verdict [runId]`. `check` — полный health-check (50 проверок, «ИТОГ n/50»; контейнеры и /health обоих экземпляров cdc-outbox + 10 таргетов Prometheus); после смены топологии PG команды сами прогоняют bootstrap и settle-ожидания (первая проба Writer /ready после demote даёт разовую 503 на мёртвом коннекте пула; залипающий postgres-exporter рестартуется автоматически).
- **Никогда не запускать `docker compose up`/`run` без `--no-deps` на живом стенде** — compose пересоздаст pg-контейнеры (флап промоушенов Patroni). Один сервис: `stand.sh rebuild X` (build + `up -d --no-deps X`); полный сброс — `stand.sh wipe`.
- **После пересоздания pg-контейнеров — `docker compose restart haproxy`** (резолвит имена нод один раз при старте).
- **После каждой смены primary — `scripts/init/apply-bootstrap.sh`** (обновляет `synchronized_standby_slots` на новом лидере).
- Host-порты совпадают со старым стендом `streaming-polling-outbox` (5432, 8008-8010, 8404, 9090, 9092-9094, 18080, 18083) плюс новые 18084/18085 (cdc-outbox-1/2) — оба стенда одновременно не поднять; docker-подсеть новая (10.98.0.0/16).
- `docker compose kill` контейнера НЕ перезапускает его (рестарт-политика не применяется) — возврат явным `docker compose start`.

- **Writer и KafkaValidator — самостоятельные проекты: общего кода с outbox-сервисом и между собой НЕ имеют.** Контракт тестового body/key зафиксирован в `docs/outbox-table.md` и сознательно продублирован в коде обоих сервисов.
- Writer: INSERT+DELETE одной транзакцией (таблица всегда пустая); id — `bigint` identity, резервируется `nextval` ДО сборки body и вставляется с `OVERRIDING SYSTEM VALUE` (детали и гранты sys_writer — `docs/dependencies.md` §3, «Запись тестовой нагрузки»); повтор батча теми же id → `unique_violation` = детект AlreadyCommitted.
- KafkaValidator: идентичность сообщения — `body.id` (= id строки, `long`), группировка — `body.runId`; консюмер KafkaFlow 3.0.10 (группа `kafka-validator`, Earliest, `WithoutStoringOffsets` — рестарт всегда перематывает topic с начала); обогащение снапшотом CDC удалено до появления outbox-сервиса (`verdict.Cdc` всегда null).
- Метрики обоих сервисов — prometheus-net 5.0 (`Metrics.Create*`, `MapMetrics`), без OTel; OTel-пины 1.11 — для outbox-сервиса. Грабли: внутри namespace `*.Metrics` имя `Metrics` резолвится в namespace — квалифицировать `Prometheus.Metrics`; `UseMetrics` в 5.0 нет — достаточно `MapMetrics`; labels — `WithLabels(...).Inc(...)`.

## Линт (обязательно)

- Код и тесты проверяются на соответствие `.editorconfig` (корень репозитория): **после любого изменения `.cs`/`.csproj` запускать `scripts/lint.sh`** (обёртка над `dotnet format StreamingOutbox.sln --verify-no-changes` в контейнере `mcr.microsoft.com/dotnet/sdk:8.0`). Линт должен быть зелёным так же, как сборка и тесты, — до завершения работы. Авто-фикс нарушений: `dotnet format StreamingOutbox.sln` без `--verify-no-changes` (правки пересматривать по диффу).
- **Строгая сборка — `Directory.Build.props`** (применяется ко всем проектам): `TreatWarningsAsErrors`, `EnableNETAnalyzers` + `AnalysisLevel=preview`, `EnforceCodeStyleInBuild` — правила .editorconfig уровня warning/error блокируют и сборку (проверено на SDK 8.0: CS0219-warning валит сборку; IDE0161 из .editorconfig валит сборку). Известное исключение: **IDE0005 (неиспользуемые using'и) в SDK 8 не эскалируется ни в сборку, ни в dotnet format** (несмотря на severity и GenerateDocumentationFile) — это правило сейчас ни один гейт не ловит.
- Блокирующие правила (severity warning/error в .editorconfig): форматирование (4 пробела, LF, UTF-8, финальный перевод строки, без хвостовых пробелов), file-scoped namespaces, using вне namespace + System-группой первой, без `this.`, явные модификаторы доступа, скобки у многострочных `if` (`when_multiline`), нейминг (типы/методы/свойства PascalCase, интерфейсы `IPascalCase`, private instance-поля `_camelCase`, `const`/`static` — PascalCase, параметры/локальные camelCase), readonly-поля, где возможно, авто-свойства, target-typed `new()`, неиспользуемые using'и (IDE0005 — для его корректной работы в csproj включён `GenerateDocumentationFile` + `NoWarn CS1591`).
- Спорные предпочтения (var, expression-bodied и пр.) зафиксированы в .editorconfig на уровне suggestion — линт их не блокирует.
- XML-doc комментарии — только в многострочном виде: `/// <summary>`, текст и `/// </summary>` на отдельных строках.

## Ключевые ограничения

- Чтение outbox — только через logical replication slot; SELECT-polling таблицы запрещён.
- Механика CDC — outbox-таблица + pgoutput (pgoutput не передаёт logical messages ни в одной версии PostgreSQL — доставка строится на INSERT).
- Подтверждение LSN слота — только после Kafka delivery (`acks=all`): семантика at-least-once, дубликаты допустимы, потери нет.
- Kafka-клиент outbox-сервиса — Confluent.Kafka напрямую: идемпотентный продюсер событий (`acks=all`), транзакционный продюсер checkpoint'ов `cdc-control` (фенсинг лидера), консюмер control-группы. KafkaFlow в outbox-сервисе исключён (решение владельца 2026-10-22): нет доступа к Kafka-транзакциям ни в одной линии — [Farfetch/kafkaflow#566](https://github.com/Farfetch/kafkaflow/issues/566) открыт с 2024-04 без реакции мейнтейнеров, проверено рефлексией 3.0.10 (публичный API без транзакционных членов, обёрнутый IProducer наружу не торчит); полная причина — `docs/dependencies.md` §2. KafkaFlow 3.0.10 остаётся только в KafkaValidator (тестовый контур).
- .NET 8 (`net8.0`) и PostgreSQL 17 — строго; всё в Docker Compose.
- Комментарии в коде (включая XML-doc) — только с разрешения владельца; пояснения по реализации живут в `docs/*.md` (стиль Writer — `docs/writer.md`).
