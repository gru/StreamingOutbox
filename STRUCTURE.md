# Структура репозитория

Решение — `StreamingOutbox.sln`: продукт (`src/` + `tests/`), сервисы тестового контура (`testloop/`), стенд и инфраструктура (`docker-compose.yml` + `infrastructure/`), документация (корневые `*.md`; справочник версий и контрактов — раздел в `AGENTS.md`), сценарии управления (`scripts/`). Сборка и тесты — в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (`.NET 8` строго), контейнерные прогоны в папку репозитория не пишут.

## Проекты решения

| Проект | Назначение |
|---|---|
| `src/CdcOutbox.Core` | вся логика стриминга: чтение слота, парсинг, публикация в Kafka, контрольные точки, лидерство Active-Passive, метрики, конфигурация |
| `src/CdcOutbox.Host` | хост сервиса (ASP.NET Core): `Program.cs`, health-эндпоинты, `/metrics`, бюджет остановки |
| `tests/CdcOutbox.UnitTests` | unit-тесты продукта — чистая логика без живых PostgreSQL/Kafka |
| `tests/CdcOutbox.IntegrationTests` | интеграционные тесты на реальных PostgreSQL/Kafka (одноразовый мини-стенд `docker-compose.tests.yml`) |
| `testloop/Writer` | генератор нагрузки: пишет батчи в outbox-таблицу (INSERT+DELETE одной транзакцией), HTTP `:18080` |
| `testloop/Writer.UnitTests` | unit-тесты Writer |
| `testloop/KafkaValidator` | валидатор доставки: независимый консюмер Kafka, сверяет прогоны Writer (missing/extra/dup), HTTP `:18083` |
| `testloop/KafkaValidator.UnitTests` | unit-тесты валидатора |
| `testloop/IntegrationTests` | контрактные тесты тестового контура (таблица outbox, publication — на живом стенде) |

Writer и KafkaValidator — самостоятельные сервисы тестового контура: общего кода с outbox-сервисом и между собой не имеют.

## Каталоги

| Каталог | Содержимое |
|---|---|
| `infrastructure/` | определение стенда: `patroni/` (образ Patroni поверх postgres:17.2, bootstrap кластера), `postgres/init/` (роли, БД, схема outbox, publication — SQL инициализации), `haproxy/` (маршрутизация на текущий primary), `prometheus/` (конфиг Prometheus + кастомные запросы postgres-exporter) |
| `src/`, `tests/`, `testloop/` | код решения — см. таблицу проектов выше |
| `testruns/` | результаты прогонов: нагрузочные тесты (`loadtest-*.md` — text/binary/batchsize, батч 1) и отказоустойчивость (`faulttest-*.md` + логи фаз) — имена файлов отражают условия и тип тестирования |

В корне: `docker-compose.yml` — полный стенд (Patroni/PostgreSQL ×3, etcd ×3, HAProxy, Kafka ×3, Prometheus с экспортёрами, пара `cdc-outbox-1`/`cdc-outbox-2`, Writer, валидатор); `docker-compose.tests.yml` — одноразовый мини-стенд интеграционных тестов (PostgreSQL + Kafka, без публикации портов); `Directory.Build.props` — строгая сборка (`TreatWarningsAsErrors`, анализаторы, StyleCop-правила порядка членов); `.env` / `.env.example` — переменные стенда.

## Скрипты и харнессы

Все — в `scripts/`, запуск от корня репозитория.

| Скрипт | Назначение |
|---|---|
| `scripts/stand.sh` | управление стендом: `up`/`down`/`wipe`/`bootstrap`/`status`/`check`/`restart`/`rebuild`/`logs`, отказы `fail pg\|kafkaN\|outbox-N\|...` + `restore`, `switchover`, тестовый контур `load <total> [size] [rate]` + `verdict [runId]` |
| `scripts/test.sh` | полный прогон тестов решения: поднимает одноразовый мини-стенд и исполняет `dotnet test` в sdk-контейнере в его сети |
| `scripts/lint.sh` | проверка стиля: `dotnet format --verify-no-changes` в sdk-контейнере |
| `scripts/init/apply-bootstrap.sh` | пересборка `synchronized_standby_slots` на текущем primary (после каждой смены лидера) |
| `scripts/test/integration-delivery.sh` | приёмочный харнесс доставки end-to-end: запись → вердикт → рестарт в середине прогона → отказы PG/Kafka → фаза Active-Passive при поднятой паре |
| `scripts/test/upgrade-rehearsal.sh` | приёмочный харнесс репетиции обновления PostgreSQL по пути dump/restore (разрушительный для БД outbox) |
| `scripts/reset/wipe.sh` | полный сброс стенда: контейнеры, сети, volumes только этого compose-проекта |

## Стенд

Полный состав — `docker-compose.yml`: Patroni/PostgreSQL 17 ×3 (etcd ×3, HAProxy), Kafka KRaft ×3, Prometheus с экспортёрами, init-db/kafka-init (идемпотентная инициализация ролей/БД/таблицы/publication/топиков), Writer, валидатор, пара `cdc-outbox-1`/`cdc-outbox-2` (версии образов — `AGENTS.md` «Версии и контракты»). Все подключения к PostgreSQL — через HAProxy; docker-сеть `outbox-net` 10.98.0.0/16, host-порты совпадают со старым стендом `streaming-polling-outbox` — оба одновременно не поднять.

Порты: HAProxy **5432** (stats 8404); Patroni REST 8008–8010; Kafka 9092–9094; Prometheus 9090; Writer 18080; валидатор 18083; cdc-outbox-1 18084; cdc-outbox-2 18085.

Быстрая проверка и нагрузка:

```bash
cp .env.example .env && docker compose up -d --build   # первый подъём ~2–3 мин
curl -s localhost:18084/health                         # cdc-outbox-1; /ready — postgres-чек
./scripts/stand.sh load 500 && ./scripts/stand.sh verdict   # pass ⇔ missing=0 && unexpected=0 (дубли допустимы)
```

Пара Active-Passive (cdc-outbox-2 закрыт compose-профилем `redundancy`; алгоритм арбитража и fencing — README «Резервирование и Active-Passive»):

```bash
docker compose --profile redundancy up -d --no-deps cdc-outbox-2
# + вернуть таргет cdc-outbox-2:8080 в infrastructure/prometheus/prometheus.yml
# + docker compose restart prometheus (bind-mount одиночного файла — рестарт обязателен)
```

Recovery Mode на стенде (сброс позиции для чистых замеров / восстановление после утраты слота; семантика — README «Recovery Mode»):

```bash
docker compose stop cdc-outbox-1
docker compose exec -T pg1 psql -h haproxy -U postgres -d outbox -tAc "SELECT pg_drop_replication_slot('outbox_slot')"
CDCOUTBOX__RECOVERYMODE=true docker compose up -d --no-deps cdc-outbox-1   # WARN «Recovery Mode: checkpoint X → Y»
docker compose up -d --no-deps cdc-outbox-1                              # рестарт без флага
```

Объекты БД (создаёт init-db идемпотентно): БД `outbox` — bootstrap-методом Patroni `bootstrap-with-appdb` (PG17 slotsync синхронизирует failover-слоты только БД из `dbname` primary_conninfo); схема/таблица/publication — README «PostgreSQL и Patroni»; failover-слот `outbox_slot` создаёт сам сервис при первом старте. Роли: `sys_writer` (CONNECT, USAGE схемы, INSERT/DELETE, колоночный SELECT(id), USAGE на identity-последовательность), `sys_replication` (README), `sys_monitor` — `pg_monitor` **WITH INHERIT TRUE** (грабля PG16+: опция inherit членства фиксируется в момент GRANT, позже `ALTER ROLE … INHERIT` её не обновляет — permission denied на каждом scrape).

Кастомные запросы postgres-exporter (`infrastructure/prometheus/postgres-exporter-queries.yaml`): bool → `CASE 1/0`; байты → `double precision` (LSN до 2^48 точно в float64); primary-only запросы — `WHERE NOT pg_is_in_recovery()`; колонки с возможными NULL — отдельными запросами с `IS NOT NULL` (иначе NaN-сэмплы); префиксы `pg_slot_*` не совпадают со встроенным коллектором.

Правила (проверенные грабли):

- **Никогда `docker compose up`/`run` без `--no-deps` на живом стенде** — compose реконсилят граф зависимостей и может пересоздать pg-контейнеры: флап промоушенов и рассинхрон failover-слотов. Один сервис: `up -d --no-deps <name>` (= `stand.sh rebuild`).
- После пересоздания pg-контейнеров — `docker compose restart haproxy` (резолвит имена pg-нод один раз при старте).
- После каждой смены primary — `scripts/init/apply-bootstrap.sh`: пересборка `synchronized_standby_slots` из активных физических слотов на новом лидере (харнессы делают это сами).
- `docker compose kill` НЕ перезапускает контейнер (Docker трактует kill как ручную остановку; у cdc-outbox рестарт-политик нет вовсе) — возврат явным `docker compose start` / `stand.sh restore`.
- `prometheus.yml`/`queries.yaml` — bind-mount одиночных файлов: правки на хосте видны контейнеру только после рестарта сервиса.
- Graceful `switchover` при живом консюмере слота может виснуть — лечится рестартом активного экземпляра (грабля старого стенда).
- Редактировать работающий bash-скрипт нельзя (bash дочитывает файл по offset) — фиксить после завершения прогона.
- `stand.sh check` — 59 проверок (62 с профилем `redundancy`, код ≠ 0 при провале); postgres-exporter может залипнуть на мёртвых соединениях после серии смен лидера — харнесс рестартует его сам.

Из старого стенда не перенесены: PgBouncer (pooling — пул Npgsql), polling-слой (`sys_app`, схема `cdc`, SECURITY DEFINER-функции), cAdvisor (Docker Desktop с containerd видит только VM-root), харнессы ha-suite/benchmark/collect-results (базовые инъекции закрыты `stand.sh fail/restore` и приёмочными харнессами).
