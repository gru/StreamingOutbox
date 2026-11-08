# Структура репозитория

Решение — `StreamingOutbox.sln`: продукт (`src/` + `tests/`), сервисы тестового контура (`testloop/`), стенд и инфраструктура (`docker-compose.yml` + `infrastructure/`), документация (`docs/`), сценарии управления (`scripts/`). Сборка и тесты — в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (`.NET 8` строго), контейнерные прогоны в папку репозитория не пишут.

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
| `docs/` | архитектурная документация: `dependencies.md` (стек, схема, SQL-канон, конфигурация), `recommendations.md` (требования к CDC-конвейеру и резервированию), `outbox-table.md` (таблица и контракты доставки), `metrics.md` (метрики), `implementation-plan.md` (план задач с чек-листом), `stand.md` (стенд: состав, команды, грабли), `writer.md` (особенности Writer) |
| `infrastructure/` | определение стенда: `patroni/` (образ Patroni поверх postgres:17.2, bootstrap кластера), `postgres/init/` (роли, БД, схема outbox, publication — SQL инициализации), `haproxy/` (маршрутизация на текущий primary), `prometheus/` (конфиг Prometheus + кастомные запросы postgres-exporter) |
| `src/`, `tests/`, `testloop/` | код решения — см. таблицу проектов выше |

В корне: `docker-compose.yml` — полный стенд (Patroni/PostgreSQL ×3, etcd ×3, HAProxy, Kafka ×3, Prometheus с экспортёрами, пара `cdc-outbox-1`/`cdc-outbox-2`, Writer, валидатор); `docker-compose.tests.yml` — одноразовый мини-стенд интеграционных тестов (PostgreSQL + Kafka, без публикации портов); `Directory.Build.props` — строгая сборка (`TreatWarningsAsErrors`, анализаторы, StyleCop-правила порядка членов); `.env` / `.env.example` — переменные стенда; отчёты `LOADTEST-*.md` / `FAULTTEST-*.md` — результаты нагрузочных тестов и отказоустойчивости.

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

Правила работы со стендом (никогда `up` без `--no-deps` на живом стенде, рестарт HAProxy после пересоздания pg-контейнеров, восстановление слота через Recovery Mode) — `docs/stand.md`.
