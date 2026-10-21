# stand.md — тестовый стенд (Docker Compose)

Минимальная инфраструктура для нагрузочного тестирования и тестов восстановления после отказов: Patroni/PostgreSQL 17 ×3 (etcd ×3, HAProxy) + Kafka KRaft ×3 + Prometheus + сервисы тестового контура (Writer, KafkaValidator). Перенесена из исследовательского стенда `streaming-polling-outbox`; пины версий — `docs/dependencies.md` §1.

## Состав и порты

| Сервис | Образ | Host-порт | Назначение |
|---|---|---|---|
| etcd1-3 | `gcr.io/etcd-development/etcd:v3.5.17` | — | DCS для Patroni |
| pg1-3 | своя сборка `streaming-outbox/patroni-pg17:4.1.5` (`postgres:17.2` + patroni) | 8008/8009/8010 (REST) | Patroni-кластер, синхронная репликация, БД `outbox` |
| haproxy | `haproxy:3.0.30-alpine` | **5432**, 8404 (stats) | Маршрут на текущий primary |
| init-db | `postgres:17.2` (one-shot) | — | Идемпотентная инициализация ролей/БД/таблицы |
| kafka1-3 | `apache/kafka:3.9.0` (KRaft) | 9092/9093/9094 | Брокеры; topic `outbox.events` (6 партций, RF 3, min.insync.replicas=2, auto-create off) |
| kafka-init | `apache/kafka:3.9.0` (one-shot) | — | Явное создание topic |
| writer | `streaming-outbox/writer:net8.0` | 18080 | Генератор нагрузки (`/api/generation/*`, `/metrics`) |
| kafka-validator | `streaming-outbox/kafka-validator:net8.0` | 18083 | Валидатор доставки (`/api/validation/*`, `/metrics`) |
| prometheus | `prom/prometheus:v2.54.1` | 9090 | Сбор метрик (interval 5s) |
| postgres-exporter | `prometheuscommunity/postgres-exporter:v0.15.0` | — | PG-метрики от `sys_monitor` (порт не публикуется) |
| kafka-exporter | `danielqsj/kafka-exporter:v1.8.0` | — | Kafka-метрики (порт не публикуется) |

Все подключения к PostgreSQL — напрямую через HAProxy (5432). Docker-сеть `outbox-net`, подсеть `10.98.0.0/16` (отличается от `10.99.0.0/16` старого стенда — сети не конфликтуют, но **host-порты совпадают**: оба стенда одновременно не поднять; старый остановить `docker compose stop` в его репозитории).

## Quickstart

```bash
cp .env.example .env          # при необходимости поправить пароли
docker compose up -d --build  # первый подъём: ~2-3 мин (bootstrap кластера)
docker compose ps             # все Up/healthy; init-db и kafka-init — exited 0
```

Быстрая проверка:

```bash
curl -s localhost:18080/health          # {"status":"Healthy"}
curl -s localhost:18080/ready           # {"status":"Healthy"} (postgres-чек)
curl -s localhost:18083/api/validation/state   # "state":"running"
curl -s localhost:9090/-/healthy        # OK
# Patroni-кластер: curl -s localhost:8008/cluster | python3 -m json.tool
```

Нагрузочный прогон (доставка в Kafka появится только с outbox-сервисом — см. ниже):

```bash
curl -s -X POST localhost:18080/api/generation/start \
  -H 'Content-Type: application/json' \
  -d '{"totalMessages": 500, "messageSizeBytes": 256}'
curl -s localhost:18080/api/generation/state
curl -s localhost:18080/api/generation/results
```

## Управление стендом

| Команда | Когда |
|---|---|
| `scripts/init/apply-bootstrap.sh` | Идемпотентный повтор init-db. **Обязателен после каждой смены primary** (failover/switchover): `synchronized_standby_slots` пишется ALTER SYSTEM'ом локально в `postgresql.auto.conf` лидера (`docs/dependencies.md` §5) |
| `scripts/reset/wipe.sh` | Полный сброс: контейнеры, сети, volumes (`down -v --remove-orphans`) |
| `docker compose build writer && docker compose up -d --no-deps writer` | Пересборка одного сервиса на живом стенде |

## Объекты БД и роли

Создаются init-db (`infrastructure/postgres/init/`), идемпотентно:

- БД **`outbox`** (не `postgres`) — создаётся bootstrap-методом Patroni `bootstrap-with-appdb` ещё до старта кластера: PG17 slotsync worker синхронизирует failover-слоты только БД из `dbname` primary_conninfo (`docs/dependencies.md` §5).
- Схема `outbox` + таблица **`outbox.messages`** — канонический DDL `docs/dependencies.md` §3.
- **`sys_writer`** — Writer: `CONNECT` на БД, `USAGE` на схему, `INSERT, DELETE` на таблицу, колоночный `SELECT (id)`, `USAGE` на identity-последовательность.
- **`sys_replication`** (`REPLICATION`) — будущий outbox-сервис; прикладные гранты добавит его этап.
- **`sys_monitor`** (`pg_monitor` с `INHERIT TRUE`) — postgres-exporter.
- `synchronized_standby_slots` = физические слоты кластера (`02-sync-slots.sql`).

**Publication `outbox_publication` и failover-слот `outbox_slot` init НЕ создаёт** — по контракту проекта их создаёт сам outbox-сервис при первом старте (`docs/dependencies.md` §4–§5). До его появления слота нет — это норма: непотребляемый слот со временем перестаёт синхронизироваться и при promotion теряется (поведение PG17). Метрики `pg_slot_*` в Prometheus показывают только физические слоты Patroni до появления сервиса.

## Что НЕ перенесено из streaming-polling-outbox

- **PgBouncer** — Writer ходит напрямую через HAProxy; pooling — встроенный пул Npgsql (MaxPoolSize 72).
- **Polling-слой**: роль `sys_app`, схема `cdc`, SECURITY DEFINER-функции peek/advance/outbox_slot_status — в этом проекте polling-механики нет.
- **CDC-сервисы** (cdc-polling/cdc-streaming) и их compose-профили — заменит единый outbox-сервис.
- **cAdvisor** — на Docker Desktop (containerd image store) видит только VM-root, per-container серий нет; если понадобятся per-container CPU/RAM — `docker stats`.
- **Харнессы** (check-infra, smoke, integration-delivery, ha-suite, benchmark, collect-results) — будут написаны заново под outbox-сервис на его этапе.
- reconcile-скрипт остатков слота (03-slot-reconcile) — без слота не нужен; вернётся вместе со слотом, если грабли PG17 slotsync повторятся.

## Что появится с outbox-сервисом

- Слот `outbox_slot` + publication `outbox_publication` (создаёт сервис) → в Prometheus оживут `pg_slot_*`-метрики, вердикты валидатора станут содержательными (сейчас вердикт по любому прогону — `missing` всех записанных id: сообщения лежат в WAL, но доставлять их некому).
- Сервис в compose (на его этапе), харнессы нагрузочных и HA-тестов.
- После каждой смены primary — `scripts/init/apply-bootstrap.sh` + при зависшем graceful switchover с живым консюмером слота рестарт сервиса (грабли старого стенда).

## Грабли (унаследовано из старого стенда, проверено там)

- **Никогда не запускать `docker compose up`/`run` без `--no-deps` на живом стенде** — compose реконсилят весь граф зависимостей и могут пересоздать pg-контейнеры: одновременный рестарт всех нод Patroni = флап промоушенов и рассинхрон failover-слотов. Один сервис: `up -d --no-deps <name>`.
- **После пересоздания pg-контейнеров — `docker compose restart haproxy`**: HAProxy резолвит имена pg-нод один раз при старте, новые IP не подхватываются сам собой (в cfg есть `resolvers docker`, но рестарт надёжен).
- `docker compose kill` pg-ноды/kafka-брокера НЕ перезапускает контейнер (Docker трактует API-kill как ручную остановку) — возврат явным `docker compose start` после выборов лидера. Актуально для будущих HA-тестов.
- **prometheus.yml примонтирован одиночным файлом**: после правки на хосте живой контейнер видит СТАРУЮ копию (bind-mount держит исходный inode, `kill -HUP` не помогает) — только `docker compose restart prometheus`. То же для queries.yaml + `restart postgres-exporter`.
- Свежий стенд: первые минуты возможны кэшированные ошибки подключения — самоизлечивается (ретраи Writer).
- Редактировать работающий bash-скрипт НЕЛЬЗЯ (bash дочитывает файл по offset) — фиксить после завершения прогона.
