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
| kafka-init | `apache/kafka:3.9.0` (one-shot) | — | Явное создание topic'ов `outbox.events` + `cdc-checkpoints` (checkpoint-стор outbox-сервиса) |
| writer | `streaming-outbox/writer:net8.0` | 18080 | Генератор нагрузки (`/api/generation/*`, `/metrics`) |
| kafka-validator | `streaming-outbox/kafka-validator:net8.0` | 18083 | Валидатор доставки (`/api/validation/*`, `/metrics`) |
| cdc-outbox-1 | `streaming-outbox/cdc-outbox:net8.0` | 18084 | Outbox-сервис (CDC outbox→Kafka), активный экземпляр; реализован по задачам 1–10 (конвейер: чтение слота → публикация → checkpoint → подтверждение LSN; устойчивость, Recovery Mode, idle-предупреждение, метрики, Active-Passive) |
| cdc-outbox-2 | `streaming-outbox/cdc-outbox:net8.0` | 18085 | Второй экземпляр пары Active-Passive (задача 10) — **закрыт compose-профилем `redundancy`** по умолчанию (раздел «Резервирование cdc-outbox») |
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
curl -s localhost:18084/health          # Healthy — cdc-outbox-1 (дефолтный форматтер; /ready: postgres-чек)
# localhost:18085 (cdc-outbox-2) отвечает только при включённом профиле redundancy
curl -s localhost:9090/-/healthy        # OK
# Patroni-кластер: curl -s localhost:8008/cluster | python3 -m json.tool
```

Нагрузочный прогон и вердикт доставки:

```bash
curl -s -X POST localhost:18080/api/generation/start \
  -H 'Content-Type: application/json' \
  -d '{"totalMessages": 500, "messageSizeBytes": 256}'
curl -s localhost:18080/api/generation/state
curl -s localhost:18080/api/generation/results
./scripts/stand.sh verdict    # pass ⇔ missing=0 && unexpected=0 (дубликаты допустимы)
```

## Управление стендом

Основной вход — **`scripts/stand.sh`** (подкоманды: `up [--build]`, `down`, `wipe`, `bootstrap`, `status`, `check`, `primary`, `restart <svc>`, `rebuild <svc>`, `logs <svc> [N]`, `load`, `verdict`, `fail`, `restore`, `switchover`):

| Команда | Что делает |
|---|---|
| `stand.sh check` | Полный health-check стенда: контейнеры, Patroni, роли/гранты БД + publication + failover-слот, topic'и Kafka, сервисы, Prometheus — «ИТОГ n/59» (с включённым профилем redundancy — n/62; +проверки арбитража Active-Passive: is_active=1 у cdc-outbox-1, в паре активен ровно один; код ≠ 0 при провале) |
| `stand.sh status` | Быстрая сводка: compose ps + Patroni + сервисы + Prometheus + список остановленных контейнеров |
| `stand.sh up [--build]` | Поднять стенд и дождаться init-db/kafka-init (exit 0) |
| `stand.sh load <total> [size] [rate]` | Нагрузочный прогон Writer с ожиданием завершения + итоговый `/results` |
| `stand.sh verdict [runId]` | Вердикт валидатора (по умолчанию последний прогон: реестр валидатора, при пустом topic — последний прогон Writer) |
| `stand.sh fail …` / `restore [target]` / `switchover [candidate]` | Инъекции отказов и восстановление — см. следующий раздел |

Idle-предупреждение в логах `cdc-outbox-1`: WARN `Простой потока N с (порог M с): …` выдаётся не чаще одного раза за `IdleTimeoutSeconds` (дефолт 30 с; env `CDCOUTBOX__PROCESSOR__IDLETIMEOUTSECONDS`); N — фактическое время простоя с последнего события и растёт с каждым повтором, любая транзакция с outbox-сообщением сбрасывает таймер (под нагрузкой предупреждений нет). Это не «норма», а предупреждение о риске: без новых событий слот не подтверждается и удерживаемый WAL может накапливаться — heartbeat-механизм отсутствует (сознательное решение: сервис рассчитан на постоянно пишущиеся outbox, `docs/recommendations.md` «Контроль простоя потока»); на стенде при остановленной записи ожидаем, для «горячего» outbox в проде — повод проверить запись. Прекращение WARN означает поступление новых событий; устойчивые предупреждения при остановленной записи — накопленные события доставлены (используется при подготовке обновления PG).

Нижележащие скрипты (вызываются командами `wipe`/`bootstrap`):

| Скрипт | Когда |
|---|---|
| `scripts/init/apply-bootstrap.sh` | Идемпотентный повтор init-db. **Обязателен после каждой смены primary** (failover/switchover): `synchronized_standby_slots` пишется ALTER SYSTEM'ом локально в `postgresql.auto.conf` лидера (`docs/dependencies.md` §5) |
| `scripts/reset/wipe.sh` | Полный сброс: контейнеры, сети, volumes (`down -v --remove-orphans`) |
| `docker compose build X && docker compose up -d --no-deps X` (= `stand.sh rebuild X`) | Пересборка одного сервиса на живом стенде |

## Отказы и восстановление (stand.sh fail / restore / switchover)

Семантика: `docker compose kill` = «ручная остановка» — рестарт-политика Docker НЕ применяется, возврат только явным `docker compose start` (`stand.sh restore`). После любой смены лидера harness сам прогоняет `apply-bootstrap` (refresh `synchronized_standby_slots` на новом лидере — обязательное правило, `docs/dependencies.md` §5) и делает settle-ожидания (см. ниже).

| Команда | Сценарий |
|---|---|
| `fail pg` | kill текущего leader → выборы нового (замер времени) → bootstrap. Убитая нода лежит до `restore` |
| `fail pg1\|pg2\|pg3` | kill конкретной ноды (реплика: кворум жив, выборов нет) |
| `fail pg-all` | полный отказ PG (Writer /ready → 503, лидера нет) |
| `fail kafka1\|kafka2\|kafka3` | kill брокера (кластер жив: RF3/min.insync.replicas=2) |
| `fail kafka-all` | полный outage Kafka |
| `fail haproxy` | kill маршрутизатора (кластер жив, путь записи рвётся) |
| `fail outbox-1` | kill cdc-outbox-1 — при поднятом резерве доставку принимает cdc-outbox-2 (rebalance ~15–20 с), без резерва доставка останавливается, WAL копится |
| `fail outbox-2` | kill cdc-outbox-2 — только если поднят (профиль `redundancy`); иначе отказ с подсказкой включения |
| `fail outbox-all` | kill cdc-outbox-1 (+ cdc-outbox-2, если поднят); с реализацией сервиса это останавливает доставку: слот не потребляется, WAL копится |
| `restore [target]` | `docker compose start`: без аргумента — все остановленные (pg-ноды пошагово, пауза 3с — анти-флап промоушенов); target: `pg1..3\|kafka1..3\|haproxy\|outbox-1\|outbox-2` (outbox-2 — с флагом профиля `redundancy`); после восстановления pg-нод — ожидание лидера + bootstrap + settle |
| `switchover [candidate]` | graceful-переключение (по умолчанию кандидат = текущий sync_standby): REST `POST /switchover` на порт КАНИДАТА (basic-auth), ожидание лидера, bootstrap. При живом консюмере логического слота (cdc-outbox) может виснуть — лечится рестартом активного экземпляра (грабля старого стенда) |

Проверенное поведение (живая верификация 2026-10-21, все ветки):

- Выборы при `fail pg` — ~30с (TTL 30 Patroni); `fail pg-all` + пошаговый `restore` — без флапа промоушенов.
- **Settle после смены топологии — ожидаемые окна неготовности** (harness ждёт сам, сразу после переключений `check` зелёный):
  - первый проба Writer `/ready` после demote отдаёт 503 — Patroni рвёт сессии (`57P01`), проба попадает на мёртвый коннект пула Npgsql, который вычищается; следующая проба — 200;
  - **postgres-exporter может залипнуть** на мёртвых соединениях после серии переключений лидера (EOF на каждом скрейпе, `pg_up` пропадает, сам не восстанавливается — проверено) — harness автоматически рестартует его при `pg_up ≠ 1` дольше 30с.
- **Полный outage Kafka: валидатор НЕ падает** (Up, 0 рестартов, возвращается в потребление после возврата брокеров; наблюдалось на KafkaFlow 3.0.10, консюмер с 2026-10-27 — сырой Confluent.Kafka, см. dependencies.md §2 «Отказ от KafkaFlow») — в отличие от SIGSEGV-дефекта KafkaFlow 4.2.0 старого стенда. Writer при этом продолжает писать (от Kafka не зависит — проектная точка).
- `check` — 59 проверок при выключенном профиле redundancy (эталон после `wipe`+`up`: 59/59, включая is_active=1 у cdc-outbox-1 — одиночный экземпляр всегда лидер контрольной группы; проверка failover-слота требует, чтобы cdc-outbox-1 хотя бы раз стартовал — слот создаёт он); с включённым профилем — 62 (+контейнер, +`/health` cdc-outbox-2, таргет Prometheus, +«в паре активен ровно один»).

### Резервирование cdc-outbox (два экземпляра)

`cdc-outbox-1` и `cdc-outbox-2` — два одинаковых экземпляра outbox-сервиса. **Резервирование Active-Passive реализовано (задача 10, 2026-11-04)**; по умолчанию второй экземпляр всё так же закрыт compose-профилем `redundancy` (решение владельца): `docker compose up` его не поднимает, таргета в Prometheus нет. Включение пары:

```bash
docker compose --profile redundancy up -d --no-deps cdc-outbox-2
# + вернуть таргет cdc-outbox-2:8080 в infrastructure/prometheus/prometheus.yml
# + docker compose restart prometheus (bind-mount одиночного файла — рестарт обязателен)
```

Целевая модель — **Active-Passive** (полный алгоритм — `docs/recommendations.md` «Балансировка CDC-экземпляров через Kafka»):

- Арбитр — Kafka Consumer Group на служебном topic `cdc-checkpoints` (1 партиция, `cleanup.policy=compact`): partition назначается одному экземпляру — он активен, и только он имеет право подключаться к слоту `outbox_slot`, читать WAL и публиковать события. Второй в standby поддерживает членство в группе (регулярный `poll()`).
- Активный убит (`fail outbox-N`, kill, потеря сети) → координатор группы замечает пропажу heartbeat'ов за session timeout (15 с) → rebalance → новый лидер: `InitTransactions` (fencing предыдущей эпохи, аборт её висящей транзакции), загрузка checkpoint из `cdc-checkpoints` (`read_committed`, ключ — имя слота), стартовые проверки, ожидание освобождения слота при живом старом walsender (55006 — транзиент) и продолжение от `confirmed_flush` (at-least-once, дубли возможны; окно повторной доставки ≤ каденс checkpoint 5 с + неподтверждённый хвост). Итого переключение ≈ 15–20 с. Graceful stop лидера (`docker compose stop`) быстрее: Close консюмера = LeaveGroup → мгновенный rebalance без ожидания session timeout.
- Потерявший лидерство (отзыв партиций) или получивший fencing экземпляр останавливает чтение, публикацию и обновление checkpoint (graceful — с финальной checkpoint-транзакцией при отзыве партиций, без — при fencing) и остаётся живым standby (`is_active` 0, таргет Prometheus up); повторная инициализация transactional producer после потери лидерства запрещена — только при новом получении лидерства.
- Эксклюзивность слота PostgreSQL — последняя линия защиты: даже при ошибке координации два walsender на одном слоте невозможны (55006 — транзиентная ситуация на время переключения).
- Сейчас сервис реализован по задачам 1–7 (конфигурация + стартовый блок + чтение слота + публикация в Kafka + checkpoint-стор в Kafka и подтверждение LSN + устойчивость + Recovery Mode): `cdc-outbox-1` держит активный walsender слота `outbox_slot` (виден в `pg_stat_replication` как `application_name=cdc-outbox`), декодирует INSERT `outbox.messages` в `OutboxMessage` и публикует в `outbox.events` (один батч ≤ `BatchSize` в полёте, доставка = delivery-репорт; заголовки — JSON-объект `headers`; провал доставки — упорядоченная очередь повторов, новые не публикуются пока она не пуста — контракт `outbox-table.md` «Публикация в Kafka»). **LSN подтверждается после durable checkpoint** (контракт — `outbox-table.md` «Подтверждение LSN и checkpoint»): `confirmed_flush_lsn` двигается за водяным знаком доставки, удерживаемый слотом WAL в покое ≈ 0 (`pg_slot_wal_retained_bytes`); записи видны в `cdc-checkpoints` (`kafka-console-consumer --topic cdc-checkpoints --from-beginning`), метрики `outbox_checkpoint_age_seconds` / `outbox_checkpoint_lag_bytes` / `outbox_checkpoint_errors_total`. Проверено 2026-11-01: `load 500` → verdict **pass** (500/500, дубликатов 0, retained 0); hard-kill под нагрузкой (2000@100/с) → дубликаты ровно в окне неподтверждённого (100), verdict pass; drop слота при живом checkpoint → старт валится явной ошибкой «автопересоздание запрещено», слот не пересоздаётся. **Сброс позиции для чистых замеров и восстановление после утраты слота — Recovery Mode (задача 7)**: stop cdc-outbox-1 → `pg_drop_replication_slot('outbox_slot')` на лидере → `CDCOUTBOX__RECOVERYMODE=true docker compose up -d --no-deps cdc-outbox-1` — WARN «Recovery Mode: checkpoint X → Y», слот создан с текущей позиции WAL, checkpoint переписан до начала чтения (в compose флаг идёт через интерполяцию `${CDCOUTBOX__RECOVERYMODE:-false}`) → перезапуск без флага (`docker compose up -d --no-deps cdc-outbox-1`); пересоздавать `cdc-checkpoints` больше не нужно. Recovery при включённой паре безопасен без остановки соседа (закрыто задачей 10): recovery выполняет только лидер, standby с флагом ничего не пишет до получения лидерства. Старт без флага после дропа слота закономерно валится явной ошибкой «автопересоздание запрещено» (штатная защита, слот не пересоздаётся) — это и есть сигнал включать recovery. Обрыв чтения (HAProxy рвёт простаивающее соединение примерно раз в сутки) → переподключение с `confirmed_flush`, пере-доставляется только окно неподтверждённого. Нагрузка больше `QueueCapacity` (2048) при стоящей публикации останавливает чтение backpressure'ом (метрика `read_messages_current` замирает на границе канала); парная работа (rebalance, fencing) проверена задачей 2026-11-04: `fail outbox-1` под нагрузкой 2000@30/с → outbox-2 принял лидерство (~15–20 с), `outbox_failovers_total` +1, `active_pid` у соединения outbox-2, verdict pass; graceful stop → мгновенная передача; `restore outbox-1` → возвращается standby, лидерство не крадёт (CooperativeSticky); recovery на standby не пишет и не откатывает checkpoint лидера; harness-ветки `fail outbox-1|outbox-2|outbox-all` + `restore` работают (ветки outbox-2 — при включённом профиле, иначе понятный отказ).
- **Устойчивость (задача 6, контракт — `outbox-table.md` «Переподключения и устойчивость», проверено 2026-11-02)**: ошибка чтения не роняет хост — транзиентные ошибки (сеть/55006/25006/42704 «слот пропал» — решение владельца: сервис не падает/декодирование) ретраятся с backoff (база `RETRYDELAYSECONDS`, удвоение до `MAXRETRYDELAYSECONDS`, сброс стабильной сессией `BACKOFFRESETSESSIONSECONDS`), каждая повторная сессия считает `outbox_slot_reconnects_total`; инвалидированный слот — терминально: Critical-лог, хост останавливается, без рестарт-политики контейнер остаётся exited. Живые проверки: `fail pg` → переподключение к новому лидеру (reconnects 5→10с backoff, verdict pass 500/500); `fail kafka2` под нагрузкой → pass 1000/1000; `fail kafka-all` под нагрузкой → WAL копится у слота (backpressure), после возврата брокеров догоняет — pass 1500/1500, дубликаты = окно at-least-once (записано в Kafka, delivery-репорт не вернулся); `fail haproxy` → ретраи с backoff, после restore pass 500/500; **graceful stop под нагрузкой** (`docker compose stop cdc-outbox-1`) — порядок ACK → checkpoint → подтверждение → закрытие, после остановки `confirmed_flush == checkpoint`, рестарт «продолжение с позиции слота» с нулём пере-доставки (pass 1000/1000, дубликатов 0); **дроп слота на живом сервисе** → сервис НЕ падает, ретраит 42704 с насыщением backoff, восстановление — Recovery Mode (процедура сброса выше).
- **Recovery Mode (задача 7, контракт — `outbox-table.md` «Recovery Mode», проверено 2026-11-02)**: env `CDCOUTBOX__RECOVERYMODE=true` на один старт (в обычном режиме защита согласования из задачи 5 не меняется). Живой сценарий на стенде: stop → load 2×200 в недоставленном окне → дроп слота → старт без флага — терминальная ошибка «автопересоздание запрещено», контейнер exited; старт с флагом — слот пересоздан (`restart_lsn=0/4AE4148`), checkpoint переписан `0/4A9BB58 → 0/4AE4148` **до** начала чтения, WARN в логе, `outbox_recovery_mode{slot="outbox_slot"} 1`; свежий `load 500` → verdict **pass** (дублей 0); записи старых runId в `outbox.events` — 0 (окно осознанно пропущено, verdict старого прогона показал бы missing); рестарт без флага — штатное согласование (diff=0), gauge 0, `check` 59/59. Автодроп существующего слота запрещён (пустой `confirmed_flush`/чужой плагин — терминальная ошибка с инструкцией); при согласованном состоянии флаг — no-op с WARN (забытый флаг безопасен).
- **У cdc-outbox-1/2 нет рестарт-политики** (решение владельца, задача 6): упавший или остановленный тестом экземпляр среда не поднимает — только явно (`docker compose start cdc-outbox-1` / `stand.sh restore outbox-1`); у compose `stop_grace_period: 45s` > бюджет остановки сервиса `SHUTDOWNTIMEOUTSECONDS` (30с) — требование «таймаут принудительного завершения средой больше бюджета остановки сервиса» (`outbox-table.md`).

## Объекты БД и роли

Создаются init-db (`infrastructure/postgres/init/`), идемпотентно:

- БД **`outbox`** (не `postgres`) — создаётся bootstrap-методом Patroni `bootstrap-with-appdb` ещё до старта кластера: PG17 slotsync worker синхронизирует failover-слоты только БД из `dbname` primary_conninfo (`docs/dependencies.md` §5).
- Схема `outbox` + таблица **`outbox.messages`** — канонический DDL `docs/dependencies.md` §3.
- **`sys_writer`** — Writer: `CONNECT` на БД, `USAGE` на схему, `INSERT, DELETE` на таблицу, колоночный `SELECT (id)`, `USAGE` на identity-последовательность.
- **`sys_replication`** (`REPLICATION`) — outbox-сервис cdc-outbox (чтение слота, репликационный протокол); гранты — init (CONNECT на БД и USAGE схемы — 01-appdb-objects.sql; табличные гранты стримингу не требуются).
- **`sys_monitor`** (`pg_monitor` с `INHERIT TRUE`) — postgres-exporter: чтение статистики кластера, без REPLICATION и прав на прикладные таблицы (мониторинг не может потреблять или двигать слот). `pg_monitor` — встроенная роль (объединяет pg_read_all_settings/pg_read_all_stats/pg_stat_scan_tables), её достаточно для pg_stat_replication и pg_replication_slots, откуда берутся lag/состояние слота/WAL retention. Грабля PG16+ (проверена на старом стенде): у членства в роль собственная опция inherit, фиксируемая в момент GRANT по атрибуту роли на тот момент — первый GRANT при NOINHERIT-роли записывает inherit=false, и позже `ALTER ROLE ... INHERIT` её НЕ обновляет (permission denied на каждом scrape); поэтому роль создаётся INHERIT, а грант даётся `WITH INHERIT TRUE`.
- `synchronized_standby_slots` = **активные** физические слоты кластера (`02-sync-slots.sql`): слот мёртвого стендбая в список не включается — его включение блокирует логический декодинг primary до возврата стендбая (грабля найдена задачей 6; после восстановления — повторный `apply-bootstrap.sh`, `docs/dependencies.md` §5).

- **Publication `outbox_publication` создаёт init** (`03-outbox-publication.sql`, от postgres; гранты `sys_replication` — там же). `sys_replication` создать её не может: CREATE PUBLICATION требует CREATE на БД и владения таблицей — экспериментально подтверждено, ремарка в `docs/dependencies.md` §4. **Failover-слот `outbox_slot` init НЕ создаёт** — его создаёт сам outbox-сервис при первом старте идемпотентно (`docs/dependencies.md` §5, задача 2): до первого старта cdc-outbox-1 слота нет — это норма (непотребляемый слот со временем перестаёт синхронизироваться и при promotion теряется — поведение PG17), `check` проверяет слот как выполненную задачу 2. Метрики `pg_slot_*` видят `outbox_slot` после первого старта сервиса (до него — только физические слоты Patroni).

## Кастомные запросы postgres-exporter (queries.yaml)

Кастомные метрики PG — `infrastructure/prometheus/postgres-exporter-queries.yaml`: имя ключа = префикс метрик (`pg_wal_position_*`, `pg_slot_*`, …); краткое назначение каждого запроса — однострочным комментарием в самом файле. Конвенции, которым следует файл:

- bool → `CASE … 1/0` — экспортер не умеет bool;
- байты — `::double precision`: LSN до 2^48 точно помещается в float64;
- `WHERE NOT pg_is_in_recovery()` у primary-only запросов (`pg_wal_position`, `pg_slot*`, `pg_replication`): HAProxy ведёт к primary, но в окне failover соединение может попасть на реплику — там `pg_current_wal_lsn()` падает ошибкой и ломал бы весь scrape (`pg_wal_dir` фильтра не имеет — работает на любой ноде);
- колонки с возможными NULL — в отдельных запросах с фильтром `IS NOT NULL` (`pg_slot_flush_lag`, `pg_slot_safe_wal`): иначе экспортер пишет NaN-сэмплы (synced-слоты без confirmed_flush, проверено на старом стенде);
- префиксы `pg_slot_*` не совпадают со встроенным коллектором replication_slot (его метрики — `pg_replication_slot_*_lsn` как float) — по имени не путаются источники;
- `pg_wal_lsn_diff(end, start)` = end − start — порядок аргументов важен (перепутанный порядок даёт отрицательные значения, поймано первым прогоном);
- в `pg_replication` nullable-колонки write/flush_lag сознательно не берутся; synced-копии failover-слота живут на standby и с primary не видны.

Встроенные коллекторы stat_bgwriter/database/wal выключены флагами в docker-compose.yml (обоснование — комментарий сервиса postgres-exporter: WAL-метрики — один источник правды, queries.yaml).

## Что НЕ перенесено из streaming-polling-outbox

- **PgBouncer** — Writer ходит напрямую через HAProxy; pooling — встроенный пул Npgsql (MaxPoolSize 72).
- **Polling-слой**: роль `sys_app`, схема `cdc`, SECURITY DEFINER-функции peek/advance/outbox_slot_status — в этом проекте polling-механики нет.
- **CDC-сервисы** (cdc-polling/cdc-streaming) и их compose-профили — заменит единый outbox-сервис.
- **cAdvisor** — на Docker Desktop (containerd image store) видит только VM-root, per-container серий нет; если понадобятся per-container CPU/RAM — `docker stats`.
- **Харнессы**: health-check старого check-infra заменён `stand.sh check`; приёмочные харнессы доставки написаны заново (задача 11, раздел «Приёмочные харнессы»). Не перенесены: ha-suite (полный набор HA-прогонов HA-001…008 — базовые инъекции закрывают `stand.sh fail/restore` и приёмочный харнесс), benchmark (нагрузочные A/B-измерения старого исследования), collect-results (сбор артефактов прогонов).
- reconcile-скрипт остатков слота (03-slot-reconcile) — не перенесён: слот есть (создаёт cdc-outbox), остатки слота на репликах (грабли PG17 slotsync старого стенда) здесь не наблюдались; после каждой смены primary применяется `apply-bootstrap.sh`. Вернётся, если грабли повторятся.

## Что появилось с outbox-сервисом

- ~~Слот `outbox_slot` создаёт сервис~~ (появилось, задача 2); ~~публикация и чтение слота~~ (появилось, задачи 3–4): вердикты валидатора содержательные — `load 500 && verdict` → pass, missing=0; подтверждение LSN/checkpoint ~~— задача 5~~ (появилось, задача 5: `confirmed_flush` двигается, рестарт продолжает с позиции слота).
- Compose-часть готова: экземпляр `cdc-outbox-1` (скрейпится Prometheus'ом); резерв `cdc-outbox-2` — за профилем `redundancy` (резервирование Active-Passive реализовано задачей 10 — раздел выше; таргет Prometheus включается вместе с профилем). Конфигурационные env (`CDCOUTBOX__*`, одинаковые у обоих экземпляров — YAML-якорь) — таблица в `docs/dependencies.md` §2 «Конфигурация outbox-сервиса» (подключение PG — одна строка, пароль интерполяцией из `.env`); служебный topic `cdc-checkpoints` (checkpoint'ы + лидерство consumer group) уже создаёт kafka-init; ~~харнессы приёмки под доставку — задача 11~~ (написаны — раздел «Приёмочные харнессы»).
- После каждой смены primary — `scripts/init/apply-bootstrap.sh` + при зависшем graceful switchover с живым консюмером слота рестарт активного экземпляра (грабли старого стенда).

## Приёмочные харнессы (задача 11)

Оба живут в `scripts/test/`, запускаются на поднятом стенде (предусловия проверяют сами), оставляют стенд рабочим (все инъекции восстанавливаются), завершаются «ИТОГ: PASS/FAIL» — код возврата 0 только при нулевом FAIL. Вердикты после отказов допускают дубликаты (at-least-once): pass ⇔ missing=0 && unexpected=0.

Известное ограничение (обновлено 2026-11-05): все три дефекта приёмки задачи 11 закрыты. Четвёртая находка приёмки (2026-11-05) — не продукт, а тест: юнит happy-path гранта (`Grant_RegistersFailover_InitsProducerBeforeChecks_RunsDelivery`) флакал на HEAD (3 из 4 прогонов красные; на e7f8f17 — 4/4 зелёные, фикс гранта поднял вероятность) — ожидание срабатывало по записи ридера в канал, а ассерты записей Kafka шли до дренирования канала публикатором; тест исправлен: ждёт появления самих записей (поллинг по Count без перечисления — список пишет поток публикатора), стабилен 6/6 × 170/170, решение 264/264. Повторное лидерство тем же процессом больше не роняет хост — продюсер пересоздаётся на каждый период лидерства, `InitTransactions` всегда на свежем экземпляре (проверено живьём двойным отбоем всех брокеров: партиция вернулась тому же процессу, `outbox_failovers_total` 1→2, оба экземпляра живы). Гонка финального checkpoint при полном outage Kafka тоже закрыта (2026-11-04, заказ владельца): опоздавший commit фиксировал позицию выше подтверждённой слота, и следующий лидер погибал терминальным reconcile «checkpoint опережает confirmed_flush» — теперь опережение при живом слоте не терминально: WARN + продолжение с позиции слота, окно пере-доставится дублями (at-least-once); терминальным остаётся «слота нет при живом checkpoint», Recovery Mode для случая опережения больше не нужен (штатный рестарт самовосстанавливается). Обоснование стратегии для всех вариантов расхождения — recommendations.md «Согласование checkpoint-стора с replication slot при старте». Третий дефект вскрыл повторный прогон приёмки 2026-11-05 (пара пересобрана из HEAD): транзиентный обрыв PG-чтения (`Npgsql EndOfStreamException`) при повторном получении лидерства после outage терминировал хост catch-all'ом гранта («Запуск доставки не удался. Хост завершается») — рестарт-политики нет, экземпляр лежал до ручного подъёма; исправлен 2026-11-05 (заказ владельца: хост не останавливается на ошибках, восстановимых в перспективе): восстановимые ошибки запуска на лидерстве — повторы с backoff `RetryDelaySeconds`/`MaxRetryDelaySeconds` со свежим DI-скоупом попытки и удержанием лидерства (отзыв партиций прерывает повторы), терминальные нарушения стартовых проверок выделены в `StartupCheckException` → остановка хоста (группу принимает standby), fencing при `InitTransactions` — standby без остановки хоста; контракт — outbox-table.md «Параллельные экземпляры». Полный прогон харнесса после всех трёх фиксов (2026-11-05): 97 PASS / 1 FAIL — T4c пережит парой: после restore оба экземпляра живы (ассерт «активен ровно один» зелёный — до фикса повторный прогон ловил лежащий cdc-outbox-1), WARN опережения на новом лидере, доставка pass 600/600, дублей 300 (пере-доставленное окно at-least-once), подписей «Хост завершается» и «Operation not valid in state Ready» в логах обоих 0, recovery-процедура не потребовалась. Единственный FAIL — ассерт T5 `outbox_failovers_total` +1: харнесс T5 был законтрактован на лидера cdc-outbox-1, но после пережитого T4c лидером стоит cdc-outbox-2 — kill standby переключения не давал. T5 исправлена (2026-11-05, заказ владельца): лидер определяется по is_active (после T4c им может быть любой из пары), kill именно лидера, лидерство и failover-счётчик проверяются у выжившего партнёра, возврат — standby; пустой failover-счётчик до переключения трактуется как 0 — счётчик OTel без записей не экспортируется, taker, ни разу не лидерствовавший в этом процессе, метрики не имеет. Прогоны после правки: 98 PASS / 0 FAIL дважды (в обоих лидером к T5 оказался cdc-outbox-1: kill → cdc-outbox-2 принимает, failovers 2→3 и 3→4, verdict pass 900/900, возврат standby); ветка «лидер cdc-outbox-2» добита ручной прогонкой фазы (kill → cdc-outbox-1 принимает, счётчик «нет метрики»→1, verdict pass 600/600, возврат standby). Первый полностью зелёный прогон приёмки. Одиночный контур (без профиля redundancy) полный outage проходил ещё в задаче 6 (pass 1500/1500, дубликаты = окно at-least-once).

- **`scripts/test/integration-delivery.sh`** — приёмка конвейера end-to-end (~15–20 мин с поднятой парой): запись → вердикт чистого прогона → живая проба контракта `headers` (непустые заголовки через INSERT+DELETE одной транзакцией, чтение `kafka-console-consumer --property print.headers=true`) → рестарт cdc-outbox-1 в середине прогона (продолжение от `confirmed_flush`) → отказ-восстановление: `fail pg` (kill лидера Patroni под нагрузкой), `fail kafka2` (минорное большинство), `fail kafka-all` (полный outage: WAL у слота растёт, после `restore` backlog дренируется) → при поднятой паре cdc-outbox-2 автоматически добавляется фаза Active-Passive (лидер определяется по is_active — после полного outage им может быть любой из пары; kill лидера → доставку принимает партнёр, `outbox_failovers_total` +1 у принявшего) → финальный `stand.sh check`. Инъекции делегированы `stand.sh` (общая хореография: выборы + bootstrap + settle).
- **`scripts/test/upgrade-rehearsal.sh`** — репетиция обновления PostgreSQL по пути dump/restore: базовый прогон pass → тишина (idle-предупреждение в логах + `outbox_checkpoint_lag_bytes=0`) → graceful stop cdc-outbox-1 → `pg_dump` БД outbox → «новый кластер» (`DROP DATABASE WITH (FORCE)` + `CREATE` + restore дампа; слот исчезает вместе со старой БД — допущение пути dump/restore) → `apply-bootstrap.sh` (CONNECT-гранты ролей и publication — их возвращает bootstrap: гранты БД в plain `pg_dump` не входят, publication выгружается) → recovery-старт `CDCOUTBOX__RECOVERYMODE=true` (слот пересоздан, checkpoint переписан, старые прогоны не пере-доставляются) → свежий прогон pass → возврат к дефолту (перезапуск без флага; если пара была поднята — возвращается). **Сценарий разрушительный для БД outbox** — запускать отдельно от приёмочного харнесса; при поломке стенд пересоздаётся `wipe` + `up`.

## Грабли (унаследовано из старого стенда, проверено там)

- **Никогда не запускать `docker compose up`/`run` без `--no-deps` на живом стенде** — compose реконсилят весь граф зависимостей и могут пересоздать pg-контейнеры: одновременный рестарт всех нод Patroni = флап промоушенов и рассинхрон failover-слотов. Один сервис: `up -d --no-deps <name>`.
- **После пересоздания pg-контейнеров — `docker compose restart haproxy`**: HAProxy резолвит имена pg-нод один раз при старте, новые IP не подхватываются сам собой (в cfg есть `resolvers docker`, но рестарт надёжен).
- `docker compose kill` pg-ноды/kafka-брокера НЕ перезапускает контейнер (Docker трактует API-kill как ручную остановку) — возврат явным `docker compose start` после выборов лидера; используется инъекциями `stand.sh fail`/`restore`.
- **prometheus.yml примонтирован одиночным файлом**: после правки на хосте живой контейнер видит СТАРУЮ копию (bind-mount держит исходный inode, `kill -HUP` не помогает) — только `docker compose restart prometheus`. То же для queries.yaml + `restart postgres-exporter`.
- Свежий стенд: первые минуты возможны кэшированные ошибки подключения — самоизлечивается (ретраи Writer).
- Редактировать работающий bash-скрипт НЕЛЬЗЯ (bash дочитывает файл по offset) — фиксить после завершения прогона.
