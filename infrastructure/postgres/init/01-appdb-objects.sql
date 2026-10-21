-- Прикладная БД outbox, схема и таблица outbox.messages (docs/dependencies.md §3).
-- Идемпотентно. Выполняется от имени postgres на primary (через HAProxy),
-- стартует подключённым к postgres, дальше переключается на outbox.
--
-- БД outbox на холодном старте уже создаёт bootstrap-метод Patroni
-- (bootstrap-with-appdb.sh); CREATE DATABASE ниже идемпотентен и покрывает
-- повторные прогоны bootstrap на существующем кластере (CREATE DATABASE вне
-- транзакции — psql \gexec).
--
-- Publication и failover-слот здесь СОЗНАТЕЛЬНО не создаются: по контракту
-- проекта их создаёт сам outbox-сервис при первом старте / миграциях
-- (docs/dependencies.md §4–§5). До его появления слот не нужен и не
-- синхронизируется (непотребляемый слот — грабли PG17 slotsync).

SELECT 'CREATE DATABASE outbox' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'outbox') \gexec

-- postgres остаётся сервисной БД кластера: прикладным ролям там делать нечего.
REVOKE ALL ON DATABASE postgres FROM PUBLIC;

\connect outbox

-- Outbox-таблица — канонический DDL docs/dependencies.md §3. Таблица
-- сознательно без лишних индексов (PK достаточно): индексы увеличивают объём
-- WAL на INSERT и искажают нагрузочные измерения; алгоритм выборки и очистка
-- определяются реализацией процессора (docs/outbox-table.md).
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

-- Отзыв избыточных прав PUBLIC, затем явные минимальные GRANT.
REVOKE ALL ON DATABASE outbox FROM PUBLIC;
GRANT CONNECT ON DATABASE outbox TO sys_writer;      -- запись тестовой нагрузки
GRANT CONNECT ON DATABASE outbox TO sys_replication; -- будущий outbox-сервис (replication-протокол)
GRANT CONNECT ON DATABASE outbox TO sys_monitor;     -- postgres-exporter (custom queries)

-- Схема outbox: запись требует USAGE; CREATE не выдаём никому.
REVOKE CREATE ON SCHEMA outbox FROM PUBLIC;
GRANT USAGE ON SCHEMA outbox TO sys_writer;

-- Минимум для Writer: INSERT + DELETE (id генерирует identity-последовательность
-- БД; UPDATE не нужен). Writer держит таблицу ПУСТОЙ: INSERT и DELETE выполняются
-- в одной транзакции — строка существует только внутри транзакции, сообщение
-- живёт в WAL и доставляется слотом (docs/dependencies.md §3, «Запись тестной нагрузки»).
GRANT INSERT, DELETE ON outbox.messages TO sys_writer;

-- Колоночный SELECT(id): PostgreSQL требует SELECT на колонки, читаемые в WHERE
-- команды DELETE (docs: "the SELECT privilege on any column whose values are read
-- in the condition"). DELETE-очистка Writer фильтрует по id = ANY($1) — достаточно
-- SELECT только на эту колонку; SELECT всей таблицы (body и пр.) НЕ выдаётся.
GRANT SELECT (id) ON outbox.messages TO sys_writer;

-- Резерв id до сборки body — nextval по identity-последовательности таблицы.
-- Имя последовательности НЕ хардкодим: резолвим pg_get_serial_sequence (имя
-- выдаёт фактическое — outbox.messages_id_seq).
SELECT format('GRANT USAGE ON SEQUENCE %s TO sys_writer', pg_get_serial_sequence('outbox.messages', 'id'))
WHERE pg_get_serial_sequence('outbox.messages', 'id') IS NOT NULL \gexec
