-- Прикладная БД outbox, схема и таблица outbox.messages (README «Таблица outbox»).

SELECT 'CREATE DATABASE outbox' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'outbox') \gexec

REVOKE ALL ON DATABASE postgres FROM PUBLIC;

\connect outbox

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

REVOKE ALL ON DATABASE outbox FROM PUBLIC;
GRANT CONNECT ON DATABASE outbox TO sys_writer;
GRANT CONNECT ON DATABASE outbox TO sys_replication;
GRANT CONNECT ON DATABASE outbox TO sys_monitor;

REVOKE CREATE ON SCHEMA outbox FROM PUBLIC;
GRANT USAGE ON SCHEMA outbox TO sys_writer;

-- Стримингу pgoutput табличные гранты не нужны; USAGE схемы — для служебных
-- SQL-запросов сервиса (README «Publication»).
GRANT USAGE ON SCHEMA outbox TO sys_replication;

GRANT INSERT, DELETE ON outbox.messages TO sys_writer;

-- SELECT(id): PostgreSQL требует SELECT на колонки, читаемые в WHERE DELETE.
GRANT SELECT (id) ON outbox.messages TO sys_writer;

-- Имя последовательности не хардкодим: резолвим pg_get_serial_sequence
-- (фактическое — outbox.messages_id_seq).
SELECT format('GRANT USAGE ON SEQUENCE %s TO sys_writer', pg_get_serial_sequence('outbox.messages', 'id'))
WHERE pg_get_serial_sequence('outbox.messages', 'id') IS NOT NULL \gexec
