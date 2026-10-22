-- Publication outbox_publication и гранты outbox-сервиса — sys_replication
-- (docs/dependencies.md §4). Идемпотентно. Выполняется от имени postgres на
-- primary (через HAProxy), подключение переключается на БД outbox.
--
-- ПОЧЕМУ publication создаёт инициализация, а не outbox-сервис (ремарка;
-- экспериментально подтверждено на стенде): CREATE PUBLICATION требует
-- привилегию CREATE на текущую БД, а добавление таблицы — владения таблицей.
-- У sys_replication нет ни того, ни другого (минимальные привилегии;
-- SUPERUSER прикладным ролям запрещён): попытка
--   CREATE PUBLICATION ... FOR TABLE outbox.messages
-- от sys_replication падает «permission denied for database outbox». Выдавать
-- CREATE на БД или владение прикладной таблицей сервисной роли нельзя, поэтому
-- publication создаётся здесь от postgres, а outbox-сервис при старте только
-- проверяет её конфигурацию (имя, publish='insert', состав таблиц).
--
-- Failover-слот outbox_slot (dependencies.md §5), в отличие от publication,
-- создаёт сам outbox-сервис при первом старте: pg_create_logical_replication_slot
-- требует только атрибут REPLICATION (есть у sys_replication; проверено тем же
-- экспериментом — probe-слот создавался и удалялся от этой роли без ошибок).

\connect outbox

-- Каноническая идемпотентная форма (dependencies.md §4).
SELECT 'CREATE PUBLICATION outbox_publication'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec

SELECT format('ALTER PUBLICATION outbox_publication ADD TABLE %I.%I', 'outbox', 'messages')
WHERE NOT EXISTS (
  SELECT FROM pg_publication_tables
  WHERE pubname = 'outbox_publication' AND schemaname = 'outbox' AND tablename = 'messages'
) \gexec

ALTER PUBLICATION outbox_publication SET (publish = 'insert');

-- Гранты outbox-сервиса (sys_replication). CONNECT на БД outbox выдан в
-- 01-appdb-objects.sql. Стриминг pgoutput НЕ требует SELECT на outbox.messages
-- (проверено исследовательским стендом streaming-polling-outbox: доставка
-- 525k сообщений ролью без табличных грантов) — тела сообщений читаются из
-- слота. USAGE на схему — для служебных SQL-запросов сервиса к объектам схемы;
-- каталоги pg_replication_slots роли доступны и без грантов.
GRANT USAGE ON SCHEMA outbox TO sys_replication;
