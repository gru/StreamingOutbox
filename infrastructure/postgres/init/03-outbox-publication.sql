-- Publication outbox_publication (README «Publication»): создаёт инициализация,
-- а не outbox-сервис — CREATE PUBLICATION требует CREATE на БД и владения таблицей,
-- у sys_replication таких прав нет; сервис при старте только проверяет её конфигурацию.

\connect outbox

-- Идемпотентная форма (README «Publication»).
SELECT 'CREATE PUBLICATION outbox_publication'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec

SELECT format('ALTER PUBLICATION outbox_publication ADD TABLE %I.%I', 'outbox', 'messages')
WHERE NOT EXISTS (
  SELECT FROM pg_publication_tables
  WHERE pubname = 'outbox_publication' AND schemaname = 'outbox' AND tablename = 'messages'
) \gexec

ALTER PUBLICATION outbox_publication SET (publish = 'insert');
