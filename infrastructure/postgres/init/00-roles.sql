-- Роли прикладных пользователей и мониторинга.

-- sys_writer: Writer — запись тестовой нагрузки в outbox.messages.
SELECT format('CREATE ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD %L',
              :'sys_writer_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_writer') \gexec

ALTER ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD :'sys_writer_password';

-- sys_replication: outbox-сервис cdc-outbox — чтение слота логической репликации (pgoutput).
SELECT format('CREATE ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD %L',
              :'sys_replication_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_replication') \gexec

ALTER ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD :'sys_replication_password';

-- sys_monitor: postgres-exporter — чтение статистики кластера (pg_monitor).
SELECT format('CREATE ROLE sys_monitor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD %L',
              :'sys_monitor_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_monitor') \gexec

ALTER ROLE sys_monitor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD :'sys_monitor_password';

GRANT pg_monitor TO sys_monitor WITH INHERIT TRUE;
