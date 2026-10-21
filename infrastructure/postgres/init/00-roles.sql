-- Роли прикладных пользователей и мониторинга (docs/dependencies.md §3, docs/stand.md).
-- Идемпотентно: повторный запуск не создаёт дубликаты и приводит пароли/атрибуты
-- к заданному состоянию. Пароли передаются через psql-переменные (-v), чтобы
-- не хранить их в исходном коде.
--
-- Выполняется от имени postgres на primary (через HAProxy), подключение к БД postgres.
-- sys_app (SQL Polling старого стенда streaming-polling-outbox) НЕ переносится:
-- polling-механика в этом проекте отсутствует.

-- sys_writer: только запись тестовой нагрузки в outbox.messages (гранты —
-- 01-appdb-objects.sql); прав репликации и чтения таблицы не нужно.
SELECT format('CREATE ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD %L',
              :'sys_writer_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_writer') \gexec

ALTER ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD :'sys_writer_password';

-- sys_replication: будущий outbox-сервис — чтение слота через протокол
-- логической репликации (Npgsql Logical Replication API, docs/dependencies.md §5).
-- Гранты на прикладные объекты добавит этап outbox-сервиса; здесь — только роль.
SELECT format('CREATE ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD %L',
              :'sys_replication_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_replication') \gexec

ALTER ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD :'sys_replication_password';

-- sys_monitor: postgres-exporter (подключение напрямую через HAProxy, всегда к
-- primary; одного собственного соединения достаточно). Роль читает статистику
-- кластера, но не имеет ни REPLICATION, ни прав на прикладные таблицы: мониторинг
-- не может потреблять или двигать слот.
--
-- pg_monitor — встроенная роль PostgreSQL (не SUPERUSER): объединяет
-- pg_read_all_settings, pg_read_all_stats и pg_stat_scan_tables. Её достаточно
-- для pg_stat_replication и pg_replication_slots, из которых берутся
-- replication lag, состояние logical slot и WAL retention (кастомные запросы
-- infrastructure/prometheus/postgres-exporter-queries.yaml).
--
-- Привилегии pg_monitor обязаны действовать при логине (pg_ls_waldir,
-- pg_stat_replication): роль — INHERIT, а членство — WITH INHERIT TRUE.
-- Грабли PG16+: у членства своя опция inherit, она фиксируется в момент
-- GRANT по атрибуту роли на тот момент; позже ALTER ROLE ... INHERIT её
-- НЕ обновляет (первый грант при NOINHERIT-роли записал inherit=false —
-- permission denied на каждом scrape, проверено на старом стенде).
SELECT format('CREATE ROLE sys_monitor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD %L',
              :'sys_monitor_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_monitor') \gexec

ALTER ROLE sys_monitor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD :'sys_monitor_password';

GRANT pg_monitor TO sys_monitor WITH INHERIT TRUE;
