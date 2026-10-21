-- synchronized_standby_slots — защитная настройка кластера под failover-слот
-- (docs/dependencies.md §5). Сам слот outbox_slot НЕ создаётся: его создаёт сам
-- outbox-сервис при первом старте (идемпотентно). До появления слота настройка
-- не имеет эффекта, но кластер сразу готов к его созданию.
--
-- synchronized_standby_slots на primary принимает ТОЛЬКО физические слоты
-- (PG17: это слоты стендбаев, за WAL которых primary следит перед recycle,
-- чтобы failover логического слота не терял данные). Patroni сам этот параметр
-- не выставляет — собираем список из фактических физических слотов.
--
-- Замечание: ALTER SYSTEM пишет postgresql.auto.conf локально; после смены
-- primary повторный запуск scripts/init/apply-bootstrap.sh обновит значение
-- на новом лидере.
--
-- Выполняется от имени postgres на primary (через HAProxy); pg_replication_slots
-- видна из любой БД — \connect не нужен.

SELECT format('ALTER SYSTEM SET synchronized_standby_slots = %L',
              (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
               FROM pg_replication_slots WHERE slot_type = 'physical'))
WHERE (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
       FROM pg_replication_slots WHERE slot_type = 'physical') IS NOT NULL \gexec
SELECT pg_reload_conf();
