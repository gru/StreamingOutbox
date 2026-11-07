-- Защита failover-слота outbox_slot: primary удерживает WAL за физическими
-- слотами стендбаев (synchronized_standby_slots), чтобы логический слот не
-- терял данные при promotion. В список идут только
-- активные слоты — слот мёртвого стендбая блокирует логический декодинг.

SELECT format('ALTER SYSTEM SET synchronized_standby_slots = %L',
              (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
               FROM pg_replication_slots WHERE slot_type = 'physical' AND active))
WHERE (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
       FROM pg_replication_slots WHERE slot_type = 'physical' AND active) IS NOT NULL \gexec
SELECT pg_reload_conf();
