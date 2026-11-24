-- Варианты доставки рядом со штатной таблицей outbox.messages:
--   1) Целевой безтабличный WAL (pg_logical_emit_message, формат OBX1, пустая publication outbox_wal_publication);
--   2) Почасовая секционированная таблица-страховка outbox.messages_partitioned (INSERT-only без DELETE,
--      publication outbox_partitioned_publication с publish_via_partition_root = true,
--      SECURITY DEFINER-функции нарезки на 48ч вперёд и безопасного удаления после сдвига LSN);
--   3) Единая атомарная функция outbox.publish_message(..., p_write_to_table boolean) для переключения INSERT ↔ WAL.
-- Идемпотентно. Выполняется от имени postgres на primary.

\connect outbox

-- ---------- 1. Секционированная таблица-страховка (контракт колонок 1-в-1 с outbox.messages) ----------

CREATE TABLE IF NOT EXISTS outbox.messages_partitioned (
    id               bigint       GENERATED ALWAYS AS IDENTITY NOT NULL,
    topic            varchar(255) NOT NULL,
    key              bytea        NOT NULL,
    body             bytea        NOT NULL,
    headers          varchar      NOT NULL,
    partition        integer,
    producer_key     varchar(255),
    created_at       timestamptz  NOT NULL DEFAULT now(),
    outbox_partition integer      NOT NULL DEFAULT 0,
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

-- ---------- 2. Служебные таблицы реестра партиций и защитного курсора ----------

CREATE TABLE IF NOT EXISTS outbox.partition_registry (
    schema_name     text        NOT NULL,
    parent_table    text        NOT NULL,
    partition_name  text        NOT NULL,
    range_start_utc timestamptz NOT NULL,
    range_end_utc   timestamptz NOT NULL,
    sealed_lsn      pg_lsn,
    sealed_at_utc   timestamptz,
    dropped_at_utc  timestamptz,
    PRIMARY KEY (schema_name, parent_table, partition_name)
);

CREATE TABLE IF NOT EXISTS outbox.partition_guard (
    slot_name                     text        PRIMARY KEY,
    system_identifier             bigint      NOT NULL,
    contiguous_delivered_lsn      pg_lsn,
    last_delivered_created_at_utc timestamptz,
    recovery_in_progress          boolean     NOT NULL DEFAULT false,
    updated_at_utc                timestamptz NOT NULL DEFAULT now()
);

-- ---------- 3. SECURITY DEFINER-функции управления почасовыми партициями ----------

CREATE OR REPLACE FUNCTION outbox.ensure_partitions(
    p_schema          text,
    p_parent          text,
    p_now_utc         timestamptz DEFAULT now(),
    p_precreate_hours integer     DEFAULT 48
)
RETURNS TABLE(created_count integer, future_count integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
DECLARE
    v_schema       text := lower(btrim(p_schema));
    v_parent       text := lower(btrim(p_parent));
    v_current_hour timestamptz;
    v_start        timestamptz;
    v_end          timestamptz;
    v_part_name    text;
    v_exists       boolean;
    v_created      integer := 0;
    v_future       integer := 0;
    v_hour_offset  integer;
BEGIN
    IF v_schema !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'Недопустимое имя схемы: %', p_schema;
    END IF;
    IF v_parent !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'Недопустимое имя родительской таблицы: %', p_parent;
    END IF;
    IF p_precreate_hours < 1 OR p_precreate_hours > 720 THEN
        RAISE EXCEPTION 'Горизонт предсоздания вне диапазона [1..720]: %', p_precreate_hours;
    END IF;

    v_current_hour := date_trunc('hour', p_now_utc AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';

    FOR v_hour_offset IN 0..p_precreate_hours LOOP
        v_start := v_current_hour + make_interval(hours => v_hour_offset);
        v_end := v_start + interval '1 hour';
        v_part_name := v_parent || '_' || to_char(v_start AT TIME ZONE 'UTC', 'YYYYMMDD_HH24');

        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = v_schema AND c.relname = v_part_name
        ) INTO v_exists;

        IF NOT v_exists THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                v_schema, v_part_name, v_schema, v_parent, v_start, v_end
            );
            v_created := v_created + 1;

            INSERT INTO outbox.partition_registry (
                schema_name, parent_table, partition_name, range_start_utc, range_end_utc
            )
            VALUES (v_schema, v_parent, v_part_name, v_start, v_end)
            ON CONFLICT (schema_name, parent_table, partition_name) DO UPDATE SET
                range_start_utc = EXCLUDED.range_start_utc,
                range_end_utc = EXCLUDED.range_end_utc,
                sealed_lsn = NULL,
                sealed_at_utc = NULL,
                dropped_at_utc = NULL;
        ELSE
            INSERT INTO outbox.partition_registry (
                schema_name, parent_table, partition_name, range_start_utc, range_end_utc
            )
            VALUES (v_schema, v_parent, v_part_name, v_start, v_end)
            ON CONFLICT (schema_name, parent_table, partition_name) DO NOTHING;
        END IF;
    END LOOP;

    SELECT count(*)::integer INTO v_future
    FROM outbox.partition_registry
    WHERE schema_name = v_schema
      AND parent_table = v_parent
      AND dropped_at_utc IS NULL
      AND range_start_utc > v_current_hour;

    RETURN QUERY SELECT v_created, v_future;
END;
$$;

CREATE OR REPLACE FUNCTION outbox.seal_closed_partitions(
    p_schema     text,
    p_parent     text,
    p_cutoff_utc timestamptz
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
DECLARE
    v_sealed integer;
BEGIN
    UPDATE outbox.partition_registry
    SET sealed_lsn = pg_catalog.pg_current_wal_lsn(),
        sealed_at_utc = now()
    WHERE schema_name = lower(btrim(p_schema))
      AND parent_table = lower(btrim(p_parent))
      AND dropped_at_utc IS NULL
      AND sealed_lsn IS NULL
      AND range_end_utc <= p_cutoff_utc;

    GET DIAGNOSTICS v_sealed = ROW_COUNT;
    RETURN v_sealed;
END;
$$;

CREATE OR REPLACE FUNCTION outbox.read_partition_guard(p_slot_name text)
RETURNS TABLE(
    system_identifier             bigint,
    contiguous_delivered_lsn      pg_lsn,
    last_delivered_created_at_utc timestamptz,
    recovery_in_progress          boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
    SELECT g.system_identifier,
           g.contiguous_delivered_lsn,
           g.last_delivered_created_at_utc,
           g.recovery_in_progress
    FROM outbox.partition_guard g
    WHERE g.slot_name = p_slot_name;
$$;

CREATE OR REPLACE FUNCTION outbox.ensure_partition_guard(
    p_slot_name          text,
    p_system_identifier  bigint,
    p_initial_lsn        pg_lsn,
    p_initial_created_at timestamptz
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
    INSERT INTO outbox.partition_guard (
        slot_name, system_identifier, contiguous_delivered_lsn,
        last_delivered_created_at_utc, recovery_in_progress, updated_at_utc
    )
    VALUES (p_slot_name, p_system_identifier, p_initial_lsn, p_initial_created_at, false, now())
    ON CONFLICT (slot_name) DO NOTHING;
$$;

CREATE OR REPLACE FUNCTION outbox.update_partition_guard_progress(
    p_slot_name         text,
    p_system_identifier bigint,
    p_lsn               pg_lsn,
    p_last_created_at   timestamptz
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
    UPDATE outbox.partition_guard
    SET contiguous_delivered_lsn = CASE
            WHEN contiguous_delivered_lsn IS NULL OR p_lsn > contiguous_delivered_lsn THEN p_lsn
            ELSE contiguous_delivered_lsn
        END,
        last_delivered_created_at_utc = CASE
            WHEN p_last_created_at IS NULL THEN last_delivered_created_at_utc
            WHEN last_delivered_created_at_utc IS NULL OR p_last_created_at > last_delivered_created_at_utc THEN p_last_created_at
            ELSE last_delivered_created_at_utc
        END,
        updated_at_utc = now()
    WHERE slot_name = p_slot_name
      AND system_identifier = p_system_identifier
      AND recovery_in_progress = false;
$$;

CREATE OR REPLACE FUNCTION outbox.set_partition_recovery_lock(
    p_slot_name         text,
    p_in_progress       boolean,
    p_system_identifier bigint,
    p_reset_lsn         pg_lsn,
    p_reset_created_at  timestamptz
)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
    INSERT INTO outbox.partition_guard (
        slot_name, system_identifier, contiguous_delivered_lsn,
        last_delivered_created_at_utc, recovery_in_progress, updated_at_utc
    )
    VALUES (p_slot_name, p_system_identifier, p_reset_lsn, p_reset_created_at, p_in_progress, now())
    ON CONFLICT (slot_name) DO UPDATE SET
        recovery_in_progress = EXCLUDED.recovery_in_progress,
        system_identifier = CASE
            WHEN EXCLUDED.recovery_in_progress = false THEN EXCLUDED.system_identifier
            ELSE outbox.partition_guard.system_identifier
        END,
        contiguous_delivered_lsn = CASE
            WHEN EXCLUDED.recovery_in_progress = false AND EXCLUDED.contiguous_delivered_lsn IS NOT NULL
                THEN EXCLUDED.contiguous_delivered_lsn
            ELSE outbox.partition_guard.contiguous_delivered_lsn
        END,
        last_delivered_created_at_utc = CASE
            WHEN EXCLUDED.last_delivered_created_at_utc IS NOT NULL THEN EXCLUDED.last_delivered_created_at_utc
            ELSE outbox.partition_guard.last_delivered_created_at_utc
        END,
        updated_at_utc = now();
$$;

CREATE OR REPLACE FUNCTION outbox.drop_delivered_partitions(
    p_schema            text,
    p_parent            text,
    p_slot_name         text,
    p_safe_lsn          pg_lsn,
    p_drop_before_utc   timestamptz,
    p_system_identifier bigint
)
RETURNS TABLE(dropped_partition text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
DECLARE
    v_schema        text := lower(btrim(p_schema));
    v_parent        text := lower(btrim(p_parent));
    v_guard_sys_id  bigint;
    v_guard_lsn     pg_lsn;
    v_in_recovery   boolean;
    v_effective_lsn pg_lsn;
    v_part          record;
BEGIN
    IF v_schema !~ '^[a-z_][a-z0-9_]{0,62}$' OR v_parent !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
        RAISE EXCEPTION 'Недопустимый идентификатор схемы или таблицы.';
    END IF;

    SELECT g.system_identifier, g.contiguous_delivered_lsn, g.recovery_in_progress
    INTO v_guard_sys_id, v_guard_lsn, v_in_recovery
    FROM outbox.partition_guard g
    WHERE g.slot_name = p_slot_name;

    IF NOT FOUND OR v_in_recovery IS TRUE OR v_guard_sys_id <> p_system_identifier OR v_guard_lsn IS NULL THEN
        RETURN;
    END IF;

    v_effective_lsn := CASE
        WHEN p_safe_lsn IS NOT NULL AND p_safe_lsn < v_guard_lsn THEN p_safe_lsn
        ELSE v_guard_lsn
    END;

    -- Ограничиваем время ожидания блокировки, чтобы DROP старой партиции никогда не задерживал живые INSERT
    SET LOCAL lock_timeout = '2s';

    FOR v_part IN
        SELECT r.partition_name
        FROM outbox.partition_registry r
        WHERE r.schema_name = v_schema
          AND r.parent_table = v_parent
          AND r.dropped_at_utc IS NULL
          AND r.sealed_lsn IS NOT NULL
          AND r.sealed_lsn <= v_effective_lsn
          AND r.range_end_utc <= p_drop_before_utc
        ORDER BY r.range_start_utc
    LOOP
        IF v_part.partition_name !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
            CONTINUE;
        END IF;

        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema, v_part.partition_name);

            UPDATE outbox.partition_registry
            SET dropped_at_utc = now()
            WHERE schema_name = v_schema
              AND parent_table = v_parent
              AND partition_name = v_part.partition_name;

            dropped_partition := v_part.partition_name;
            RETURN NEXT;
        EXCEPTION
            WHEN lock_not_available THEN
                -- конкурентная блокировка: пропустим и безопасно доудалим на следующем тике
                CONTINUE;
        END;
    END LOOP;
END;
$$;

-- ---------- 4. Функции записи для Приложения 1 (единый переключатель INSERT ↔ WAL) ----------

CREATE OR REPLACE FUNCTION outbox.emit_frame(
    p_frame  bytea,
    p_prefix text DEFAULT 'outbox'
)
RETURNS pg_lsn
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
    SELECT pg_catalog.pg_logical_emit_message(true, p_prefix, p_frame);
$$;

CREATE OR REPLACE FUNCTION outbox.publish_message(
    p_topic          varchar(255),
    p_key            bytea,
    p_body           bytea,
    p_headers        varchar     DEFAULT '{}',
    p_partition      integer     DEFAULT NULL,
    p_producer_key   varchar(255) DEFAULT NULL,
    p_created_at     timestamptz DEFAULT now(),
    p_write_to_table boolean     DEFAULT false,
    p_prefix         text        DEFAULT 'outbox'
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, outbox
AS $$
DECLARE
    v_id             bigint;
    v_created_at     timestamptz := coalesce(p_created_at, now());
    v_created_us     bigint;
    v_flags          integer := 0;
    v_topic_bytes    bytea;
    v_headers_bytes  bytea;
    v_producer_bytes bytea;
    v_frame          bytea;
    v_seq            text;
BEGIN
    IF p_write_to_table THEN
        INSERT INTO outbox.messages_partitioned (
            topic, key, body, headers, partition, producer_key, created_at
        )
        VALUES (
            p_topic, p_key, p_body, coalesce(p_headers, '{}'), p_partition, p_producer_key, v_created_at
        )
        RETURNING id INTO v_id;

        RETURN v_id;
    END IF;

    v_seq := pg_catalog.pg_get_serial_sequence('outbox.messages_partitioned', 'id');
    v_id := nextval(v_seq::regclass);
    v_created_us := trunc(extract(epoch FROM v_created_at) * 1000000)::bigint;

    IF p_partition IS NOT NULL THEN
        v_flags := v_flags | 1;
    END IF;
    IF p_producer_key IS NOT NULL THEN
        v_flags := v_flags | 2;
    END IF;

    v_topic_bytes := convert_to(p_topic, 'UTF8');
    v_headers_bytes := convert_to(coalesce(p_headers, '{}'), 'UTF8');
    v_producer_bytes := CASE WHEN p_producer_key IS NOT NULL THEN convert_to(p_producer_key, 'UTF8') ELSE NULL END;

    -- Кадр OBX1 (big-endian), полностью совместимый с WalOutboxFrameCodec.Decode в C#
    v_frame := '\x4f425831'::bytea
        || int4send(1)
        || int8send(v_id)
        || int8send(v_created_us)
        || set_byte('\x00'::bytea, 0, v_flags)
        || CASE WHEN p_partition IS NOT NULL THEN int4send(p_partition) ELSE ''::bytea END
        || int4send(octet_length(v_topic_bytes)) || v_topic_bytes
        || int4send(octet_length(coalesce(p_key, ''::bytea))) || coalesce(p_key, ''::bytea)
        || int4send(octet_length(coalesce(p_body, ''::bytea))) || coalesce(p_body, ''::bytea)
        || int4send(octet_length(v_headers_bytes)) || v_headers_bytes
        || CASE WHEN v_producer_bytes IS NOT NULL THEN int4send(octet_length(v_producer_bytes)) || v_producer_bytes ELSE ''::bytea END;

    PERFORM pg_catalog.pg_logical_emit_message(true, p_prefix, v_frame);
    RETURN v_id;
END;
$$;

-- ---------- 5. Publications для новых режимов ----------

SELECT 'CREATE PUBLICATION outbox_partitioned_publication FOR TABLE outbox.messages_partitioned WITH (publish = ''insert'', publish_via_partition_root = true)'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_partitioned_publication') \gexec

ALTER PUBLICATION outbox_partitioned_publication SET (publish = 'insert', publish_via_partition_root = true);

SELECT 'CREATE PUBLICATION outbox_wal_publication WITH (publish = ''insert'')'
WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_wal_publication') \gexec

-- ---------- 6. Минимальные права ролей ----------

GRANT INSERT ON outbox.messages_partitioned TO sys_writer;
SELECT format('GRANT USAGE ON SEQUENCE %s TO sys_writer', pg_get_serial_sequence('outbox.messages_partitioned', 'id'))
WHERE pg_get_serial_sequence('outbox.messages_partitioned', 'id') IS NOT NULL \gexec

-- SELECT на секционированную таблицу для sys_replication нужен только для 24/7 авто-восстановления при смерти слота
GRANT SELECT ON outbox.messages_partitioned TO sys_replication;

REVOKE ALL ON FUNCTION outbox.ensure_partitions(text, text, timestamptz, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.seal_closed_partitions(text, text, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.read_partition_guard(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.ensure_partition_guard(text, bigint, pg_lsn, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.update_partition_guard_progress(text, bigint, pg_lsn, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.set_partition_recovery_lock(text, boolean, bigint, pg_lsn, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.drop_delivered_partitions(text, text, text, pg_lsn, timestamptz, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.emit_frame(bytea, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION outbox.publish_message(varchar, bytea, bytea, varchar, integer, varchar, timestamptz, boolean, text) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION outbox.ensure_partitions(text, text, timestamptz, integer) TO sys_replication;
GRANT EXECUTE ON FUNCTION outbox.seal_closed_partitions(text, text, timestamptz) TO sys_replication;
GRANT EXECUTE ON FUNCTION outbox.read_partition_guard(text) TO sys_replication;
GRANT EXECUTE ON FUNCTION outbox.ensure_partition_guard(text, bigint, pg_lsn, timestamptz) TO sys_replication;
GRANT EXECUTE ON FUNCTION outbox.update_partition_guard_progress(text, bigint, pg_lsn, timestamptz) TO sys_replication;
GRANT EXECUTE ON FUNCTION outbox.set_partition_recovery_lock(text, boolean, bigint, pg_lsn, timestamptz) TO sys_replication;
GRANT EXECUTE ON FUNCTION outbox.drop_delivered_partitions(text, text, text, pg_lsn, timestamptz, bigint) TO sys_replication;

GRANT EXECUTE ON FUNCTION outbox.emit_frame(bytea, text) TO sys_writer;
GRANT EXECUTE ON FUNCTION outbox.publish_message(varchar, bytea, bytea, varchar, integer, varchar, timestamptz, boolean, text) TO sys_writer;

-- ---------- 7. Первичная нарезка часовых партиций на 48 часов (2 дня) вперёд ----------

SELECT * FROM outbox.ensure_partitions('outbox', 'messages_partitioned', now(), 48);
