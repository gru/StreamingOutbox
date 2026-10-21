# writer.md — Writer: особенности реализации

Генератор тестовой нагрузки: INSERT-батчи в `outbox.messages` от `sys_writer`, управляется HTTP API (`/api/generation/*`). Стек и пины — `docs/dependencies.md` §2; контракт тестового body/key — `docs/outbox-table.md`; SQL записи и гранты — `docs/dependencies.md` §3 («Запись тестовой нагрузки»). Здесь собраны особенности реализации, вынесенные из комментариев кода.

## Запись батча

- Один батч = одна транзакция: multi-row INSERT → DELETE тех же строк → commit. Таблица остаётся **пустой**: строка живёт только внутри транзакции, сообщение доставляется через WAL (`publish='insert'` — DELETE из эмиссии pgoutput исключён вовсе). Таблица не растёт, DELETE по PK только что вставленных идентификаторов дёшев.
- **id — bigint identity, но вставляется явно**: id должен попасть в body и key **до** вставки, поэтому исполнитель батча сначала резервирует id у identity-последовательности (`SELECT nextval($1::regclass) FROM generate_series(1, $2)`), собирает сообщения и вставляет их с `OVERRIDING SYSTEM VALUE` (обход `GENERATED ALWAYS`).
- Имя последовательности вычисляется через `pg_get_serial_sequence('outbox.messages','id')` и кэшируется (оба NpgsqlDataSource смотрят в одну БД). NULL от функции → явная ошибка «таблица/identity-колонка не найдены».
- `nextval` нетранзакционен: «потерянные» при отмене значения дают лишь пропуски в последовательности — это безопасно.
- 6 позиционных параметров на строку (id, topic, key, body, headers, created_at) — позиционные (без имён); `MaxBatchSize = 5000` (6×5000 < лимита протокола 65535). Текст INSERT кэшируется по числу строк.
- `created_at` задаёт клиент (UTC DateTimeOffset); `DEFAULT now()` остаётся для чужих вставок.

## Ретраи и неоднозначный commit

- Транзиентные сбои ретраятся **теми же id** с exponential backoff: база 250 мс, удвоение, потолок 5 с, jitter ±30%. Транзиентными считаются: классы PG 08xxx (connection_exception), 53300 (too_many_connections), 57P01/57P02/57P03 (шатдауны, cannot_connect_now — Patroni failover/switchover), 40001/40P01 (сериализация/дедлок), 55P03 (lock_not_available), 54000 (program limit), а также Npgsql с внутренними SocketException/IOException/TimeoutException. Ошибки прав/схемы/ограничений фатальны.
- **Разрешение неоднозначного commit**: `unique_violation` (23505) по PK на повторной попытке доказывает, что прошлая попытка закоммитилась → `BatchOutcome.AlreadyCommitted` (сообщения считаются written, дубликатов в outbox не возникает — противоречие невозможно).
- Семантика отмены: `hardCt` (остановка хоста) прерывает попытки немедленно; `stopCt` (мягкий stop) прерывает только ожидание между ретраями — начавшийся commit всегда доигрывается. Отмена **до вставки** (при резервировании id) учитывается как ambiguous: вставки не было, сообщений не существует; консервативная верхняя оценка, вердикт валидатора это не фейлит.
- У `AlreadyCommitted` реальная длительность commit прошлой попытки неизвестна — в выборку commit-латентности не попадает, только в batch-длительность.
- Резерв id выполняется в **исполнителе** батча, а не в диспетчере: диспетчер остаётся без I/O, каденс генерации не зависит от латентности БД.

## Диспетчер и исполнители

- Старт в Idle; генерация только после `POST /api/generation/start`; после рестарта процесса — снова Idle (состояние in-memory, авто-возобновления нет).
- Rate-каденс: один батч каждые `batchSize/rate` секунд (первый батч без задержки); без rate — закрытый контур: новый батч создаётся, как только освобождается слот parallelism (`SemaphoreSlim`). Число одновременно выполняемых батчей ≤ parallelism (1..64).
- Пока исполнитель ждёт слот семафора, условия могли измениться — диспетчер перепроверяет цели/stop после ожидания.
- Объём диспетчеризуется по оценке (`EstimateBodyBytes` × size, ширина id — 12 цифр), фактические байты считает исполнитель.
- Прогон завершается по первой достигнутой цели (totalMessages / totalPayloadBytes / durationSeconds) или по stop; `runUntilStopped=true` — до явного stop. `ExecuteBatchAsync` не бросает исключений (страховочный catch + release семафора в finally).

## Подключения

- Один `NpgsqlDataSource`: напрямую через HAProxy (5432). Режимов подключения нет — PgBouncer в стенде отсутствует (решение владельца: polling-режим не тестируется).
- Пул фиксированно широк (72; parallelism ≤ 64 + запас), соединения открываются лениво.
- AutoPrepare выключен.
- `/ready` выполняет `SELECT 1` (3 с бюджет): не требует привилегий на таблицы, годится для sys_writer.

## Формат тестового сообщения

- body — UTF-8 JSON `{"id","runId","eventType","payload","timestamp"}`; timestamp — UTC с миллисекундами (`yyyy-MM-ddTHH:mm:ss.fffZ`), поэтому у идентичных сообщений id/runId/payload точный размер body воспроизводим оценкой.
- payload — псевдослучайный alphanumeric: 1 символ ASCII = 1 байт UTF-8, размер контролируется точно; неконстантное содержимое не даёт TOAST-компрессии занижать фактический объём body/WAL.
- key = UTF-8 десятичного id; headers = `"{}"`; partition/producer_key/outbox_partition не пишутся (NULL/DEFAULT).

## Метрики

- prometheus-net (`Metrics.Create*`, `app.MapMetrics()`); имена: `writer_messages_written_total`, `writer_bytes_written_total`, `writer_commit_duration_milliseconds` (histogram), `writer_batch_duration_milliseconds` (histogram), `writer_write_errors_total{reason=fatal|ambiguous}`, `writer_ambiguous_commits_total`, `writer_reconnects_total`, gauge `writer_generation_active`.
- Кардинальность фиксированная: message id не используется как label.
- Грабли prometheus-net 5.0: внутри namespace `Writer.Metrics` имя `Metrics` резолвится в namespace — класс квалифицируется как `Prometheus.Metrics`; метода `UseMetrics` в 5.0 нет — достаточно `MapMetrics`; инкремент с label — `WithLabels(...).Inc()` (перегрузки `Inc(value, labels)` нет). Счётчик без единой записи не экспортируется вовсе.

## Устройство файлов и видимость

- По одному типу на файл; исключения (решение владельца pending): `MessageFactory.cs` (record-продукт + фабрика), `OutboxWriter.cs` (enum + record контракта `WriteBatchAsync` + класс), `Contracts/RunSpec.cs` (HTTP DTO запроса + `LoadProfile.Resolve` — перенесён в Contracts целиком). `RunContext` — вложенный private-класс состояния прогона в `GenerationEngine`.
- **Публичный HTTP-контракт — в `src/Writer/Contracts/`**: тела ответов-записей (`ErrorResponse`, `ErrorsResponse`, `StartAcceptedResponse`, `StateResponse`, `HealthResponse`, `ReadyResponse`) и HTTP-DTO (`RunSpec`+`LoadProfile`, `RunResults`, `RunStateSnapshot`, `RunIdsDto`); JSON-форма та же, что у прежних анонимных тел (camelCase web-дефолтами). `LoadProfile` — двойного назначения: сериализуется в телах 202/state/results и служит входом движка. В `Generation` остались только внутренние типы конвейера (`GeneratedMessage`, `BatchResult`, `BatchOutcome`) и логика.
- **Типы Writer — `internal`, кроме public-контрактов, которые вынужденно публичны** (решение владельца): контроллеры (`Controllers/GenerationController.cs`, `Controllers/HealthController.cs` — MVC обнаруживает только public-классы) и их зависимости в сигнатурах: `GenerationEngine`, `OutboxWriter` (+ `BatchOutcome`/`BatchResult`/`GeneratedMessage`), `WriterOptions` со всей конфигурацией, `RunSpec`/`LoadProfile`, `RunResults`, `RunStateSnapshot`, `RunIdsDto`, `WriterMetrics` (компилятор требует: public-член не может принимать/возвращать internal-тип — CS0051/CS0050). Все public-типы и члены документированы XML-комментариями; остальное (MessageFactory, Percentiles, RunTargets) — internal. HTTP-контракт при переходе с minimal API на контроллеры не менялся. Тестовые сборки (`Writer.UnitTests`, `IntegrationTests`) видят internal-типы через `InternalsVisibleTo` (Writer.csproj); члены внутри internal-типов остаются public — это поверхность для тестов.
