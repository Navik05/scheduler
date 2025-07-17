/* contrib/scheduler/scheduler--1.0.sql */

\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

-- Схема для расширения
CREATE SCHEMA IF NOT EXISTS scheduler;

-- Таблица для хранения заданий
CREATE TABLE IF NOT EXISTS  scheduler.jobs (
    job_id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    job_command TEXT NOT NULL, -- команда для выполнения
    schedule_interval INTERVAL NOT NULL, -- интервал выполнения
    last_run TIMESTAMPTZ, -- время последнего выполнения
    next_run TIMESTAMPTZ, -- время следующего выполнения
    status BOOLEAN NOT NULL DEFAULT TRUE -- статус задания
);

CREATE FUNCTION scheduler.start_worker()
RETURNS VOID
AS '$libdir/scheduler', 'scheduler_start_worker'
LANGUAGE C STRICT;

-- Функция для запуска воркера
CREATE OR REPLACE FUNCTION scheduler._on_extension_create()
RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Именно scheduler
    IF EXISTS (
        SELECT 1 
        FROM pg_event_trigger_ddl_commands() 
        WHERE object_identity = 'scheduler'
    ) THEN
        PERFORM scheduler.start_worker();
        RAISE NOTICE 'Scheduler worker started';
    END IF;
END;
$$;

-- Триггер на создания расширения
CREATE EVENT TRIGGER scheduler_worker_trigger
ON ddl_command_end
WHEN TAG IN ('CREATE EXTENSION')
EXECUTE FUNCTION scheduler._on_extension_create();

-- Функция для добавления задания
CREATE FUNCTION scheduler.add_job(
    p_job_name TEXT,
    p_job_command TEXT,
    p_schedule_interval INTERVAL
)
RETURNS BIGINT
AS $$
    INSERT INTO scheduler.jobs (job_name, job_command, schedule_interval, next_run)
    VALUES (p_job_name, p_job_command, p_schedule_interval, NOW())
    RETURNING job_id;
$$ LANGUAGE SQL;

-- Функция для удаления задания
CREATE FUNCTION scheduler.delete_job(p_job_id BIGINT)
RETURNS VOID
AS $$
    DELETE FROM scheduler.jobs WHERE job_id = p_job_id;
$$ LANGUAGE SQL;

-- Функция для включения/выключения задания
CREATE FUNCTION scheduler.set_job_status(p_job_id BIGINT, p_status BOOLEAN)
RETURNS VOID
AS $$
    UPDATE scheduler.jobs SET status = p_status WHERE job_id = p_job_id;
$$ LANGUAGE SQL;