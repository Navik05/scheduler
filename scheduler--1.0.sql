/* contrib/scheduler/scheduler--1.0.sql */

\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

-- Схема для расширения
CREATE SCHEMA IF NOT EXISTS scheduler;

-- Таблица для хранения заданий
CREATE TABLE scheduler.jobs (
    job_id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    job_command TEXT NOT NULL, -- команда для выполнения
    schedule_interval INTERVAL NOT NULL, -- интервал выполнения
    last_run TIMESTAMP WITH TIME ZONE, -- время последнего выполнения
    next_run TIMESTAMP WITH TIME ZONE, -- время следующего выполнения
    status BOOLEAN DEFAULT TRUE -- статус задания
);

CREATE FUNCTION scheduler.run_jobs()
RETURNS VOID
AS '$libdir/scheduler', 'scheduler_run_jobs'
LANGUAGE C STRICT;