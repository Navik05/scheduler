#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "postmaster/bgworker.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(scheduler_run_jobs);

void _PG_init(void);
void _PG_fini(void);
void scheduler_worker_main(Datum main_arg);

void 
_PG_init(void)
{
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));

    snprintf(worker.bgw_name, BGW_MAXLEN, "scheduler_worker");
    worker.bgw_flags = BGWORKER_CLASS_PROCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;    // запрашивается возможность устанавливать подключение к базе данных
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;     // выполнить запуск, как только система перейдёт в обычный режим чтения-записи
    worker.bgw_restart_time = 10;   // перезапуск каждые 10 секунд, если процесс завершится
    sprintf(worker.bgw_function_name, "scheduler_worker_main");
    sprintf(worker.bgw_library_name, "scheduler");
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    
    RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
    // пусто)
}

void 
scheduler_worker_main(Datum main_arg)
{
    while (!proc_exit_inprogress)
    {
        int ret;

        // Подключение к SPI
        ret = SPI_connect();
        if (ret != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect failed");

        // Вызов функции выполнения заданий
        ret = SPI_execute("SELECT scheduler.run_jobs()", false, 0);
        if (ret != SPI_OK_SELECT)
            elog(WARNING, "Failed to run scheduler jobs");

        SPI_finish();

        // 10 секунд перед следующей проверкой
        pg_usleep(10000000L);
    }
}

Datum
scheduler_run_jobs(PG_FUNCTION_ARGS)
{
    int ret;
    char *query = "SELECT job_id, job_name, job_command, schedule_interval "
                  "FROM scheduler.jobs "
                  "WHERE status = true AND next_run <= NOW()";
    char update_query[512];

    // Инициализация SPI
    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Запрос для получения заданий
    ret = SPI_execute(query, false, 0);
    if (ret != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed");

    // Обработка результатов
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        char *job_id = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
        char *job_name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
        char *job_command = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
        char *schedule_interval = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);

        // Выполнение sql команду задания
        ret = SPI_execute(job_command, false, 0);
        if (ret != SPI_OK_UTILITY && ret != SPI_OK_SELECT)
            elog(WARNING, "Failed to execute job %s: %s", job_name, job_command);

        // Обновление next_run
        snprintf(update_query, sizeof(update_query),
                 "UPDATE scheduler.jobs SET last_run = NOW(), next_run = NOW() + INTERVAL '%s' WHERE job_id = %s",
                 schedule_interval, job_id);
        ret = SPI_execute(update_query, false, 0);
        if (ret != SPI_OK_UPDATE)
            elog(WARNING, "Failed to update job %s", job_name);
    }

    SPI_finish();
    PG_RETURN_VOID();
}