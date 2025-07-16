#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(scheduler_run_jobs);

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