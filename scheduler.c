#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(scheduler_start_worker);
PGDLLEXPORT void scheduler_worker_main(Datum main_arg);

// Регистрация фонового процесса
Datum
scheduler_start_worker(PG_FUNCTION_ARGS)
{
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));

    snprintf(worker.bgw_name, BGW_MAXLEN, "scheduler_worker");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 0; 
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "scheduler_worker_main");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "scheduler");
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;

    if (!RegisterDynamicBackgroundWorker(&worker, NULL))
        elog(ERROR, "Failed to register scheduler worker");
    else
        elog(INFO, "Scheduler worker registered successfully");

    PG_RETURN_VOID();
}

// Основная функция фонового процесса
PGDLLEXPORT void 
scheduler_worker_main(Datum main_arg) {
    BackgroundWorkerUnblockSignals();
    
    while (!proc_exit_inprogress) {
        int ret;
        CHECK_FOR_INTERRUPTS();

        // Подключение SPI
        if (SPI_connect() != SPI_OK_CONNECT) {
            elog(ERROR, "SPI_connect failed");
            break;
        }

        const char *query = "SELECT job_id, job_command FROM scheduler.jobs "
                          "WHERE status AND next_run <= NOW() LIMIT 10";

        ret = SPI_execute(query, true, 0);  
        
        if (ret != SPI_OK_SELECT) {
            elog(WARNING, "Job fetch failed ");
            SPI_finish();
            pg_usleep(1000000L);
            continue;
        }

        // Обработка задачи
        for (uint64 i = 0; i < SPI_processed; i++) {
            bool isNull;
            Datum job_id = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isNull);
            
            if (isNull) continue;

            Datum job_command = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isNull);
            
            if (isNull) {
                elog(WARNING, "NULL command for job ");
                continue;
            }

            // Выполнение команды
            char *cmd = TextDatumGetCString(job_command);
            int cmd_ret = SPI_execute(cmd, false, 0);
            
            if (cmd_ret < 0) {
                elog(WARNING, "Job %d failed: %s (SPI: %d)", 
                     DatumGetInt32(job_id), cmd, cmd_ret);
            } else {
                // Обновление 
                char update[256];
                snprintf(update, sizeof(update),
                         "UPDATE scheduler.jobs SET "
                         "last_run = NOW(), "
                         "next_run = NOW() + schedule_interval "
                         "WHERE job_id = %d",
                         DatumGetInt32(job_id));
                
                if (SPI_execute(update, false, 0) != SPI_OK_UPDATE) {
                    elog(WARNING, "Failed to update job %d", DatumGetInt32(job_id));
                }
            }
            
            pfree(cmd);
        }

        SPI_finish();

        // Пауза и проверка прерываний
        for (int i = 0; i < 10 && !proc_exit_inprogress; i++) {
            pg_usleep(1000000L);
            CHECK_FOR_INTERRUPTS();
        }
    }
}