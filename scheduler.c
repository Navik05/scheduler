#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(scheduler_init);

Datum
scheduler_init(PG_FUNCTION_ARGS)
{
    elog(INFO, "scheduler initialized");
    PG_RETURN_VOID();
}