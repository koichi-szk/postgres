#ifndef GLOBAL_DEADLOCK_H
#define GLOBAL_DEADLOCK_H

/*
 * Interface functions
 *
 */

#include "postgres.h"

#include "storage/lock.h"

/*
 * The following represents discovered candidate of wait-for-graph segment.
 * This was used in several global deadlock detection functions.
 *
 * By checking Wfg stability locally at downstream database, number of global_wfg_in_text
 * is practically one.
 */
typedef struct RETURNED_WFG
{
    int               nReturnedWfg;
    DeadLockState    *state;                /* DS_HARD_DEADLOCK or DS_EXTERNL_LOCK */
    char            **global_wfg_in_text;   /* Text-format global wait-for-graph discovered */
} RETURNED_WFG;


typedef RETURNED_WFG	*(gdd_check_fn) (const char *conninfo, char *wfg);

extern PGDLLIMPORT gdd_check_fn	*pg_gdd_check_func;

#define gdd_check(conninfo, wfg)	pg_gdd_check_func(conninfo, wfg)

#endif /* GLOBAL_DEADLOCK_H */
