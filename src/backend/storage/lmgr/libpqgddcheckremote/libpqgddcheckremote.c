/*-----------------------------------------------------------------------------------------------------
 *
 * libpqgddcheckremote.c

 *	libapqgddcheckremote.c is an interface function to use libpq from the backend.
 *  This is using similar strategy as in libpqwalreceiver.c
 *
 * Portions Copyright (c) 2020-2021, 2ndQUadrant Ltd.,
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   sec/backend/storage/lmgr/libpqgddcheckremote/libpqgddcheckremote.c
 *-----------------------------------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/time.h>

#include "libpq-fe.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/global_deadlock.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

void	_PG_init(void);

static RETURNED_WFG	*pg_gdd_check_remote(const char *connstr, char *wfg);
static PGconn		*gdd_check_connect(const char *connstr);

#define	LIBGDDCHECKREMOTE_DEBUG

/*
 * Caller 
 */
void
_PG_init(void)
{
	if (pg_gdd_check_func != NULL)
		elog(ERROR, "libpqgddcheckremote already loaded");
	pg_gdd_check_func = pg_gdd_check_remote;
}

/*
 * Check global deadlock status of downstream database.
 *
 * Arguments:
 *	connstr: connection string for libpq to the downstream database,
 *  wfg: global wait-for-graph discovered so far.
 *
 * Return value:
 *	struct RETURNED_WFG. See global_deadlock.h for details.
 */
static RETURNED_WFG *
pg_gdd_check_remote(const char *connstr, char *wfg)
{
	PGconn		*conn;
	PGresult	*res;
	StringInfoData	query;
	StringInfoData	rv;
	RETURNED_WFG	*returning_wfg;
	int			 nTuples;
	int			 ii;
#ifdef	LIBGDDCHECKREMOTE_DEBUG
	pid_t		 remote_pid;
#endif

	/* Connect to downstream database */
	conn = gdd_check_connect(connstr);

	/*
	 * Get downstream database session pid for further debug
	 *
	 * This pid can be used to invoke gdb on global deadlock check backend at downstream database.
	 */
#ifdef	LIBGDDCHECKREMOTE_DEBUG
	res = PQexec(conn, "SELECT pg_backend_pid();");
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "Cannot get remote session pid. %s\n",
					res ? PQresultErrorMessage(res) : "");
	remote_pid = atoi(PQgetvalue(res, 0, 0));
	elog(LOG, "Checking downstream WfG, connstr: '%s', remote pid: %d.", connstr, remote_pid);
	PQclear(res);
#endif

	/* Check WfG for downstream database */
	initStringInfo(&query);
	initStringInfo(&rv);
	appendStringInfo(&query, "SELECT * FROM pg_global_deadlock_check_from_remote($GDD$%s$GDD$);", wfg);
	res = PQexec(conn, query.data);
	pfree(query.data);
	query.data = NULL;
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "Cannot run remote query to check global deadlock. %s\n",
					res ? PQresultErrorMessage(res) : "");
	nTuples = PQntuples(res);

	returning_wfg = (RETURNED_WFG *)palloc(sizeof(RETURNED_WFG));
	returning_wfg->nReturnedWfg = nTuples;
	returning_wfg->state = (DeadLockState *)palloc(sizeof(DeadLockState) * nTuples);
	returning_wfg->global_wfg_in_text = (char **)palloc(sizeof(char *) * nTuples);

	for (ii = 0; ii < nTuples; ii++)
	{
		char		*state_s;

		state_s = PQgetvalue(res, ii, 0);
		returning_wfg->state[ii] = atoi(state_s);
		returning_wfg->global_wfg_in_text[ii] = pstrdup(PQgetvalue(res, ii, 1));
	}
	PQclear(res);
	PQfinish(conn);
	return returning_wfg;
}

static PGconn *
gdd_check_connect(const char *connstr)
{
	PGconn *conn;

	conn = PQconnectdb(connstr);
	if (conn == NULL || PQstatus(conn) == CONNECTION_BAD)
	{
		elog(ERROR, "Failed to connect to remote database '%s' not reacheable.",
				connstr);
		return NULL;
	}
	return conn;
}
