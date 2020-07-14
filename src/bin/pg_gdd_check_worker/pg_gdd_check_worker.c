/*-----------------------------------------------------------------------------------------------------
 * pg_gdd_check_worker.c
 *	pg_gdd_check_worker is a separate worker binary used in global deadlock detection.
 *
 * Portions Copyright (c) 2020, 2ndQUadrant Ltd.,
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *  pg_gdd_check_worker is invoked by global deadlock check functions in deadlock.c of
 *  postgres backend.   This became a separate worker because we cannot issue libpq-fe
 *  functions from the backend.
 *
 *  This runs in two mode: check mode a recheck mode.
 *
 *  In check mode sends global wait-for-graph found to current database, connect to the downstream
 *  database specified in the wait-for-graph, pass this wait-for-graph to continue tracking
 *  global wait-for-graph.   This is continued until global deadlock candidates are found.
 *  At the downstream, built-in SQL function pg_global_deadlock_check_from_remote() will
 *  initiate this check.  If there's another link to furnter downstream database through
 *  EXTERNAL LOCK (see lock.[ch]), such tracking will be continued until wait-for-grap
 *  terminates withouth cycle or global wait-for-graph cycke is found.
 *
 *  There can be more than one candidate of such global wait-for-graph cycle.  All the
 *  candidate will be returned to the original node which initiated the deadlock check.
 *
 *  We cannot determine such candidates are really deadlocks.  Here's a background.
 *  In local wait-for-graph tracking, we need to examine lock status across different
 *  backend processes.   we acquire all the low-level locks to make this safely.
 *  On the other hand, such low-level locks prevents other transactions to run and
 *  we release there locks when the examination continues at donwstream databases.
 *
 *  Therefore, global wait-for-graph cycle found in check phase may not be actual
 *  deadlock cycle.
 *
 *  We need recheck phase to confirm such candidates are really global wait-for-graph
 *  cycle.
 *
 *  In the recheck phase, we examine if local segment of such wait-for-graph is stable.
 *  If not, that is, if any status in recorded wait-for-graph changes, then this cannot
 *  be a part of wait-for-graph cycle.
 *
 *  Recheck phase does all this.   pg_gdd_check_worker connects to the downstream
 *  recorded in the global wait-for-graph and continues to examine if whoe status is
 *  stable.
 *
 *  This is done by calling pg_global_deadlock_recheck_from_remote() built-in SQL
 *  function.
 *
 * IDENTIFICATION
 *   sec/bin/pg_gdd_check_worker/pg_gdd_check_worker.c
 *-----------------------------------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "libpq-fe.h"

#define GDD_DEBUG_AID

/*
 * Run mode
 */
typedef enum {
	MODE_CHECK,		/* Check mode */
	MODE_RECHECK	/* Reckeck mode */
} MODE;

/* Internal Functions */
static int gdd_check(char *connstr);
static int gdd_recheck(int pos, char *conn_str);
static PGconn *connect(char *connstr);
static void oom_err(void);
static char *get_wfg(int *wfg_len);
static FILE *file_open(char *fname, char mode);

/*
 * INPUT/OUTPUT of the command
 *
 * File name will be supplied by the caller.   We use file to receive wait-for-graph
 * and the result.
 *
 * Then size of wait-for-graph may be large and may not fit to the maximum size of
 * command line parameters.
 * 
 */
FILE	*inf;
FILE	*outf;

/*
 * Two command syntac:
 *
 * Check mode:
 * 		pg_gdd_check_worker c dsn [infile [outfile] ]
 *
 * Recheck mode:
 *		pg_gdd_check_worker r pos dsn [infile [outfile] ]
 *
 * pos: position of the current database in global wait-for-graph
 * dsn: connection string to the next downstream database
 * infile: input file to read global wait-for-graph.   If omitted, stdin.
 * outfile: output file of the command.  If omitted, stdout.
 *
 * In check mode, output file consists of candidate global wait-for-graph in the
 * enxt format:
 *
 * status, global-wait-for-graph
 *		status: value of DeadLockState (lock.h)
 *		wait-for-graph: wait-for-graph
 *
 * In recheck mode, output file consists of one line of the text.  It's value
 * is from DeadLockState.
 */
int
main(int argc, char *argv[])
{
	MODE			 my_mode;
	char			*conn_str;
	int				 pos;

	inf = stdin;
	outf = stdout;
	if (argc <= 2)
	{
		fprintf(stderr, "Invalid argument.\n");
		exit(1);
	}
	if (strcmp(argv[1], "c") == 0)
		my_mode = MODE_CHECK;
	else if (strcmp(argv[1], "r") == 0)
		my_mode = MODE_RECHECK;
	else
	{
		fprintf(stderr, "Invalid argument.\n");
		exit(1);
	}
	if (my_mode == MODE_CHECK)
	{
		conn_str = argv[2];
		if (argc >= 4)
			inf = file_open(argv[3], 'r');
		if (argc >= 5)
			outf = file_open(argv[4], 'w');
		exit(gdd_check(conn_str));
	}
	else
	{
		if (argc <= 3)
		{
			fprintf(stderr, "Invalid argument.\n");
			exit(1);
		}
		pos = atoi(argv[2]);
		conn_str = argv[3];
		if (argc >= 5)
			inf = file_open(argv[4], 'r');
		if (argc >= 6)
			outf = file_open(argv[5], 'w');
		exit(gdd_recheck(pos, conn_str));
	}
}

#ifdef GDD_DEBUG_AID
int gdd_debug_remote_pid;
#endif

static FILE *
file_open(char *fname, char mode)
{
	if (mode != 'r' && mode != 'w')
		return NULL;
	if (strcmp(fname, "-") == 0)
	{
		if (mode == 'r')
			return stdin;
		else
			return stdout;
	}
	if (mode == 'r')
		return fopen(fname, "r");
	else
		return fopen(fname, "w");
}

/*
 * Body of check mode
 */
static int
gdd_check(char *connstr)
{
#define	QUERY_LEN	1024
	PGconn		*conn;
	PGresult	*res;
	char		*wfg;
	int			 wfg_len;
	char		*query;
	int			 nTuples;
	int			 ii;

	wfg = get_wfg(&wfg_len);
	if (wfg == NULL)
		return 1;
	query = malloc(QUERY_LEN + wfg_len);
	if (query == NULL)
		oom_err();
	snprintf(query, QUERY_LEN + wfg_len, "SELECT * FROM pg_global_deadlock_check_from_remote('%s');", wfg);
	free(wfg);
	conn = connect(connstr);
	if (conn == NULL)
	{
		printf("-1");
		return 1;
	}
#ifdef GDD_DEBUG_AID
	/* For debug, obtain remote checker PID */
	{
		char	*pid_query = "SELECT pg_backend_pid() as pid;";
		char	*pid_str;

		res = PQexec(conn, pid_query);
		if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(outf, "1\n%d, Failed to obtain backend pid of the remote database.\n", -1);
			return 1;
		}
		else
		{
			pid_str = PQgetvalue(res, 0, 0);
			gdd_debug_remote_pid = atoi(pid_str);
			fprintf(stderr, "Downstream database backend pid: %d\n", gdd_debug_remote_pid);
		}
		if (res)
			PQclear(res);
	}
#endif
	res = PQexec(conn, query);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(outf, "1\n%d, Cannot run remote query to check global deadlock. %s\n",
					  -1,
					  res ? PQresultErrorMessage(res) : "");
		exit(1);
	}
	nTuples = PQntuples(res);

	fprintf(outf, "%d\n", nTuples);
	for (ii = 0; ii < nTuples; ii++)
	{
		char		*state_s;
		char		*wfg;

		state_s = PQgetvalue(res, ii, 0);
		wfg = PQgetvalue(res, ii, 1);
		fprintf(outf, "%s, %s\n", state_s, wfg);
	}
	PQclear(res);
	PQfinish(conn);
	return 0;
#undef QUERY_LEN
}

/*
 * Body of recheck mode
 */
static int
gdd_recheck(int pos, char *connstr)
{
#define	QUERY_LEN	1024
	PGconn		*conn;
	PGresult	*res;
	char		*wfg;
	int			 wfg_len;
	char		*query;
	char		*state_s;

	wfg = get_wfg(&wfg_len);
	if (wfg == 0)
		return 1;
	query = malloc(QUERY_LEN + wfg_len);
	if (query == NULL)
		oom_err();
	snprintf(query, QUERY_LEN + wfg_len, "SELECT * FROM pg_global_deadlock_recheck_from_remote(%d, '%s');",
										 pos, wfg);
	free(wfg);
	conn = connect(connstr);
	if (conn == NULL)
	{
		return 1;
	}
#ifdef GDD_DEBUG_AID
	/* For debug, obtain remote checker PID */
	{
		char	*pid_query = "SELECT pg_backend_pid() as pid;";
		char	*pid_str;

		res = PQexec(conn, pid_query);
		if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(outf, "1\n%d, Failed to obtain backend pid of the remote database.\n", -1);
			return 1;
		}
		else
		{
			pid_str = PQgetvalue(res, 0, 0);
			gdd_debug_remote_pid = atoi(pid_str);
			fprintf(stderr, "Downstream database backend pid: %d\n", gdd_debug_remote_pid);
		}
		if (res)
			PQclear(res);
	}
#endif
	res = PQexec(conn, query);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(outf, "1\n%d, Failed to run remote query to check global deadlock. %s\n",
					  -1,
					  res ? PQresultErrorMessage(res) : "");
		exit(1);
	}
	state_s = PQgetvalue(res, 0, 0);
	fprintf(outf, "%s\n", state_s);
	PQclear(res);
	PQfinish(conn);
	return 0;
#undef QUERY_LEN
}


static PGconn *
connect(char *connstr)
{
	PGconn *conn;

	conn = PQconnectdb(connstr);
	if (conn == NULL || PQstatus(conn) == CONNECTION_BAD)
	{
		printf("1\n%d, Failed to connect to remote database '%s' not reacheable.",
				-1,
				connstr);
		return NULL;
	}
	return conn;
}

static void
oom_err(void)
{
	fprintf(stderr, "Could not allocate memory.\n");
	exit(1);
}

static char *
get_wfg(int *wfg_len)
{
#define	ALLOCSIZE (1024)
	int		 len;
	int		 size;
	char	*wfg;
	char	*curr;
	char	 cc;

	wfg = curr = (char *)malloc(ALLOCSIZE);
	if (wfg == NULL)
		oom_err();
	size = ALLOCSIZE;
	len = 0;
	*wfg = '\0';
	while ((cc = fgetc(inf)) >= 0)
	{
		if (len >= (size - 1))
		{
			wfg = (char *)realloc(wfg, size + ALLOCSIZE);
			if (wfg == NULL)
				oom_err();
			size += ALLOCSIZE;
			curr = wfg + len;
		}
		*curr++ = cc;
		len++;
	}
	*curr = '\0';
	*wfg_len = len + 1;	/* include terminating NULL */
	fclose(inf);
	return wfg;
#undef ALLOCSIZE
}
