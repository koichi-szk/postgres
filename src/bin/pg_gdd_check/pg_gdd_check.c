#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "libpq-fe.h"

#define GDD_DEBUG_AID

typedef enum {
	MODE_CHECK,
	MODE_RECHECK
} MODE;

static int gdd_check(char *connstr);
static int gdd_recheck(int pos, char *conn_str);
static int gdd_check(char *connstr);
static int gdd_recheck(int pos, char *connstr);
static PGconn *connect(char *connstr);
static void oom_err(void);
static char *get_wfg(FILE *inf, int *wfg_len);


int
main(int argc, char *argv[])
{
	MODE			 my_mode;
	char			*conn_str;
	int				 pos;

	if (argc <= 2)
	{
		fprintf(stderr, "Invalid argument.\n");
		exit(1);
	}
	if (strcmp(argv[0], "c") == 0)
		my_mode = MODE_CHECK;
	else if (strcmp(argv[0], "r") == 0)
		my_mode = MODE_RECHECK;
	else
	{
		fprintf(stderr, "Invalid argument.\n");
		exit(1);
	}
	if (my_mode == MODE_CHECK)
	{
		conn_str = argv[1];
		exit(gdd_check(conn_str));
	}
	else
	{
		if (argc <= 3)
		{
			fprintf(stderr, "Invalid argument.\n");
			exit(1);
		}
		pos = atoi(argv[1]);
		conn_str = argv[2];
		exit(gdd_recheck(pos, conn_str));
	}
}

#ifdef GDD_DEBUG_AID
int gdd_debug_remote_pid;
#endif

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

	wfg = get_wfg(stdin, &wfg_len);
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
			printf("%d, Failed to obtain backend pid of the remote database.\n", -1);
			return 1;
		}
		else
		{
			pid_str = PQgetvalue(res, 0, 0);
			gdd_debug_remote_pid = atoi(pid_str);
		}
		if (res)
			PQclear(res);
	}
#endif
	res = PQexec(conn, query);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		printf("%d, Cannot run remote query to check global deadlock. %s\n",
				-1,
				res ? PQresultErrorMessage(res) : "");
		exit(1);
	}
	nTuples = PQntuples(res);

	for (ii = 0; ii < nTuples; ii++)
	{
		char		*state_s;
		char		*wfg;

		state_s = PQgetvalue(res, ii, 0);
		wfg = PQgetvalue(res, ii, 1);
		printf("%s, %s\n", state_s, wfg);
	}
	PQclear(res);
	PQfinish(conn);
	return 0;
#undef QUERY_LEN
}

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

	wfg = get_wfg(stdin, &wfg_len);
	query = malloc(QUERY_LEN + wfg_len);
	if (query == NULL)
		oom_err();
	snprintf(query, QUERY_LEN + wfg_len, "SELECT * FROM pg_global_deadlock_recheck_from_remote(%d, '%s');",
										 pos, wfg);
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
			printf("%d, Failed to obtain backend pid of the remote database.\n", -1);
			return 1;
		}
		else
		{
			pid_str = PQgetvalue(res, 0, 0);
			gdd_debug_remote_pid = atoi(pid_str);
		}
		if (res)
			PQclear(res);
	}
#endif
	res = PQexec(conn, query);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		printf("%d, Failed to run remote query to check global deadlock. %s\n",
				-1,
				res ? PQresultErrorMessage(res) : "");
		exit(1);
	}
	state_s = PQgetvalue(res, 0, 0);
	printf("%s\n", state_s);
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
		printf("%d, Failed to connect to remote database '%s' not reacheable.",
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
get_wfg(FILE *inf, int *wfg_len)
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
	len = 0;
	size = ALLOCSIZE;
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
	return wfg;
#undef ALLOCSIZE
}
