/*-------------------------------------------------------------------------
 *
 * gdd_test_deadlock.c
 *
 *	  POSTGRES global deadlock detection test functions
 *
 * See src/backend/storage/lmgr/README for a description of the deadlock
 * detection and resolution algorithms.
 *
 * Portions Copyright (c) 2020, 2ndQuadrant Ltd.,
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/gdd_test_external_lock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type_d.h"
#include "common/controldata_utils.h"
#include "funcapi.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/memutils.h"

/*
 * K.Suzuki:
 *	名前をつけて locktag をしまっておくようにする
 */
typedef struct LOCKTAG_DICT
{
	struct	LOCKTAG_DICT	*next;
	char	*label;
	LOCKTAG	 tag;
} LOCKTAG_DICT;

static LOCKTAG_DICT	*locktag_dict_head = NULL;
static LOCKTAG_DICT	*locktag_dict_tail = NULL;
static char		*outfilename = NULL;
static FILE		*outf = NULL;
static PGPROC	*pgproc;
static int64	 database_system_id;

static Datum gdd_external_lock_acquire_int(PGPROC *proc);
static Datum gdd_test_set_locktag_external_int(char *label, PGPROC *target_proc, bool increment);
static Datum gdd_test_show_myself_int(PGPROC *proc);
static PGPROC *find_pgproc(int pid);
static bool register_locktag_label(char *label, LOCKTAG *locktag);
static bool unregister_locktag_label(char *label);
static const char *lockAcquireResultName(LockAcquireResult res);
static const char *lockModeName(LOCKMODE lockmode);
static const char *waitStatusName(int status);
static const char *waitStatusName(int status);
static void hold_all_lockline(void);
static void release_all_lockline(void);
static bool add_locktag_dict(LOCKTAG_DICT *locktag_dict);
static void free_locktag_dict(LOCKTAG_DICT *locktag_dict);
static bool remove_locktag_dict(LOCKTAG_DICT *locktag_dict);
static LOCKTAG_DICT *find_locktag_dict(char *label);

/*
 * CREATE FUNCTION gdd_external_lock_test_begin(outfname text)
 *		VOLATILE
 *		RETURNS int
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_test_init_test';
 */
PG_FUNCTION_INFO_V1(gdd_test_init_test);

Datum
gdd_test_init_test(PG_FUNCTION_ARGS)
{
	database_system_id = get_database_system_id();
	outfilename = PG_GETARG_CSTRING(0);
	outf = fopen(outfilename, "w");
	if (outf == NULL)
	{
		elog(INFO, "Could not open the output file.");
		fprintf(outf, "Could not open the output file.");
	}
	fprintf(outf, "%s: beginning global deadlock detection test.\n", __func__);
	PG_RETURN_INT32(0);
}



/*
 * CREATE FUNCTION gdd_external_lock_test_finish()
 *		VOLATILE
 *		RETURNS int
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_test_finish_test';
 */
PG_FUNCTION_INFO_V1(gdd_test_finish_test);

Datum
gdd_test_finish_test(PG_FUNCTION_ARGS)
{
	fprintf(outf, "%s: finishing global deadlock detection test.\n", __func__);
	fclose(outf);
	PG_RETURN_INT32(0);
}


/*
 * CREATE FUNCTION gdd_show_myself()
 *		IMMUTABLE
 *		RETURNS TABLE(system_id bigint, pid int, pgprocno int, lxn int)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_test_show_myself';
 */

PG_FUNCTION_INFO_V1(gdd_test_show_myself);

Datum
gdd_test_show_myself(PG_FUNCTION_ARGS)
{
	Datum	result;

	result = gdd_test_show_myself_int(MyProc);
	PG_RETURN_DATUM(result);
}


/*
 * CREATE FUNCTION gdd_show_proc(pid int)
 *		IMMUTABLE
 *		RETURNS TABLE(system_id bigint, pid int, pgprocno int, lxn int)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_test_show_proc';
 */

PG_FUNCTION_INFO_V1(gdd_test_show_proc);

Datum
gdd_test_show_proc(PG_FUNCTION_ARGS)
{
	Datum	result;
	PGPROC *pgproc;

	pgproc = find_pgproc(PG_GETARG_INT32(0));
	if (pgproc == NULL)
		PG_RETURN_NULL();
	result = gdd_test_show_myself_int(pgproc);
	PG_RETURN_DATUM(result);
}


static Datum
gdd_test_show_myself_int(PGPROC *proc)
{
#define CHARLEN 64
	TupleDesc        tupd;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;
	char             values[4][CHARLEN];
	char			*Values[4];
	Datum            result;
	int64			 database_system_id;
	int				 ii;

	for(ii = 0; ii < 4; ii++)
		Values[ii] = &values[ii][0];
	fprintf(outf, "%s(): PGPROC: %016lx, pid: %d, pgprocno: %d, lxid: %d\n", __func__,
				(unsigned long)proc, proc->pid, proc->pgprocno, proc->lxid);
	fflush(outf);
	database_system_id = get_database_system_id();
	ii = 1; 
	tupd = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupd, ii++, "system_id", INT8OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "pgprocno", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "lxid", INT4OID, -1, 0);
	ii = 0;
	snprintf(values[ii++], CHARLEN, "%016lx", database_system_id);
	snprintf(values[ii++], CHARLEN, "%d", proc->pid);
	snprintf(values[ii++], CHARLEN, "%d", proc->pgprocno);
	snprintf(values[ii++], CHARLEN, "%d", proc->lxid);
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), Values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	return result;
#undef CHARLEN
}

/*
 * CREATE FUNCTION gdd_set_locktag_external_pgprocno(label text, pgprocno int, increment bool)
 *		VOLATILE
 *		RETURNS TABLE (label text, field1 int, field2 int, field3 int, field4 int, type text, lockmethod int)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_test_set_locktag_external_pgprocno';
 * 
 * K.Suzuki: 引数は pgprocno? show self でできればいいか？定義を変更すること
 *			 注意：このロックはロックタグを作成しただけで、まだロックは取得していないことに注意。
 */

PG_FUNCTION_INFO_V1(gdd_test_set_locktag_external_pgprocno);

Datum
gdd_test_set_locktag_external_pgprocno(PG_FUNCTION_ARGS)
{
	int		 pgprocno;
	char	*label;
	bool	 increment;
	PGPROC	*proc;
	Datum	 result;

	label = PG_GETARG_CSTRING(0);
	pgprocno = PG_GETARG_INT32(1);
	increment = PG_GETARG_BOOL(2);

	if (pgprocno >= ProcGlobal->allProcCount)
		elog(ERROR, "Given parameter value %d exceeds number of PGPROC.", pgprocno);
	if (pgprocno < 0)
		proc = MyProc;
	else
		proc = &ProcGlobal->allProcs[pgprocno];
	result = gdd_test_set_locktag_external_int(label, proc, increment);
	PG_RETURN_DATUM(result);
}

/*
 * CREATE FUNCTION gdd_set_locktag_external_pid(label text, pid int, increment bool)
 *		VOLATILE
 *		RETURNS TABLE (label text, field1 int, field2 int, field3 int, field4 int, type text, lockmethod int)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_test_set_locktag_external_pid';
 * 
 * K.Suzuki: 引数は pgprocno? show self でできればいいか？定義を変更すること
 *			 注意：このロックはロックタグを作成しただけで、まだロックは取得していないことに注意。
 */
PG_FUNCTION_INFO_V1(gdd_test_set_locktag_external_pid);

Datum
gdd_test_set_locktag_external_pid(PG_FUNCTION_ARGS)
{
	int		 pid;
	char	*label;
	bool	 increment;
	PGPROC	*proc;
	Datum	 result;

	label = PG_GETARG_CSTRING(0);
	pid = PG_GETARG_INT32(1);
	increment = PG_GETARG_BOOL(2);

	proc = find_pgproc(pid);
	if (proc == NULL)
		elog (ERROR, "No PGPROC found for pid = %d.", pid);
	result = gdd_test_set_locktag_external_int(label, proc, increment);
	PG_RETURN_DATUM(result);
}

static Datum
gdd_test_set_locktag_external_int(char *label, PGPROC *target_proc, bool increment)
{
#define CHARLEN 64
	LOCKTAG	 locktag;
	/* outoput */
	TupleDesc        tupd;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;
	char             values[6][CHARLEN];
	char            *Values[6];
	Datum            result;
	int				 ii;


	set_locktag_external(&locktag, target_proc, increment);
	register_locktag_label(label, &locktag);

	fprintf(outf, "%s(): : field1:%d, field2: %d, field3: %d, field4: %d, type: %s, lockemthod: %d\n",
			    "set_locktag_external",
				locktag.locktag_field1, locktag.locktag_field2, locktag.locktag_field3, locktag.locktag_field4,
				locktagTypeName(locktag.locktag_type), locktag.locktag_lockmethodid);
	fflush(outf);
	tupd = CreateTemplateTupleDesc(6);
	ii = 1;
	TupleDescInitEntry(tupd, ii++, "label", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "field1", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "field2", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "field3", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "field4", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "type", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "lockmethod", INT4OID, -1, 0);
	ii = 0;
	strncpy(values[ii++], label, CHARLEN);
	snprintf(values[ii++], CHARLEN, "%d", locktag.locktag_field1);
	snprintf(values[ii++], CHARLEN, "%d", locktag.locktag_field2);
	snprintf(values[ii++], CHARLEN, "%d", locktag.locktag_field3);
	snprintf(values[ii++], CHARLEN, "%d", locktag.locktag_field4);
	strncpy(values[ii++], locktagTypeName(locktag.locktag_type), CHARLEN);
	snprintf(values[ii++], CHARLEN, "%d", locktag.locktag_lockmethodid);
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), Values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	PG_RETURN_DATUM(result);
#undef CHARLEN
}


/*
 * CREATE FUNCTION gdd_external_lock_acquire_myself()
 *		VOLATILE
 *		RETURNS TABLE (result text, field1 int, field2 int, field3 int, field4 int, type text, lockmethod text)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_external_lock_acquire_myself';
 */

PG_FUNCTION_INFO_V1(gdd_external_lock_acquire_myself);

Datum
gdd_external_lock_acquire_myself(PG_FUNCTION_ARGS)
{
	Datum result;

	result = gdd_external_lock_acquire_int(MyProc);
	PG_RETURN_DATUM(result);
}

/*
 * CREATE FUNCTION gdd_external_lock_acquire_pid(pid int)
 *		VOLATILE
 *		RETURNS TABLE (result text, field1 int, field2 int, field3 int, field4 int, type text, lockmethod text)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_external_lock_acquire_pid';
 *
 * If pid == 0, then backed pid will be taken.
 */
PG_FUNCTION_INFO_V1(gdd_external_lock_acquire_proc);

Datum
gdd_external_lock_acquire_proc(PG_FUNCTION_ARGS)
{
	Datum 	 result;
	int	  	 pid;
	PGPROC	*proc;

	pid = PG_GETARG_INT32(0);
	proc = find_pgproc(pid);
	if (proc == NULL)
		elog(ERROR, "Could not find PGPROC entry for pid = %d.", pid);
	result = gdd_external_lock_acquire_int(proc);
	PG_RETURN_DATUM(result);
}

static Datum
gdd_external_lock_acquire_int(PGPROC *proc)
{
#define CHARLEN 32
	LOCKTAG				locktag;
	LockAcquireResult	lock_result;
	/* outoput */
	TupleDesc        tupd;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;
	char             values[7][CHARLEN];
	char			*Values[7];
	Datum            result;
	int				 ii;

	for (ii = 0; ii < 7; ii++)
		Values[ii] = &values[ii][0];
	lock_result = ExternalLockAcquire(proc, &locktag);
	tupd = CreateTemplateTupleDesc(7);
	TupleDescInitEntry(tupd, 1, "result", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, 2, "field1", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 3, "field2", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 4, "field3", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 5, "field4", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 6, "type", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, 7, "lockmethod", INT4OID, -1, 0);
	strncpy(values[0], lockAcquireResultName(lock_result), CHARLEN);
	snprintf(values[1], CHARLEN, "%d", locktag.locktag_field1);
	snprintf(values[2], CHARLEN, "%d", locktag.locktag_field2);
	snprintf(values[3], CHARLEN, "%d", locktag.locktag_field3);
	snprintf(values[4], CHARLEN, "%d", locktag.locktag_field4);
	snprintf(values[5], CHARLEN, "%d", locktag.locktag_field4);
	strncpy(values[6], locktagTypeName(locktag.locktag_type), CHARLEN);
	snprintf(values[7], CHARLEN, "%d", locktag.locktag_lockmethodid);
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), Values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	return(result);

#undef CHARLEN
}

/*
 * CREATE FUNCTION gdd_describe_pid(pid int)
 *		VOLATILE
 *		RETURNS TABLE
 *			(
 *				pgprocno int, waitstaus text, lxid int,
 *				wlocktag1 int, wlocktag2 int, wlocktag3 int, wlocktag4 int, wlocktype text, wlockmethod int,
 *				lockleaderpid int	-- pid of the leader
 *			)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_external_lock_acquire_pid';
 */
PG_FUNCTION_INFO_V1(gdd_describe_pid);

Datum
gdd_describe_pid(PG_FUNCTION_ARGS)
{
#define CHARLEN 32
	PGPROC	*proc;
	int		 pid;
	/* outoput */
	TupleDesc        tupd;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;
	char             values[10][CHARLEN];
	char			*Values[10];
	Datum            result;
	int				 ii;

	for (ii = 0; ii < 10; ii++)
		Values[ii] = &values[ii][0];
	pid = PG_GETARG_INT32(0);
	proc = find_pgproc(pid);
	if (proc == NULL)
		elog(ERROR, "Could not find PGPROC entry for pid = %d.", pid);
	tupd = CreateTemplateTupleDesc(10);
	ii = 1;
	TupleDescInitEntry(tupd, ii++, "pgprocno", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "waitstatus", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag1", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag2", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag3", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag4", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktype", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlockmethod", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "lockleaderpid", INT4OID, -1, 0);
	ii = 0;
	snprintf(values[ii++], CHARLEN, "%d", proc->pgprocno);
	strncpy(values[ii++], waitStatusName(proc->waitStatus), CHARLEN);
	snprintf(values[ii++], CHARLEN, "%d", proc->pgprocno);
	snprintf(values[ii++], CHARLEN, "%d", proc->waitLock ? proc->waitLock->tag.locktag_field1 : -1);
	snprintf(values[ii++], CHARLEN, "%d", proc->waitLock ? proc->waitLock->tag.locktag_field2 : -1);
	snprintf(values[ii++], CHARLEN, "%d", proc->waitLock ? proc->waitLock->tag.locktag_field3 : -1);
	snprintf(values[ii++], CHARLEN, "%d", proc->waitLock ? proc->waitLock->tag.locktag_field4 : -1);
	strncpy(values[ii++], proc->waitLock ? locktagTypeName(proc->waitLock->tag.locktag_type) : "-1", CHARLEN);
	snprintf(values[ii++], CHARLEN, "%d", proc->waitLock ? proc->waitLock->tag.locktag_lockmethodid : -1);
	snprintf(values[ii++], CHARLEN, "%d", proc->lockGroupLeader ? proc->lockGroupLeader->pid : -1);
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), Values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	PG_RETURN_DATUM(result);
#undef CHARLEN
}

/*
 * CREATE FUNCTION gdd_if_has_external_lock(pid int)
 *		STABLE
 *		RETURNS TABLE
 *			(
 *				has_external_lock	bool,
 *				has_external_prop	bool
 *			)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_if_has_external_lock';
 */
PG_FUNCTION_INFO_V1(gdd_if_has_external_lock);

Datum
gdd_if_has_external_lock(PG_FUNCTION_ARGS)
{
#define CHARLEN 32
	int		 pid;
	PGPROC	*proc;
	bool	 has_external_lock;
	bool	 has_external_proc;
	ExternalLockInfo	*external_lock_info;
	/* outoput */
	TupleDesc        tupd;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;
	char             values[2][CHARLEN];
	char			*Values[2];
	Datum            result;
	int				 ii;

	for (ii = 0; ii < 2; ii++)
		Values[ii] = &values[ii][0];
	pid = PG_GETARG_INT32(0);
	proc = find_pgproc(pid);
	if (proc == NULL)
	{
		elog(WARNING, "Could not find PGPROC entry for pid = %d.", pid);
		PG_RETURN_NULL();
	}
	has_external_lock
		= (proc->waitLock && proc->waitLock->tag.locktag_type == LOCKTAG_EXTERNAL) ? true : false;
	if (has_external_lock)
	{
		external_lock_info = GetExternalLockProperties(&(proc->waitLock->tag));
		has_external_proc = external_lock_info ? true : false;
	}
	else
		has_external_proc = false;
	tupd = CreateTemplateTupleDesc(2);
	ii = 1;
	TupleDescInitEntry(tupd, ii++, "has_external_lock", BOOLOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "has_external_proc", BOOLOID, -1, 0);
	ii = 0;
	strncpy(values[ii++], has_external_lock ? "true" : "false", CHARLEN);
	strncpy(values[ii++], has_external_proc ? "true" : "false", CHARLEN);
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), Values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	PG_RETURN_DATUM(result);
#undef CHARLEN
}


/*
 * CREATE FUNCTION gdd_show_external_lock(pid int)
 *		VOLATILE
 *		RETURNS TABLE
 *			(
 *				wlocktag1 int, wlocktag2 int, wlocktag3 int, wlocktag4 int, wlocktype text, wlockmethod int,
 *				dsn text, target_pid int, target_pgprocno int, target_txn
 *			)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_show_external_lock';
 */

PG_FUNCTION_INFO_V1(gdd_show_external_lock);

Datum
gdd_show_external_lock(PG_FUNCTION_ARGS)
{
#define CHARLEN 1024
	int		 pid;
	PGPROC	*proc;
	LOCK	*waitLock;
	ExternalLockInfo	*external_lock_info;
	/* outoput */
	TupleDesc        tupd;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;
	char             values[10][CHARLEN];
	char			*Values[10];
	Datum            result;
	int				 ii;

	for (ii = 0; ii < 10; ii++)
		Values[ii] = &values[ii][0];
	pid = PG_GETARG_INT32(0);
	proc = find_pgproc(pid);
	if (proc == NULL)
		elog(ERROR, "Could not find PGPROC entry for pid = %d.", pid);
	waitLock = proc->waitLock;
	if (!waitLock || waitLock->tag.locktag_type != LOCKTAG_EXTERNAL)
		elog(ERROR, "Pid %d does not waiting for external lock.", pid);
	external_lock_info = GetExternalLockProperties(&(waitLock->tag));
	tupd = CreateTemplateTupleDesc(9);
	ii = 1;
	TupleDescInitEntry(tupd, ii++, "wlocktag1", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag2", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag3", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktag4", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlocktype", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "wlockmethod", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "dsn", TEXTOID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "target_pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "target_pgprocno", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "target_txn", INT4OID, -1, 0);
	ii = 0;
	snprintf(values[ii++], CHARLEN, "%d", waitLock->tag.locktag_field1);
	snprintf(values[ii++], CHARLEN, "%d", waitLock->tag.locktag_field2);
	snprintf(values[ii++], CHARLEN, "%d", waitLock->tag.locktag_field3);
	snprintf(values[ii++], CHARLEN, "%d", waitLock->tag.locktag_field4);
	strncpy(values[ii++], locktagTypeName(waitLock->tag.locktag_type), CHARLEN);
	snprintf(values[ii++], CHARLEN, "%d", waitLock->tag.locktag_lockmethodid);
	strncpy(values[ii++], external_lock_info ? external_lock_info->dsn : "", CHARLEN);
	snprintf(values[ii++], CHARLEN, "%d", external_lock_info ? external_lock_info->target_pid : -1);
	snprintf(values[ii++], CHARLEN, "%d", external_lock_info ? external_lock_info->target_pgprocno : -1);
	snprintf(values[ii++], CHARLEN, "%d", external_lock_info ? external_lock_info->target_txn : -1);
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), Values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	PG_RETURN_DATUM(result);
#undef CHARLEN
}


/*
 * CREATE FUNCTION gdd_show_deadlock_info()
 *		VOLATILE
 *		RETURNS SET OF RECORD
 *			(
 *				no int, locktag1 int, locktag2 int, locktag3 int, locktag4 int, type text, lockmethod int,
 *				lockmode text, pid int, pgprocno int, txid int
 *			)
 *		LANGUAGE c
 *		AS 'gdd_test.so', 'gdd_show_deadlock_info';
 */

PG_FUNCTION_INFO_V1(gdd_show_deadlock_info);

typedef struct myDeadlockInfo
{
	int				 nDeadlockInfo;
	DEADLOCK_INFO	*deadlock_info;
} myDeadlockInfo;

Datum
gdd_show_deadlock_info(PG_FUNCTION_ARGS)
{
#define CHARLEN 64
	FuncCallContext	*funcctx;
	TupleDesc		 tupdesc;
	AttInMetadata	*attinmeta;
	myDeadlockInfo	*my_info;
	HeapTupleData    tupleData;
	HeapTuple        tuple = &tupleData;



	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;

		/* Initialize itelating function */
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		my_info = palloc(sizeof(myDeadlockInfo));
		my_info->deadlock_info = GetDeadLockInfo(&my_info->nDeadlockInfo);
		if (my_info->deadlock_info == NULL)
			elog(ERROR, "No deadlock infor found in this proc.");
		funcctx->user_fctx = my_info;
		funcctx->max_calls = my_info->nDeadlockInfo;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					  (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("function returning record called in context "
							  "that cannot accept type record")));
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum			 result;
		int				 ii;
		char			 values[11][CHARLEN];
		char			*Values[11];

		for (ii = 0; ii < 11; ii++)
			Values[ii] = &values[ii][0];
		attinmeta = funcctx->attinmeta;
		my_info = funcctx->user_fctx;
		ii = 0;
		snprintf(values[ii++], CHARLEN, "%ld", funcctx->call_cntr);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].locktag.locktag_field1);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].locktag.locktag_field2);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].locktag.locktag_field3);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].locktag.locktag_field4);
		strncpy(values[ii++], locktagTypeName(my_info->deadlock_info[funcctx->call_cntr].locktag.locktag_type), CHARLEN);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].locktag.locktag_lockmethodid);
		strncpy(values[ii++], lockModeName(my_info->deadlock_info[funcctx->call_cntr].lockmode), CHARLEN);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].pid);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].pgprocno);
		snprintf(values[ii++], CHARLEN, "%d", my_info->deadlock_info[funcctx->call_cntr].txid);

		tuple = BuildTupleFromCStrings(attinmeta, Values);

		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
#undef CHARLEN
}
	

static PGPROC *
find_pgproc(int pid)
{
	int	ii;
	PGPROC *proc = NULL;

	hold_all_lockline();
	for (ii = 0; ii < ProcGlobal->allProcCount; ii++)
	{
		if (ProcGlobal->allProcs[ii].pid == pid)
		{
			proc = &ProcGlobal->allProcs[ii];
			break;
		}
	}
	for (ii++; ii < ProcGlobal->allProcCount; ii++)
	{
		if (ProcGlobal->allProcs[ii].pid == pid)
		{
			fprintf(outf, "%s(): found duplcate pgproc, pid = %d, pgprocno = %d, why?\n",
					   __func__, pid, ii);
			break;
		}
	}
	release_all_lockline();
	if (pgproc == NULL)
		fprintf(outf, "%s(): failed to find PGPROC, pid=%d.\n", __func__, pid);
	return proc;
}

static void
hold_all_lockline(void)
{
	int ii;

	for (ii = 0; ii < NUM_LOCK_PARTITIONS; ii++)
	LWLockAcquire(LockHashPartitionLockByIndex(ii), LW_EXCLUSIVE);
}

static void
release_all_lockline(void)
{
	int ii;

	for (ii = 0; ii < NUM_LOCK_PARTITIONS; ii++)
	LWLockRelease(LockHashPartitionLockByIndex(ii));
}

static const char *
lockAcquireResultName(LockAcquireResult res)
{
	switch (res)
	{
		case DS_NOT_YET_CHECKED:
			return "DS_NOT_YET_CHECKED";
		case DS_NO_DEADLOCK:
			return "DS_NO_DEADLOCK";
		case DS_SOFT_DEADLOCK:
			return "DS_SOFT_DEADLOCK";
		case DS_HARD_DEADLOCK:
			return "DS_HARD_DEADLOCK";
	}
	return "DS_ERROR_VALUE";
}

static const char *
waitStatusName(int status)
{
	switch (status)
	{
		case STATUS_WAITING:
			return "STATUS_WAITING";
		case STATUS_OK:
			return "STATUS_OK";
		case STATUS_ERROR:
			return "STATUS_ERROR";
	}
	return "STATUS_NO_SUCH_VALUE";
}

static const char *
lockModeName(LOCKMODE lockmode)
{
	switch(lockmode)
	{
		case NoLock:
			return "NoLock";
		case AccessShareLock:
			return "AccessShareLock";
		case RowShareLock:
			return "RowShareLock";
		case RowExclusiveLock:
			return "RowExclusiveLock";
		case ShareUpdateExclusiveLock:
			return "ShareUpdateExclusiveLock";
		case ShareLock:
			return "ShareLock";
		case ShareRowExclusiveLock:
			return "ShareRowExclusiveLock";
		case ExclusiveLock:
			return "ExclusiveLock";
		case AccessExclusiveLock:
			return "AccessExclusiveLock";
	}
	return "NoSuchLockmode";
}

static bool
register_locktag_label(char *label, LOCKTAG *locktag)
{
	LOCKTAG_DICT	*locktag_dict;

	locktag_dict = (LOCKTAG_DICT *)palloc(sizeof(LOCKTAG_DICT));
	locktag_dict->label = pstrdup(label);
	memcpy(&locktag_dict->tag, locktag, sizeof(LOCKTAG));
	return(add_locktag_dict(locktag_dict));
};

static bool
unregister_locktag_label(char *label)
{
	LOCKTAG_DICT	*locktag_dict;

	locktag_dict = find_locktag_dict(label);
	if (locktag_dict == NULL)
		return false;
	return remove_locktag_dict(locktag_dict);
}

static LOCKTAG_DICT *
find_locktag_dict(char *label)
{
	LOCKTAG_DICT *curr = locktag_dict_head;

	if (curr == NULL)
		return NULL;
	for (; curr; curr = curr->next)
		if (strcmp(label, curr->label) == 0)
			return curr;
	return NULL;
}

static bool
add_locktag_dict(LOCKTAG_DICT *locktag_dict)
{
	if (locktag_dict_head == NULL)
	{
		locktag_dict_head = locktag_dict_tail = locktag_dict;
		locktag_dict->next = NULL;
		return true;
	}
	locktag_dict->next = NULL;
	locktag_dict_tail->next = locktag_dict;
	locktag_dict_tail = locktag_dict;
	return true;
}

static void
free_locktag_dict(LOCKTAG_DICT *locktag_dict)
{
	pfree(locktag_dict->label);
	pfree(locktag_dict);
}

static bool
remove_locktag_dict(LOCKTAG_DICT *locktag_dict)
{
	LOCKTAG_DICT *curr;

	if (locktag_dict_head == locktag_dict)
	{
		if (curr->next == NULL)
		{
			locktag_dict_head = locktag_dict_tail = NULL;
			free_locktag_dict(locktag_dict);
			return true;
		}
		curr = curr->next;
		free_locktag_dict(locktag_dict);
		return true;
	}
	for(curr = locktag_dict_head; curr; curr = curr->next)
	{
		if (curr->next == locktag_dict)
		{
			if (curr->next == locktag_dict_tail)
			{
				curr->next = NULL;
				locktag_dict_tail = curr;
			}
			else
			{
				curr->next = curr->next->next;
			}
			free_locktag_dict(locktag_dict);
			return true;
		}
	}
	return false;
}
