/*-------------------------------------------------------------------------
*
* deadlock.c
*	  POSTGRES deadlock detection code
*
* See src/backend/storage/lmgr/README for a description of the deadlock
* detection and resolution algorithms.
*
* Portions Copyright (c) 2020, 2ndQuadrant Ltd.,
* Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
* IDENTIFICATION
*	  src/backend/storage/lmgr/deadlock.c
*
*	Interface:
*
*	DeadLockCheck()
*	DeadLockReport()
*	RememberSimpleDeadLock()
*	InitDeadLockChecking()
*
*  DeadLockCheck() has additional return value.  See proc.c for the handling
*  of this value.
*
*  Additional interface for global deadlock detection:
*
*  get_database_system_id()
*  locktagTypeName()
*  GlobalDeadlockCheck()
*  GlobalDeadlockCheckRemote()
*  GetDeadLockInfo()
*
* SQL functions for global deadlock detection:
*
*  pg_global_deadlock_check_from_remote()
*  pg_global_deadlock_recheck_from_remote()
*  pg_global_deadlock_check_describe_backend()
*	
*-------------------------------------------------------------------------
*/

#define	GDD_DEBUG

#include "postgres.h"

#include <unistd.h>

#include "../interfaces/libpq/libpq-fe.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type_d.h"
#include "common/controldata_utils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"

/*
* DeadlockCheckMode controls the behavior of DeadLockCheck() and DeadLockCheck_int().
* 
* DLCMODE_LOCAL:
*		Used when invoked by local proc.c.   If hard deadlock is detected, then
*		it is returned to proc.c to terminate the backend.
*		If only external lock is found, found deadlock informatin (wait-for-graph,
*		also called external lock path) are all backed up for further global
*		deadlock check.
*		Then all the exernal lick path is examined by visiting downstream database
*		using GlobalDeadlockCheck() or GlobalDeadlockCheck_int().
*
* DLCMODE_GLOBAL_NEW:
*		Indicates DeadLockCheck() or DeadLockCheck_int() is called by
*		pg_global_deadlock_check_from_remote() SQL function.  This SQL function
*		is invked by upstream database through GlobalDeadlockCheck_int().
*		This also indicates this databaes is visited for the first time in
*		global external lock path.
*		In this case, local hard deadlock found by DeadLockCheck_int() is
*		ignored (should be take care of by local DeadLockCheck()).
*		Only wait-for-graphs terminating with external lock (external lock paths) are chosen.
*		Each of these external lock paths are examined further by GlobalDeadlockCheck_int()
*		for further downstream databases.
*
* DLCMODE_GLOBAL_AGAIN:
*		Indicates DeadLockCheck() or DeadLockCheck_int() is called by
*		pg_global_deadlock_check_from_remote() SQL function, as above.
*		This also indicate that this database has been visited in the current chain
*		of external lock path but is not the database of the origin of the path.
*		In this case, if DeadLockCheck() or DeadLockCheck_int() discovers local or
*		global deadlock originating such database visited bofore, this is partial
*		deadlock and will be ignored.   External lock path will be searched and
*		taken care of by GlobalDeadlockCheck_int() for further downstream database.
*		However, to avoid infinit global loop of the serch, external lock path
*		including ever-visited backend should be ignored.
*		Please note that external lock path include more than one of such databae.
*
* DLCMODE_GLOBAL_ORIGIN:
*		Indicates DeadLockCheck() or DeadLockCheck_int() is called buy
*		pg_global_deadlock_check_from_remote() SQL function, as above.
*		This also indicates that this databaes is the origin database which started
*		this external lock path.
*		In this case, deadlock candidate is recorded and returned to upstream databases.
*		Please note that all the deadlock candidates are recorded.
*		Also, further external lock path is searched and recorded for further downstream
*		deadlock search, done by GlobalDeadlockCheck_int().
*		In searching external lock path, we need to take into accoun that another edge
*		However, to avoid infinit global loop of the serch, external lock path
*		including ever-visited backend should be ignored as in the case of DLCMODE_GLOBAL_AGAIN.
*		in the exernal lock path may be of the origin.
*
* These DeadlockCheckMode will be set by globalDeadlockCheckMode() by giving gloabl wait-for-graph
* described below.
*
* Deadlock detection basically uses LOCK component.  Other PG lock components, such as LWlock,
* spinlock and semaphore are building blocks of LOCK component.
*
* As described in many transaction textbook, deadlock can be detected by examining if there's any
* cycle in a graph called wait-for-graph.   Wait-for-graph describes block and wait relationship
* between PG backends.
*
* In local deadlock check, when a backend cannot acquire a lock within deadlock_timeout interval,
* DeadLockCheck() is called by proc.c module.  Before calling, caller must acquire all low level
* locks (LWLock) so that we can can LOCK and PGPROC stablly.
*
* When a cycle is found in the wait-for-graph, we return that deadlock is detected.   In this case,
* caller can terminate this process to resolve the deadlock situation.
*
* In the case of global deadlock, this block-and-wait relationship spans across different databases
* and we need to scan all ths block-and-wait chain across databases to find a cycle in such
* global wait-for-graph.
*
* To represent this remote relationship, new LOCK type, called EXTERNAL LOCK, was added.
* External lock represents waitor of the lock is waiting for remote transaction to complete.
* Becuase actual lock holder is not in the local database, this is also held by the backend
* waiting for it.
*
* Applicaiton of remote transaction, such as FDW remote transaction, should use lock.h
* API to acquire and wait an EXTERNAL LOCK.
*
* In the local deadlock check (DeadLockCheck()), when we find processes waiting for EXTERNAL LOCK,
* waiting-for-graph (in the form of DADLOCK_INFO), is backed up because there could be more than
* one candidate of such wait-for-graph segment.
*
* When no local deadlock was detected and wait-for-graph segment going out to remote transactions
* were found,  DeadLockCheck() returns DS_EXTERNAL_LOCK status.    When the caller receives this
* status, it can release all the LWLocks and then should call GlobalDeadlockCheck().
*
* GlobalDeadlockCheck() runs without such big set of locks so that it take long to examine
* global wait-for-graph spanning across many databases.
*
* Code between #if 0 ... #endif is just for future additional code.   Will be removed when the test
* has been done.
*/

/*
 * See above for definition of the mode.
 *
 * This mode controls behavior of DeadLockCheck().
 */
typedef enum DeadlockCheckMode
{
	DLCMODE_LOCAL,				/* Invoked locally											*/
	DLCMODE_GLOBAL_NEW,			/* Part of global deadlock detection. 						*/
								/* 		First visit to the database in External Lock path	*/
	DLCMODE_GLOBAL_AGAIN,		/* Part of global deadlock detection.						*/
								/* 		Visited this database in the past but it is			*/
								/* 		not the origin.										*/
	DLCMODE_GLOBAL_ORIGIN,		/* Part of global deadlock detection.						*/
								/*		Invoked in the origin database						*/
	DLCMODE_ERROR
} DeadlockCheckMode;

static DeadlockCheckMode	deadlockCheckMode;

/*
* One edge in the waits-for graph.
*
* waiter and blocker may or may not be members of a lock group, but if either
* is, it will be the leader rather than any other member of the lock group.
* The group leaders act as representatives of the whole group even though
* those particular processes need not be waiting at all.  There will be at
* least one member of the waiter's lock group on the wait queue for the given
* lock, maybe more.
*/
typedef struct
{
	PGPROC	   *waiter;			/* the leader of the waiting lock group */
	PGPROC	   *blocker;		/* the leader of the group it is waiting for */
	LOCK	   *lock;			/* the lock being waited for */
	int			pred;			/* workspace for TopoSort */
	int			link;			/* workspace for TopoSort */
} EDGE;

/* One potential reordering of a lock's wait queue */
typedef struct
{
	LOCK	   *lock;			/* the lock whose wait queue is described */
	PGPROC	  **procs;			/* array of PGPROC *'s in new wait order */
	int			nProcs;
} WAIT_ORDER;

/*
 * The following structure is used to backup more than one candidate of
 * global wait-for-graph segment.
 *
 * Unlike other objects used in DeadLockCheck(), DEADLOCK_INFO_BUP object must be
 * allocated dynamically when EXTERNAL LOCK is found in waiting lock of the backend
 * because we cannot forcast maximum number of such segments in advance.
 */
typedef struct DEADLOCK_INFO_BUP
{
	struct DEADLOCK_INFO_BUP *next;
	int				nDeadlock_info;
	DeadLockState	state;
	DEADLOCK_INFO	deadlock_info[0];
} DEADLOCK_INFO_BUP;

DEADLOCK_INFO_BUP	*deadlock_info_bup_head;	/* Head of the queue */
DEADLOCK_INFO_BUP	*deadlock_info_bup_tail;	/* Tail of the queue */

/*
 * The following represents discovered candidate of wait-for-graph segment.
 * This was used in several global deadlock detection functions.
 */
typedef struct RETURNED_WFG
{
	int				  nReturnedWfg;
	DeadLockState	 *state;				/* DS_HARD_DEADLOCK or DS_EXTERNL_LOCK */
	char			**global_wfg_in_text;	/* Text-format global wait-for-graph discovered */
} RETURNED_WFG;


/*
 * Functions for local deadlock detection and and wait-for-graph extraction
 */
static DeadLockState DeadLockCheckRecurse(PGPROC *proc);
static int          TestConfiguration(PGPROC *startProc);
static DeadLockState FindLockCycle(PGPROC *checkProc,
							   EDGE *softEdges, int *nSoftEdges);
static DeadLockState FindLockCycleRecurse(PGPROC *checkProc, int depth,
							   EDGE *softEdges, int *nSoftEdges);
static DeadLockState FindLockCycleRecurseMember(PGPROC *checkProc,
							   PGPROC *checkProcLeader,
							   int depth, EDGE *softEdges, int *nSoftEdges);

static bool ExpandConstraints(EDGE *constraints, int nConstraints);
static bool TopoSort(LOCK *lock, EDGE *constraints, int nConstraints,
				 PGPROC **ordering);

static LOCAL_WFG *BuildLocalWfG(PGPROC *origin, DEADLOCK_INFO_BUP *info);
static void hold_all_lockline(void);
static void release_all_lockline(void);
static void release_all_lockline(void);
#ifdef DEBUG_DEADLOCK
static void PrintLockQueue(LOCK *lock, const char *info);

/*
 * Functions for global lock detection
 */
#endif
#if 0
static bool external_lock_is_same(ExternalLockInfo *one, ExternalLockInfo *two);
#endif
static DeadLockState DeadLockCheck_int(PGPROC *proc);
static int GlobalDeadlockCheckRemote(LOCAL_WFG *local_wfg, GLOBAL_WFG *global_wfg, RETURNED_WFG **returning_wfg);
static DeadLockState GlobalDeadlockRecheckRemote(int	pos, char *global_wfg_text, ExternalLockInfo *downstream);
static DeadLockState GlobalDeadlockCheck_int(PGPROC *proc, GLOBAL_WFG *global_wfg, RETURNED_WFG **rv);
static DeadlockCheckMode globalDeadlockCheckMode(GLOBAL_WFG *global_wfg);
static StringInfo SerializeLocalWfG(LOCAL_WFG *local_wfg);
static LOCAL_WFG *DeserializeLocalWfG(char *buf);
static GLOBAL_WFG *AddToGlobalWfG(GLOBAL_WFG *g_wfg, LOCAL_WFG *local_wfg);
static StringInfo SerializeGlobalWfG(GLOBAL_WFG *g_wfg);
static GLOBAL_WFG *DeserializeGlobalWfG(char *buf);
static void clean_deadlock_info_bup_recursive(DEADLOCK_INFO_BUP *info);
static void clean_deadlock_info_bup(void);
static bool check_local_wfg_is_stable(LOCAL_WFG *localWfG);
static void backup_deadlock_info(DeadLockState state);
static void free_returned_wfg(RETURNED_WFG *returned_wfg);
static RETURNED_WFG *add_returned_wfg(RETURNED_WFG *dest, RETURNED_WFG *src, bool clean_opt);
static RETURNED_WFG *addGlobalWfgToReturnedWfg(RETURNED_WFG *returned_wfg, DeadLockState state, GLOBAL_WFG *global_wfg);
static RETURNED_WFG *globalDeadlockCheckFromRemote(char *global_wfg_text, DeadLockState *state);
static void free_local_wfg(LOCAL_WFG *local_wfg);
static void free_global_wfg(GLOBAL_WFG *global_wfg);
static PGPROC *find_pgproc(int pid);
static PGPROC *find_pgproc_pgprocno(int pgprocno);
static char *build_worker_file_name(bool input_to_command);
static RETURNED_WFG *read_returned_wfg(char *fname);
static LOCAL_WFG *copy_local_wfg(LOCAL_WFG *l_wfg);
static GLOBAL_WFG *copy_global_wfg(GLOBAL_WFG *g_wfg);
char *normalize_str(char *src);

/* WFG utility functions */
static void  appendBinaryStringInfoInt64(StringInfo str, int64 value);
static void  appendBinaryStringInfoInt32(StringInfo str, int32 value);
static void  appendBinaryStringInfoInt16(StringInfo str, int16 value);
static void  appendBinaryStringInfoInt8(StringInfo str, int8 value);
static void  replaceStringInt32(char *s, int32 value);
#if 0
static void  replaceStringInt16(char *s, int16 value);
#endif
static char *extractInt64(char *buf, int64 *value);
#if 0
static char *extractUint64(char *buf, uint64 *value);
#endif
static char *extractInt32(char *buf, int32 *value);
static char *extractUint32(char *buf, uint32 *value);
#if 0
static char *extractInt16(char *buf, int16 *value);
#endif
static char *extractUint16(char *buf, uint16 *value);
#if 0
static char *extractInt8(char *buf, int8 *value);
#endif
static char *extractUint8(char *buf, uint8 *value);
static char *binary2text(char *input, int size);
static char *hexa2bin(char *input, int insize, int *size);

#define WORKER_NAME	"pg_gdd_check_worker"


#define checklen(v, l, e) do{(v) -= (l); if ((v) < 0) goto e;}while(0)
#define Extract64(b, v) do {(b) = extractInt64((b), v);} while(0)
#if 0
#define ExtractU64(b, v) do {(b) = extractUint64((b), v);} while(0)
#endif
#define Extract32(b, v) do {(b) = extractInt32((b), v);} while(0)
#define ExtractU32(b, v) do {(b) = extractUint32((b), v);} while(0)
#if 0
#define Extract16(b, v) do {(b) = extractInt16((b), v);} while(0)
#endif
#define ExtractU16(b, v) do {(b) = extractUint16((b), v);} while(0)
#if 0
#define Extract8(b, v) do {(b) = extractInt8((b), v);} while(0)
#endif
#define ExtractU8(b, v) do {(b) = extractUint8((b), v);} while(0)

#define Lock_PgprocArray() LWLockAcquire(ProcArrayLock, LW_SHARED)
#define Unlock_PgprocArray() LWLockRelease(ProcArrayLock)

/*
 * Working space for the deadlock detector
 */

/* Workspace for FindLockCycle */
static PGPROC **visitedProcs;	/* Array of visited procs */
static int	nVisitedProcs;

/* Workplace for global lock cycle search */
static PGPROC **globalVisitedProcs = NULL;	/* Array of visited procs in global WfG */
static int		nGlobalVisitedProcs = 0;

/* Workplace for visited external locks */
static ExternalLockInfo	**globalVisitedExternalLock = NULL;
static int				  nGlobalVisitedExternalLock = 0;

/*
 * Additional visited proc list from global WfG
 *
 * In checking global cycle, we need process information of the origin of WfG.
 * This is taken from the first member of visited Procs array.
 */
static int	 			visitedOriginPid = 0;			/* pid */
static int	 			visitedOriginPgprocno = 0;		/* pgprocno */
static TransactionId	visitedOriginTxid = 0;			/* transaction id */

/* Workspace for TopoSort */
static PGPROC **topoProcs;		/* Array of not-yet-output procs */
static int *beforeConstraints;	/* Counts of remaining before-constraints */
static int *afterConstraints;	/* List head for after-constraints */

/* Output area for ExpandConstraints */
static WAIT_ORDER *waitOrders;	/* Array of proposed queue rearrangements */
static int	nWaitOrders;
static PGPROC **waitOrderProcs; /* Space for waitOrders queue contents */

/* Current list of constraints being considered */
static EDGE *curConstraints;
static int	nCurConstraints;
static int	maxCurConstraints;

/* Storage space for results from FindLockCycle */
static EDGE *possibleConstraints;
static int	nPossibleConstraints;
static int	maxPossibleConstraints;
static DEADLOCK_INFO *deadlockDetails;
static int	nDeadlockDetails;

/* PGPROC pointer of any blocking autovacuum worker found */
static PGPROC *blocking_autovacuum_proc = NULL;

#ifdef GDD_DEBUG
GLOBAL_WFG *global_wfg_test;
#endif
/*
 * InitDeadLockChecking -- initialize deadlock checker during backend startup
 *
 * This does per-backend initialization of the deadlock checker; primarily,
 * allocation of working memory for DeadLockCheck.  We do this per-backend
 * since there's no percentage in making the kernel do copy-on-write
 * inheritance of workspace from the postmaster.  We want to allocate the
 * space at startup because (a) the deadlock checker might be invoked when
 * there's no free memory left, and (b) the checker is normally run inside a
 * signal handler, which is a very dangerous place to invoke palloc from.
 */
void
InitDeadLockChecking(void)
{
	MemoryContext oldcxt;

	/* Make sure allocations are permanent */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * FindLockCycle needs at most MaxBackends entries in visitedProcs[] and
	 * deadlockDetails[].
	 */
	visitedProcs = (PGPROC **) palloc(MaxBackends * sizeof(PGPROC *));
	deadlockDetails = (DEADLOCK_INFO *) palloc(MaxBackends * sizeof(DEADLOCK_INFO));

	/*
	 * TopoSort needs to consider at most MaxBackends wait-queue entries, and
	 * it needn't run concurrently with FindLockCycle.
	 */
	topoProcs = visitedProcs;	/* re-use this space */
	beforeConstraints = (int *) palloc(MaxBackends * sizeof(int));
	afterConstraints = (int *) palloc(MaxBackends * sizeof(int));

	/*
	 * We need to consider rearranging at most MaxBackends/2 wait queues
	 * (since it takes at least two waiters in a queue to create a soft edge),
	 * and the expanded form of the wait queues can't involve more than
	 * MaxBackends total waiters.
	 */
	waitOrders = (WAIT_ORDER *)
		palloc((MaxBackends / 2) * sizeof(WAIT_ORDER));
	waitOrderProcs = (PGPROC **) palloc(MaxBackends * sizeof(PGPROC *));

	/*
	 * Allow at most MaxBackends distinct constraints in a configuration. (Is
	 * this enough?  In practice it seems it should be, but I don't quite see
	 * how to prove it.  If we run out, we might fail to find a workable wait
	 * queue rearrangement even though one exists.)  NOTE that this number
	 * limits the maximum recursion depth of DeadLockCheckRecurse. Making it
	 * really big might potentially allow a stack-overflow problem.
	 */
	maxCurConstraints = MaxBackends;
	curConstraints = (EDGE *) palloc(maxCurConstraints * sizeof(EDGE));

	/*
	 * Allow up to 3*MaxBackends constraints to be saved without having to
	 * re-run TestConfiguration.  (This is probably more than enough, but we
	 * can survive if we run low on space by doing excess runs of
	 * TestConfiguration to re-compute constraint lists each time needed.) The
	 * last MaxBackends entries in possibleConstraints[] are reserved as
	 * output workspace for FindLockCycle.
	 */
	maxPossibleConstraints = MaxBackends * 4;
	possibleConstraints =
		(EDGE *) palloc(maxPossibleConstraints * sizeof(EDGE));

	deadlock_info_bup_head = deadlock_info_bup_tail = NULL;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * DeadLockCheck -- Checks for deadlocks for a given process
 *
 * This code looks for deadlocks involving the given process.  If any
 * are found, it tries to rearrange lock wait queues to resolve the
 * deadlock.  If resolution is impossible, return DS_HARD_DEADLOCK ---
 * the caller is then expected to abort the given proc's transaction.
 *
 * Caller must already have locked all partitions of the lock tables.
 *
 * On failure, deadlock details are recorded in deadlockDetails[] for
 * subsequent printing by DeadLockReport().  That activity is separate
 * because (a) we don't want to do it while holding all those LWLocks,
 * and (b) we are typically invoked inside a signal handler.
 *
 * When DS_EXTERNAL_LOCK is returned from this function, it means there
 * are no local deadlock but there are candidate wait-for-graph segment
 * going out to a remote transaction.
 *
 * In this case, the caller must release all the low-level LWlock for
 * LOCK lockline and call GlobalDeadlochCheck().
 *
 * These two functions are separated because the caller (proc.c) must
 * terminate the backend when DS_HARD_DEADLOCK is holding all the
 * low level locks acquired.
 */
DeadLockState
DeadLockCheck(PGPROC *proc)
{
	DeadLockState	status;

	deadlockCheckMode = globalDeadlockCheckMode(NULL);

	status = DeadLockCheck_int(proc);
	if (status == DS_DEADLOCK_INFO)
		status = DS_EXTERNAL_LOCK;
	return status;
}

static DeadLockState
DeadLockCheck_int(PGPROC *proc)
{
	int			i,
				j;
	DeadLockState	status;

	/* Initialize to "no constraints" */
	nCurConstraints = 0;
	nPossibleConstraints = 0;
	nWaitOrders = 0;

	/* Initialize deadlock info storage */
	deadlock_info_bup_head = deadlock_info_bup_tail = NULL;

	/* Initialize to not blocked by an autovacuum worker */
	blocking_autovacuum_proc = NULL;

	/* Search for deadlocks and possible fixes */
	status = DeadLockCheckRecurse(proc);
	/*
	 * K.Suzuki: external lock 検出時の処理を追加
	 */
	if (status == DS_HARD_DEADLOCK)
	{
		/*
		 * This status is only for deadlockCheckMode == DLCMODE_LOCAL.
		 *
		 * Call FindLockCycle one more time, to record the correct
		 * deadlockDetails[] for the basic state with no rearrangements.
		 */
		int			nSoftEdges;

		TRACE_POSTGRESQL_DEADLOCK_FOUND();

		nWaitOrders = 0;
		if (!FindLockCycle(proc, possibleConstraints, &nSoftEdges))
			elog(FATAL, "deadlock seems to have disappeared");

		return status;	/* cannot find a non-deadlocked state */
	}
	if (status == DS_DEADLOCK_INFO)
		/*
		 * This status is only for deadlockCheckMode == DLCMODE_GLOBAL_xxx
		 */
		return status;
	
	/* Apply any needed rearrangements of wait queues */
	for (i = 0; i < nWaitOrders; i++)
	{
		LOCK	   *lock = waitOrders[i].lock;
		PGPROC	  **procs = waitOrders[i].procs;
		int			nProcs = waitOrders[i].nProcs;
		PROC_QUEUE *waitQueue = &(lock->waitProcs);

		Assert(nProcs == waitQueue->size);

#ifdef DEBUG_DEADLOCK
		PrintLockQueue(lock, "DeadLockCheck:");
#endif

		/* Reset the queue and re-add procs in the desired order */
		ProcQueueInit(waitQueue);
		for (j = 0; j < nProcs; j++)
		{
			SHMQueueInsertBefore(&(waitQueue->links), &(procs[j]->links));
			waitQueue->size++;
		}

#ifdef DEBUG_DEADLOCK
		PrintLockQueue(lock, "rearranged to:");
#endif

		/* See if any waiters for the lock can be woken up now */
		ProcLockWakeup(GetLocksMethodTable(lock), lock);
	}

	/* Return code tells caller if we had to escape a deadlock or not */
	if (nWaitOrders > 0)
		return DS_SOFT_DEADLOCK;
	else if (blocking_autovacuum_proc != NULL)
		return DS_BLOCKED_BY_AUTOVACUUM;
	else
		return DS_NO_DEADLOCK;
}

/*
 * Return the PGPROC of the autovacuum that's blocking a process.
 *
 * We reset the saved pointer as soon as we pass it back.
 */
PGPROC *
GetBlockingAutoVacuumPgproc(void)
{
	PGPROC	   *ptr;

	ptr = blocking_autovacuum_proc;
	blocking_autovacuum_proc = NULL;

	return ptr;
}

/*
 * DeadLockCheckRecurse -- recursively search for valid orderings
 *
 * curConstraints[] holds the current set of constraints being considered
 * by an outer level of recursion.  Add to this each possible solution
 * constraint for any cycle detected at this level.
 *
 * Returns true if no solution exists.  Returns false if a deadlock-free
 * state is attainable, in which case waitOrders[] shows the required
 * rearrangements of lock wait queues (if any).
 *
 * Return value:
 *	DS_NO_DEADLOCK: no hard deadlock or not waiting for external lock
 *  DS_HARD_DEADLOCK: local deadlock detected
 *  DS_EXTERNAL_LOCK: exrernal lock detected, need to do global deadlock check.
 */
static DeadLockState
DeadLockCheckRecurse(PGPROC *proc)
{
	int			nEdges;
	int			oldPossibleConstraints;
	bool		savedList;
	int			i;
	DeadLockState	status = DS_NOT_YET_CHECKED;

	/*
	 * K.Suzuki:
	 * TestConfiguration() で、DS_HARD_DEADLOCK と DS_EXTERNAL_LOCK が
	 * 識別できるようにしておく必要がある
	 */
	nEdges = TestConfiguration(proc);
	if (nEdges == -2)		/* K.Suzuki external lock 検出処理の追加 */
		return DS_DEADLOCK_INFO;
	if (nEdges == -1)
		return DS_HARD_DEADLOCK;	/* hard deadlock --- no solution */
	if (nEdges < 0)
		elog(FATAL, "inconsistent results during deadlock check");
	if (nEdges == 0)
		return DS_NO_DEADLOCK;		/* good configuration found */
	if (nCurConstraints >= maxCurConstraints)
		return DS_HARD_DEADLOCK;	/* out of room for active constraints? */
	oldPossibleConstraints = nPossibleConstraints;
	if (nPossibleConstraints + nEdges + MaxBackends <= maxPossibleConstraints)
	{
		/* We can save the edge list in possibleConstraints[] */
		nPossibleConstraints += nEdges;
		savedList = true;
	}
	else
	{
		/* Not room; will need to regenerate the edges on-the-fly */
		savedList = false;
	}

	/*
	 * Try each available soft edge as an addition to the configuration.
	 */
	for (i = 0; i < nEdges; i++)
	{
		if (!savedList && i > 0)
		{
			/* Regenerate the list of possible added constraints */
			/*
			 * K.Suzuki ここ、i = 0  に対する TestConfiguration() の呼び出しは loop の
			 * 外で済んでいるので、i == 0 では呼び出さず、結果を利用するだけ。
			 */
			if (nEdges != TestConfiguration(proc))
				elog(FATAL, "inconsistent results during deadlock check");
		}
		curConstraints[nCurConstraints] =
			possibleConstraints[oldPossibleConstraints + i];
		nCurConstraints++;
		status = DeadLockCheckRecurse(proc);
		if (status != DS_HARD_DEADLOCK && status != DS_DEADLOCK_INFO)
			return status;		/* found a valid solution! -- no deadlock */
		/* give up on that added constraint, try again */
		nCurConstraints--;
	}
	nPossibleConstraints = oldPossibleConstraints;
	return status;				/* no solution found */
}


/*--------------------
 * Test a configuration (current set of constraints) for validity.
 *
 * K.Suzuki: ここ、external lock の検出とその返却方法を追加する必要がある。
 *           返却値に -2 を追加して、これが external lock 検出を示すようにしよう。
 *
 * Returns:
 *		0: the configuration is good (no deadlocks)
 *	   -1: the configuration has a hard deadlock or is not self-consistent
 *     -2: external lock is included in the configuration
 *			the configuration has an external lock at the edge of the graph.
 *		>0: the configuration has one or more soft deadlocks
 *
 * In the soft-deadlock case, one of the soft cycles is chosen arbitrarily
 * and a list of its soft edges is returned beginning at
 * possibleConstraints+nPossibleConstraints.  The return value is the
 * number of soft edges.
 *--------------------
 */
static int
TestConfiguration(PGPROC *startProc)
{
	int			softFound = 0;
	EDGE	   *softEdges = possibleConstraints + nPossibleConstraints;
	int			nSoftEdges;
	int			i;
	DeadLockState	state;

	/*
	 * Make sure we have room for FindLockCycle's output.
	 */
	if (nPossibleConstraints + MaxBackends > maxPossibleConstraints)
		return -1;

	/*
	 * Expand current constraint set into wait orderings.  Fail if the
	 * constraint set is not self-consistent.
	 */
	if (!ExpandConstraints(curConstraints, nCurConstraints))
		return -1;

	/*
	 * Check for cycles involving startProc or any of the procs mentioned in
	 * constraints.  We check startProc last because if it has a soft cycle
	 * still to be dealt with, we want to deal with that first.
	 */
	for (i = 0; i < nCurConstraints; i++)
	{

		state = FindLockCycle(curConstraints[i].waiter, softEdges, &nSoftEdges);
		if (state)
		{
			if (nSoftEdges == 0)
			{
				if (state == DS_HARD_DEADLOCK)
					return -1;		/* hard deadlock detected */
				else if (state == DS_DEADLOCK_INFO)
					return -2;
				else
					elog(ERROR, "Inconsistent internal state in deadlock check.");
			}
			softFound = nSoftEdges;
		}
		state = FindLockCycle(curConstraints[i].blocker, softEdges, &nSoftEdges);
		if (state)
		{
			if (nSoftEdges == 0)
			{
				if (state == DS_HARD_DEADLOCK)
					return -1;		/* hard deadlock detected */
				else if (state == DS_EXTERNAL_LOCK)
					return -2;
				else
					elog(ERROR, "Inconsistent internal state in deadlock check.");
			}
			softFound = nSoftEdges;
		}
	}
	state = FindLockCycle(startProc, softEdges, &nSoftEdges);
	if (state)
	{
		if (nSoftEdges == 0)
		{
			if (state == DS_HARD_DEADLOCK)
				return -1;		/* hard deadlock detected */
			else if (state == DS_DEADLOCK_INFO)
				return -2;
			else
				elog(ERROR, "Inconsistent internal state in deadlock check.");
		}
		softFound = nSoftEdges;
	}
	return softFound;
}


/*
 * FindLockCycle -- basic check for deadlock cycles
 *
 * Scan outward from the given proc to see if there is a cycle in the
 * waits-for graph that includes this proc.  Return true if a cycle
 * is found, else false.  If a cycle is found, we return a list of
 * the "soft edges", if any, included in the cycle.  These edges could
 * potentially be eliminated by rearranging wait queues.  We also fill
 * deadlockDetails[] with information about the detected cycle; this info
 * is not used by the deadlock algorithm itself, only to print a useful
 * message after failing.
 *
 * Since we need to be able to check hypothetical configurations that would
 * exist after wait queue rearrangement, the routine pays attention to the
 * table of hypothetical queue orders in waitOrders[].  These orders will
 * be believed in preference to the actual ordering seen in the locktable.
 *
 * Please take a look at the top of this file for deadlock check mode
 * and related behavior.
 */
static DeadLockState
FindLockCycle(PGPROC *checkProc,
			  EDGE *softEdges,	/* output argument */
			  int *nSoftEdges)	/* output argument */
{
	nVisitedProcs = 0;
	nDeadlockDetails = 0;
	*nSoftEdges = 0;
	return FindLockCycleRecurse(checkProc, 0, softEdges, nSoftEdges);
}

static DeadLockState
FindLockCycleRecurse(PGPROC *checkProc,
					 int depth,
					 EDGE *softEdges,	/* output argument */
					 int *nSoftEdges)	/* output argument */
{
	dlist_iter	iter;
	DeadLockState	rv = DS_NO_DEADLOCK;

	/*
	 * If this process is a lock group member, check the leader instead. (Note
	 * that we might be the leader, in which case this is a no-op.)
	 */
	if (checkProc->lockGroupLeader != NULL)
		checkProc = checkProc->lockGroupLeader;

	/*
	 * Have we already seen this proc?
	 */
	if (deadlockCheckMode == DLCMODE_GLOBAL_ORIGIN)
	{
		if (visitedOriginPid == checkProc->pid &&
			visitedOriginPgprocno == checkProc->pgprocno &&
			visitedOriginTxid == checkProc->lxid)
		{
			/* Here, global wait-for-graph cycle has been found */

			memset(&deadlockDetails[nDeadlockDetails].locktag, 0, sizeof(LOCKTAG));

			if (checkProc->waitLock)
			{
				DEADLOCK_INFO	*info;
					
				nDeadlockDetails = depth + 1;
				info = &deadlockDetails[nDeadlockDetails - 1];

				info->locktag = checkProc->waitLock->tag;
				info->lockmode = checkProc->waitLockMode;
				info->pid = checkProc->pid;
				info->pgprocno = checkProc->pgprocno;
				info->txid = checkProc->lxid;
				backup_deadlock_info(DS_HARD_DEADLOCK);
				return DS_DEADLOCK_INFO;
			}
			else
				return DS_NO_DEADLOCK;
		}
		else
			return DS_NO_DEADLOCK;
	}
	if (deadlockCheckMode == DLCMODE_LOCAL)
	{
		/*
		 * K.Suzuki 以下の部分は元のコードを respect して活かしてあるが、そもそも ii == 0 の時だけを
		 * チェックすればいいので、余分なことをしているのでは？
		 */
		/*
		 * Check if there's local wait-for-graph cycle.  This is done only at
		 * LOCAL check mode.
		 */
#if 1
		if (visitedProcs[0] == checkProc)
		{
			Assert(depth <= MaxBackends);

			nDeadlockDetails = depth;
			return DS_HARD_DEADLOCK;
		}
#else
		for (i = 0; i < nVisitedProcs; i++)
		{
			if (visitedProcs[i] == checkProc)
			{
				/* If we return to starting point, we have a deadlock cycle */
				if (i == 0)
				{
					/*
					 * record total length of cycle --- outer levels will now fill
					 * deadlockDetails[]
					 */
					Assert(depth <= MaxBackends);
					nDeadlockDetails = depth;

					return DS_HARD_DEADLOCK;
				}

				/*
				 * Otherwise, we have a cycle but it does not include the start
				 * point, so say "no deadlock".
				 */
				/*
				 * K.Suzuki: これ、おかしい。他にもチェックすべき external lock があるかもしれないので、ここで
				 * リターンしてはいけなのでは？
				 */
				return DS_NO_DEADLOCK;
			}
		}
#endif
	}
	/* Mark proc as seen */
	Assert(nVisitedProcs < MaxBackends);
	visitedProcs[nVisitedProcs++] = checkProc;

	/*
	 * If the process is waiting, there is an outgoing waits-for edge to each
	 * process that blocks it.
	 */
	/*
	 * K.Suzuki: ここで waitLock を見ているので、external lockのチェックを
	 *			 ここに追加できるはず。
	 */
	if (checkProc->waitLock != NULL)
	{
		if (checkProc->waitLock->tag.locktag_type == LOCKTAG_EXTERNAL)
		{
			/*
			 * Now we found EXTERNAL LOCK.  We need to check wait-for-graph
			 * extending to the remote transaction specified by this EXTERNAL
			 * LOCK.
			 */

			/* fill deadlockDetails[] */
			DEADLOCK_INFO *info = &deadlockDetails[depth];

			info->locktag = checkProc->waitLock->tag;
			info->lockmode = checkProc->waitLockMode;
			/*
			 * K.Suzuki: 以下のものはロックグループリーダのものである
			 *			 必要はないか？
			 */
			info->pid = checkProc->pid;
			info->pgprocno = checkProc->pgprocno;
			info->txid = checkProc->lxid;
			/*
			 * Different from LOCAL mode, we need to add external lock to DEADLOCK_INFO
			 * for further anaoysis.
			 */
			nDeadlockDetails = depth + 1;

			/*
			 * Here we backup the deadlock info for safer place.  Deadlock info
			 * area will be reused for further analysis.
			 */
			backup_deadlock_info(DS_EXTERNAL_LOCK);

			return DS_DEADLOCK_INFO;
		}
	}
	/*
	 * K.Suzuki: FindLockCycleRecurseMember() 内部で external lock を見る必要があるかどうか
	 *			 調べること。これに寄っては FindLockCycleRecurseMember() の戻り値が変更になる
	 *			 可能性がある。
	 *			 確か FindFLockCYcleRecurseMember() 内部で FindLockCycleRecurse() 呼んでいたような
	 *			 気がする。
	 *			 取り敢えず上記は反映したけれど、DS_EXTERNAL_LOCK 時の後処理が別に必要かもしれない。
	 */
	if (checkProc->links.next != NULL && checkProc->waitLock != NULL)

	{
		DeadLockState	state;

		state = FindLockCycleRecurseMember(checkProc, checkProc, depth, softEdges, nSoftEdges);
#if 0
		if (state == DS_HARD_DEADLOCK || state == DS_EXTERNAL_LOCK)
#endif
		if (state == DS_HARD_DEADLOCK)
			return state;
		rv = state;
	}

	/*
	 * If the process is not waiting, there could still be outgoing waits-for
	 * edges if it is part of a lock group, because other members of the lock
	 * group might be waiting even though this process is not.  (Given lock
	 * groups {A1, A2} and {B1, B2}, if A1 waits for B1 and B2 waits for A2,
	 * that is a deadlock even neither of B1 and A2 are waiting for anything.)
	 */
	dlist_foreach(iter, &checkProc->lockGroupMembers)
	{
		PGPROC	   *memberProc;

		memberProc = dlist_container(PGPROC, lockGroupLink, iter.cur);

		/*
		 * K.Suzuki: FindLockCycleRecurseMember() 内部で external lock を見る必要があるかどうか
		 *			 調べること。これに寄っては FindLockCycleRecurseMember() の戻り値が変更になる
		 *			 可能性がある。
		 *			 確か FindFLockCYcleRecurseMember() 内部で FindLockCycleRecurse() 呼んでいたような
		 *			 気がする。
		 *			 取り敢えず上記は反映したけれど、DS_EXTERNAL_LOCK 時の後処理が別に必要かもしれない。
		 */
		if (memberProc->links.next != NULL && memberProc->waitLock != NULL && memberProc != checkProc)
		{
			DeadLockState	state;

			state = FindLockCycleRecurseMember(memberProc, checkProc, depth, softEdges, nSoftEdges);
#if 0
			if (state == DS_HARD_DEADLOCK || state == DS_EXTERNAL_LOCK)
#else
			if (state == DS_HARD_DEADLOCK)
#endif
				return state;
			if (state == DS_EXTERNAL_LOCK)
				rv = DS_EXTERNAL_LOCK;
		}
	}

	return rv;
}

static DeadLockState
FindLockCycleRecurseMember(PGPROC *checkProc,
						   PGPROC *checkProcLeader,
						   int depth,
						   EDGE *softEdges, /* output argument */
						   int *nSoftEdges) /* output argument */
{
	PGPROC	   *proc;
	LOCK	   *lock = checkProc->waitLock;
	PGXACT	   *pgxact;
	PROCLOCK   *proclock;
	SHM_QUEUE  *procLocks;
	LockMethod	lockMethodTable;
	PROC_QUEUE *waitQueue;
	int			queue_size;
	int			conflictMask;
	int			i;
	int			numLockModes,
				lm;
	DeadLockState	rv = DS_NO_DEADLOCK;

	lockMethodTable = GetLocksMethodTable(lock);
	numLockModes = lockMethodTable->numLockModes;
	conflictMask = lockMethodTable->conflictTab[checkProc->waitLockMode];

	/*
	 * Scan for procs that already hold conflicting locks.  These are "hard"
	 * edges in the waits-for graph.
	 */
	procLocks = &(lock->procLocks);

	proclock = (PROCLOCK *) SHMQueueNext(procLocks, procLocks,
										 offsetof(PROCLOCK, lockLink));

	while (proclock)
	{
		PGPROC	   *leader;

		proc = proclock->tag.myProc;
		pgxact = &ProcGlobal->allPgXact[proc->pgprocno];
		leader = proc->lockGroupLeader == NULL ? proc : proc->lockGroupLeader;

		/* A proc never blocks itself or any other lock group member */
		if (leader != checkProcLeader)
		{
			for (lm = 1; lm <= numLockModes; lm++)
			{
				if ((proclock->holdMask & LOCKBIT_ON(lm)) &&
					(conflictMask & LOCKBIT_ON(lm)))
				{
					DeadLockState state;
					/* fill deadlockDetails[] */
					DEADLOCK_INFO *info = &deadlockDetails[depth];

					info->locktag = lock->tag;
					info->lockmode = checkProc->waitLockMode;
					/*
					 * K.Suzuki: 以下のものはロックグループリーダのものである
					 *			 必要はないか？
					 */
					info->pid = checkProc->pid;
					info->pgprocno = checkProc->pgprocno;
					info->txid = checkProc->lxid;

					/* This proc hard-blocks checkProc */
					state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
					if (state == DS_HARD_DEADLOCK)
						return state;
					rv = state;

					/*
					 * No deadlock here, but see if this proc is an autovacuum
					 * that is directly hard-blocking our own proc.  If so,
					 * report it so that the caller can send a cancel signal
					 * to it, if appropriate.  If there's more than one such
					 * proc, it's indeterminate which one will be reported.
					 *
					 * We don't touch autovacuums that are indirectly blocking
					 * us; it's up to the direct blockee to take action.  This
					 * rule simplifies understanding the behavior and ensures
					 * that an autovacuum won't be canceled with less than
					 * deadlock_timeout grace period.
					 *
					 * Note we read vacuumFlags without any locking.  This is
					 * OK only for checking the PROC_IS_AUTOVACUUM flag,
					 * because that flag is set at process start and never
					 * reset.  There is logic elsewhere to avoid canceling an
					 * autovacuum that is working to prevent XID wraparound
					 * problems (which needs to read a different vacuumFlag
					 * bit), but we don't do that here to avoid grabbing
					 * ProcArrayLock.
					 */
					if (checkProc == MyProc &&
						pgxact->vacuumFlags & PROC_IS_AUTOVACUUM)
						blocking_autovacuum_proc = proc;

					/* We're done looking at this proclock */
					break;
				}
			}
		}

		proclock = (PROCLOCK *) SHMQueueNext(procLocks, &proclock->lockLink,
											 offsetof(PROCLOCK, lockLink));
	}

	/*
	 * Scan for procs that are ahead of this one in the lock's wait queue.
	 * Those that have conflicting requests soft-block this one.  This must be
	 * done after the hard-block search, since if another proc both hard- and
	 * soft-blocks this one, we want to call it a hard edge.
	 *
	 * If there is a proposed re-ordering of the lock's wait order, use that
	 * rather than the current wait order.
	 */
	/*
	 * K.Suzuki: 以下、これから見る。
	 */
	for (i = 0; i < nWaitOrders; i++)
	{
		if (waitOrders[i].lock == lock)
			break;
	}

	if (i < nWaitOrders)
	{
		/* Use the given hypothetical wait queue order */
		PGPROC	  **procs = waitOrders[i].procs;

		queue_size = waitOrders[i].nProcs;

		for (i = 0; i < queue_size; i++)
		{
			PGPROC	   *leader;

			proc = procs[i];
			leader = proc->lockGroupLeader == NULL ? proc :
				proc->lockGroupLeader;

			/*
			 * TopoSort will always return an ordering with group members
			 * adjacent to each other in the wait queue (see comments
			 * therein). So, as soon as we reach a process in the same lock
			 * group as checkProc, we know we've found all the conflicts that
			 * precede any member of the lock group lead by checkProcLeader.
			 */
			if (leader == checkProcLeader)
				break;

			/* Is there a conflict with this guy's request? */
			if ((LOCKBIT_ON(proc->waitLockMode) & conflictMask) != 0)
			{
				DeadLockState state;
				/* fill deadlockDetails[] */
				DEADLOCK_INFO *info = &deadlockDetails[depth];

				info->locktag = lock->tag;
				info->lockmode = checkProc->waitLockMode;
				/*
				 * K.Suzuki: 以下のデータはロックグループリーダのものである必要はないか
				 */
				info->pid = checkProc->pid;
				info->pgprocno = checkProc->pgprocno;
				info->txid = checkProc->lxid;


				/* This proc soft-blocks checkProc */
				state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
#if 0
				if (state == DS_HARD_DEADLOCK || DS_EXTERNAL_LOCK)
#endif
				if (state == DS_HARD_DEADLOCK)
				{
					/*
					 * Add this edge to the list of soft edges in the cycle
					 */
					/*
					 * K.Suzuki: 以下の後処理が EXTERNAL LOCK 時にも必要かどうか
					 *			 後で見て見る必要がある。
					 */
					Assert(*nSoftEdges < MaxBackends);
					softEdges[*nSoftEdges].waiter = checkProcLeader;
					softEdges[*nSoftEdges].blocker = leader;
					softEdges[*nSoftEdges].lock = lock;
					(*nSoftEdges)++;
					return state;
				}
				if (state == DS_DEADLOCK_INFO)
					rv = DS_DEADLOCK_INFO;
			}
		}
	}
	else
	{
		PGPROC	   *lastGroupMember = NULL;

		/* Use the true lock wait queue order */
		waitQueue = &(lock->waitProcs);

		/*
		 * Find the last member of the lock group that is present in the wait
		 * queue.  Anything after this is not a soft lock conflict. If group
		 * locking is not in use, then we know immediately which process we're
		 * looking for, but otherwise we've got to search the wait queue to
		 * find the last process actually present.
		 */
		if (checkProc->lockGroupLeader == NULL)
			lastGroupMember = checkProc;
		else
		{
			proc = (PGPROC *) waitQueue->links.next;
			queue_size = waitQueue->size;
			while (queue_size-- > 0)
			{
				if (proc->lockGroupLeader == checkProcLeader)
					lastGroupMember = proc;
				proc = (PGPROC *) proc->links.next;
			}
			Assert(lastGroupMember != NULL);
		}

		/*
		 * OK, now rescan (or scan) the queue to identify the soft conflicts.
		 */
		queue_size = waitQueue->size;
		proc = (PGPROC *) waitQueue->links.next;
		while (queue_size-- > 0)
		{
			PGPROC	   *leader;

			leader = proc->lockGroupLeader == NULL ? proc :
				proc->lockGroupLeader;

			/* Done when we reach the target proc */
			if (proc == lastGroupMember)
				break;

			/* Is there a conflict with this guy's request? */
			if ((LOCKBIT_ON(proc->waitLockMode) & conflictMask) != 0 &&
				leader != checkProcLeader)
			{
				DeadLockState state;
				/* fill deadlockDetails[] */
				DEADLOCK_INFO *info = &deadlockDetails[depth];

				info->locktag = lock->tag;
				info->lockmode = checkProc->waitLockMode;
				/*
				 * K.Suzuki: 以下のデータはロックグループリーダのものである必要はないか
				 */
				info->pid = checkProc->pid;
				info->pgprocno = checkProc->pgprocno;
				info->txid = checkProc->lxid;


				/* This proc soft-blocks checkProc */
				state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
#if 0
				if (state == DS_HARD_DEADLOCK || state == DS_EXTERNAL_LOCK)
#endif
				if (state == DS_HARD_DEADLOCK)
				{
					/*
					 * Add this edge to the list of soft edges in the cycle
					 */
					/*
					 * K.Suzuki: 以下の後処理が EXRTERNAL LOCK 検出時にも必要か
					 *			 チェックして見る必要がある。
					 */
					Assert(*nSoftEdges < MaxBackends);
					softEdges[*nSoftEdges].waiter = checkProcLeader;
					softEdges[*nSoftEdges].blocker = leader;
					softEdges[*nSoftEdges].lock = lock;
					(*nSoftEdges)++;
					return state;
				}
				if (state == DS_DEADLOCK_INFO)
					rv = DS_DEADLOCK_INFO;
			}

			proc = (PGPROC *) proc->links.next;
		}
	}

	/*
	 * No conflict detected or EXTERNAL LOCK found.
	 */
	return rv;
}


/*
 * ExpandConstraints -- expand a list of constraints into a set of
 *		specific new orderings for affected wait queues
 *
 * Input is a list of soft edges to be reversed.  The output is a list
 * of nWaitOrders WAIT_ORDER structs in waitOrders[], with PGPROC array
 * workspace in waitOrderProcs[].
 *
 * Returns true if able to build an ordering that satisfies all the
 * constraints, false if not (there are contradictory constraints).
 *
 * K.Suzuki: この関数に渡す Edge には external lock が含まれないように保証しておく必要がある。
 */
static bool
ExpandConstraints(EDGE *constraints,
				  int nConstraints)
{
	int			nWaitOrderProcs = 0;
	int			i,
				j;

	nWaitOrders = 0;

	/*
	 * Scan constraint list backwards.  This is because the last-added
	 * constraint is the only one that could fail, and so we want to test it
	 * for inconsistency first.
	 */
	for (i = nConstraints; --i >= 0;)
	{
		LOCK	   *lock = constraints[i].lock;

		/* Did we already make a list for this lock? */
		for (j = nWaitOrders; --j >= 0;)
		{
			if (waitOrders[j].lock == lock)
				break;
		}
		if (j >= 0)
			continue;
		/* No, so allocate a new list */
		waitOrders[nWaitOrders].lock = lock;
		waitOrders[nWaitOrders].procs = waitOrderProcs + nWaitOrderProcs;
		waitOrders[nWaitOrders].nProcs = lock->waitProcs.size;
		nWaitOrderProcs += lock->waitProcs.size;
		Assert(nWaitOrderProcs <= MaxBackends);

		/*
		 * Do the topo sort.  TopoSort need not examine constraints after this
		 * one, since they must be for different locks.
		 */
		if (!TopoSort(lock, constraints, i + 1,
					  waitOrders[nWaitOrders].procs))
			return false;
		nWaitOrders++;
	}
	return true;
}


/*
 * TopoSort -- topological sort of a wait queue
 *
 * Generate a re-ordering of a lock's wait queue that satisfies given
 * constraints about certain procs preceding others.  (Each such constraint
 * is a fact of a partial ordering.)  Minimize rearrangement of the queue
 * not needed to achieve the partial ordering.
 *
 * This is a lot simpler and slower than, for example, the topological sort
 * algorithm shown in Knuth's Volume 1.  However, Knuth's method doesn't
 * try to minimize the damage to the existing order.  In practice we are
 * not likely to be working with more than a few constraints, so the apparent
 * slowness of the algorithm won't really matter.
 *
 * The initial queue ordering is taken directly from the lock's wait queue.
 * The output is an array of PGPROC pointers, of length equal to the lock's
 * wait queue length (the caller is responsible for providing this space).
 * The partial order is specified by an array of EDGE structs.  Each EDGE
 * is one that we need to reverse, therefore the "waiter" must appear before
 * the "blocker" in the output array.  The EDGE array may well contain
 * edges associated with other locks; these should be ignored.
 *
 * Returns true if able to build an ordering that satisfies all the
 * constraints, false if not (there are contradictory constraints).
 *
 * K.Suzuki: この関数に渡す Edge には external lock が含まれないように保証しておく必要がある。
 */
static bool
TopoSort(LOCK *lock,
		 EDGE *constraints,
		 int nConstraints,
		 PGPROC **ordering)		/* output argument */
{
	PROC_QUEUE *waitQueue = &(lock->waitProcs);
	int			queue_size = waitQueue->size;
	PGPROC	   *proc;
	int			i,
				j,
				jj,
				k,
				kk,
				last;

	/* First, fill topoProcs[] array with the procs in their current order */
	proc = (PGPROC *) waitQueue->links.next;
	for (i = 0; i < queue_size; i++)
	{
		topoProcs[i] = proc;
		proc = (PGPROC *) proc->links.next;
	}

	/*
	 * Scan the constraints, and for each proc in the array, generate a count
	 * of the number of constraints that say it must be before something else,
	 * plus a list of the constraints that say it must be after something
	 * else. The count for the j'th proc is stored in beforeConstraints[j],
	 * and the head of its list in afterConstraints[j].  Each constraint
	 * stores its list link in constraints[i].link (note any constraint will
	 * be in just one list). The array index for the before-proc of the i'th
	 * constraint is remembered in constraints[i].pred.
	 *
	 * Note that it's not necessarily the case that every constraint affects
	 * this particular wait queue.  Prior to group locking, a process could be
	 * waiting for at most one lock.  But a lock group can be waiting for
	 * zero, one, or multiple locks.  Since topoProcs[] is an array of the
	 * processes actually waiting, while constraints[] is an array of group
	 * leaders, we've got to scan through topoProcs[] for each constraint,
	 * checking whether both a waiter and a blocker for that group are
	 * present.  If so, the constraint is relevant to this wait queue; if not,
	 * it isn't.
	 */
	MemSet(beforeConstraints, 0, queue_size * sizeof(int));
	MemSet(afterConstraints, 0, queue_size * sizeof(int));
	for (i = 0; i < nConstraints; i++)
	{
		/*
		 * Find a representative process that is on the lock queue and part of
		 * the waiting lock group.  This may or may not be the leader, which
		 * may or may not be waiting at all.  If there are any other processes
		 * in the same lock group on the queue, set their number of
		 * beforeConstraints to -1 to indicate that they should be emitted
		 * with their groupmates rather than considered separately.
		 *
		 * In this loop and the similar one just below, it's critical that we
		 * consistently select the same representative member of any one lock
		 * group, so that all the constraints are associated with the same
		 * proc, and the -1's are only associated with not-representative
		 * members.  We select the last one in the topoProcs array.
		 */
		proc = constraints[i].waiter;
		Assert(proc != NULL);
		jj = -1;
		for (j = queue_size; --j >= 0;)
		{
			PGPROC	   *waiter = topoProcs[j];

			if (waiter == proc || waiter->lockGroupLeader == proc)
			{
				Assert(waiter->waitLock == lock);
				if (jj == -1)
					jj = j;
				else
				{
					Assert(beforeConstraints[j] <= 0);
					beforeConstraints[j] = -1;
				}
			}
		}

		/* If no matching waiter, constraint is not relevant to this lock. */
		if (jj < 0)
			continue;

		/*
		 * Similarly, find a representative process that is on the lock queue
		 * and waiting for the blocking lock group.  Again, this could be the
		 * leader but does not need to be.
		 */
		proc = constraints[i].blocker;
		Assert(proc != NULL);
		kk = -1;
		for (k = queue_size; --k >= 0;)
		{
			PGPROC	   *blocker = topoProcs[k];

			if (blocker == proc || blocker->lockGroupLeader == proc)
			{
				Assert(blocker->waitLock == lock);
				if (kk == -1)
					kk = k;
				else
				{
					Assert(beforeConstraints[k] <= 0);
					beforeConstraints[k] = -1;
				}
			}
		}

		/* If no matching blocker, constraint is not relevant to this lock. */
		if (kk < 0)
			continue;

		Assert(beforeConstraints[jj] >= 0);
		beforeConstraints[jj]++;	/* waiter must come before */
		/* add this constraint to list of after-constraints for blocker */
		constraints[i].pred = jj;
		constraints[i].link = afterConstraints[kk];
		afterConstraints[kk] = i + 1;
	}

	/*--------------------
	 * Now scan the topoProcs array backwards.  At each step, output the
	 * last proc that has no remaining before-constraints plus any other
	 * members of the same lock group; then decrease the beforeConstraints
	 * count of each of the procs it was constrained against.
	 * i = index of ordering[] entry we want to output this time
	 * j = search index for topoProcs[]
	 * k = temp for scanning constraint list for proc j
	 * last = last non-null index in topoProcs (avoid redundant searches)
	 *--------------------
	 */
	last = queue_size - 1;
	for (i = queue_size - 1; i >= 0;)
	{
		int			c;
		int			nmatches = 0;

		/* Find next candidate to output */
		while (topoProcs[last] == NULL)
			last--;
		for (j = last; j >= 0; j--)
		{
			if (topoProcs[j] != NULL && beforeConstraints[j] == 0)
				break;
		}

		/* If no available candidate, topological sort fails */
		if (j < 0)
			return false;

		/*
		 * Output everything in the lock group.  There's no point in
		 * outputting an ordering where members of the same lock group are not
		 * consecutive on the wait queue: if some other waiter is between two
		 * requests that belong to the same group, then either it conflicts
		 * with both of them and is certainly not a solution; or it conflicts
		 * with at most one of them and is thus isomorphic to an ordering
		 * where the group members are consecutive.
		 */
		proc = topoProcs[j];
		if (proc->lockGroupLeader != NULL)
			proc = proc->lockGroupLeader;
		Assert(proc != NULL);
		for (c = 0; c <= last; ++c)
		{
			if (topoProcs[c] == proc || (topoProcs[c] != NULL &&
										 topoProcs[c]->lockGroupLeader == proc))
			{
				ordering[i - nmatches] = topoProcs[c];
				topoProcs[c] = NULL;
				++nmatches;
			}
		}
		Assert(nmatches > 0);
		i -= nmatches;

		/* Update beforeConstraints counts of its predecessors */
		for (k = afterConstraints[j]; k > 0; k = constraints[k - 1].link)
			beforeConstraints[constraints[k - 1].pred]--;
	}

	/* Done */
	return true;
}

#ifdef DEBUG_DEADLOCK
static void
PrintLockQueue(LOCK *lock, const char *info)
{
	PROC_QUEUE *waitQueue = &(lock->waitProcs);
	int			queue_size = waitQueue->size;
	PGPROC	   *proc;
	int			i;

	printf("%s lock %p queue ", info, lock);
	proc = (PGPROC *) waitQueue->links.next;
	for (i = 0; i < queue_size; i++)
	{
		printf(" %d", proc->pid);
		proc = (PGPROC *) proc->links.next;
	}
	printf("\n");
	fflush(stdout);
}
#endif

/*
 * Report a detected deadlock, with available details.
 *
 * K.Suzuki: EXTERNAL LOCK の追加も行うこと
 */
void
DeadLockReport(void)
{
	StringInfoData clientbuf;	/* errdetail for client */
	StringInfoData logbuf;		/* errdetail for server log */
	StringInfoData locktagbuf;
	int			i;

	initStringInfo(&clientbuf);
	initStringInfo(&logbuf);
	initStringInfo(&locktagbuf);

	/* Generate the "waits for" lines sent to the client */
	for (i = 0; i < nDeadlockDetails; i++)
	{
		DEADLOCK_INFO *info = &deadlockDetails[i];
		int			nextpid;

		/* The last proc waits for the first one... */
		if (i < nDeadlockDetails - 1)
			nextpid = info[1].pid;
		else
			nextpid = deadlockDetails[0].pid;

		/* reset locktagbuf to hold next object description */
		resetStringInfo(&locktagbuf);

		DescribeLockTag(&locktagbuf, &info->locktag);

		if (i > 0)
			appendStringInfoChar(&clientbuf, '\n');

		appendStringInfo(&clientbuf,
						 _("Process %d waits for %s on %s; blocked by process %d."),
						 info->pid,
						 GetLockmodeName(info->locktag.locktag_lockmethodid,
										 info->lockmode),
						 locktagbuf.data,
						 nextpid);
	}

	/* Duplicate all the above for the server ... */
	appendStringInfoString(&logbuf, clientbuf.data);

	/* ... and add info about query strings */
	for (i = 0; i < nDeadlockDetails; i++)
	{
		DEADLOCK_INFO *info = &deadlockDetails[i];

		appendStringInfoChar(&logbuf, '\n');

		appendStringInfo(&logbuf,
						 _("Process %d: %s"),
						 info->pid,
						 pgstat_get_backend_current_activity(info->pid, false));
	}

	pgstat_report_deadlock();

	ereport(ERROR,
			(errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
			 errmsg("deadlock detected"),
			 errdetail_internal("%s", clientbuf.data),
			 errdetail_log("%s", logbuf.data),
			 errhint("See server log for query details.")));
}

/*
 * RememberSimpleDeadLock: set up info for DeadLockReport when ProcSleep
 * detects a trivial (two-way) deadlock.  proc1 wants to block for lockmode
 * on lock, but proc2 is already waiting and would be blocked by proc1.
 *
 * K.Suzuki: この関数は EXTERNAL LOCK 対応不要なように思われる ... また後でチェックしてみる。
 */
void
RememberSimpleDeadLock(PGPROC *proc1,
					   LOCKMODE lockmode,
					   LOCK *lock,
					   PGPROC *proc2)
{
	DEADLOCK_INFO *info = &deadlockDetails[0];

	info->locktag = lock->tag;
	info->lockmode = lockmode;
	info->pid = proc1->pid;
	info++;
	info->locktag = proc2->waitLock->tag;
	info->lockmode = proc2->waitLockMode;
	info->pid = proc2->pid;
	nDeadlockDetails = 2;
}

/*
 * Build LOCAL_WFG from DADLOCK_INFO_BUP.   If Origin flag is specified,
 * then backend information of originating process will be added too.
 *
 * Resultant LOCAL_WFG will be used to build GLOBAL_WFG to be sent to
 * remote backend for global deadlock check.
 */
static LOCAL_WFG *
BuildLocalWfG(PGPROC *origin, DEADLOCK_INFO_BUP *info)
{
	int			 ii;
	LOCAL_WFG	*local_wfg;

	local_wfg = (LOCAL_WFG *)palloc0(sizeof(LOCAL_WFG));
	local_wfg->local_wfg_magic = WfG_LOCAL_MAGIC;
	if (deadlockDetails[nDeadlockDetails - 1].locktag.locktag_type == LOCKTAG_EXTERNAL)
		local_wfg->local_wfg_flag |= WfG_HAS_EXTERNAL_LOCK;
	local_wfg->database_system_identifier = get_database_system_id();
	if (origin)
	{
		Assert(nVisitedProcs > 0);
		local_wfg->local_wfg_flag |= WfG_HAS_VISITED_PROC;
		local_wfg->visitedProcPid = visitedProcs[0]->pid;
		local_wfg->visitedProcPgprocno = visitedProcs[0]->pgprocno;
		local_wfg->visitedProcLxid = visitedProcs[0]->lxid;
	}
	Assert(nDeadlockDetails > 0);
	local_wfg->nDeadlockInfo = info->nDeadlock_info;
	local_wfg->deadlock_info = (DEADLOCK_INFO *)palloc(sizeof(DEADLOCK_INFO) * info->nDeadlock_info);
	for (ii = 0; ii < info->nDeadlock_info; ii++)
	{
		memcpy(&local_wfg->deadlock_info[ii], &info->deadlock_info[ii], sizeof(DEADLOCK_INFO));
	}
	if (local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
		local_wfg->external_lock = GetExternalLockProperties(&deadlockDetails[info->nDeadlock_info -1].locktag);
	else
		local_wfg->external_lock = NULL;
	local_wfg->local_wfg_trailor = WfG_LOCAL_MAGIC;

	return local_wfg;
}

/*
 * Tur LOCAL_WFG into stream.   The stream is not in text.   It's binary format and just serialized.
 * This should then be converted into hexa-decimal array.  This function also takes care of endian.
 */
static StringInfo
SerializeLocalWfG(LOCAL_WFG *local_wfg)
{
	int				ii;
	StringInfo		str;

	str = makeStringInfo();

	/* Total length (dummy) */

	appendBinaryStringInfoInt32(str, 0);

	/* Local magic */
	appendBinaryStringInfoInt32(str, local_wfg->local_wfg_magic);

	/* Database system identifier */

	appendBinaryStringInfoInt64(str, local_wfg->database_system_identifier);

	/* Flag */
	appendBinaryStringInfoInt32(str, local_wfg->local_wfg_flag);

	/* visitedProcs */
	if (local_wfg->local_wfg_flag & WfG_HAS_VISITED_PROC)
	{
		appendBinaryStringInfoInt32(str, local_wfg->visitedProcPid);
		appendBinaryStringInfoInt32(str, local_wfg->visitedProcPgprocno);
		appendBinaryStringInfoInt32(str, local_wfg->visitedProcLxid);
	}
	/* Deadlock Info number */
	appendBinaryStringInfoInt32(str, local_wfg->nDeadlockInfo);

	/* Each deadlock info */
	for (ii = 0; ii < local_wfg->nDeadlockInfo; ii++)
	{
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].locktag.locktag_field1);
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].locktag.locktag_field2);
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].locktag.locktag_field3);
		appendBinaryStringInfoInt16(str, local_wfg->deadlock_info[ii].locktag.locktag_field4);
		appendBinaryStringInfoInt8(str, local_wfg->deadlock_info[ii].locktag.locktag_type);
		appendBinaryStringInfoInt8(str, local_wfg->deadlock_info[ii].locktag.locktag_lockmethodid);
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].lockmode);
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].pid);
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].pgprocno);
		appendBinaryStringInfoInt32(str, local_wfg->deadlock_info[ii].txid);
	}

	/* External lock */
	if (local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
	{
		ExternalLockInfo	*eLockInfo = local_wfg->external_lock;
		char				*dsn;
		int					 dsnlen;

		appendBinaryStringInfoInt32(str, eLockInfo->pid);
		appendBinaryStringInfoInt32(str, eLockInfo->pgprocno);
		appendBinaryStringInfoInt32(str, eLockInfo->txnid);
		appendBinaryStringInfoInt32(str, eLockInfo->serno);
		appendBinaryStringInfoInt32(str, eLockInfo->target_pid);
		appendBinaryStringInfoInt32(str, eLockInfo->target_pgprocno);
		appendBinaryStringInfoInt32(str, eLockInfo->target_txn);
		dsnlen = ((strlen(eLockInfo->dsn) + 4)/4) * 4;
		dsn = (char *)palloc0(dsnlen);
		strncpy(dsn, eLockInfo->dsn, dsnlen);
		appendBinaryStringInfoInt32(str, dsnlen);
		appendBinaryStringInfo(str, dsn, dsnlen);
		
		/* Append External Lock info to WfG */

		pfree(dsn);
	}

	/* Trailor: Local magic */
	appendBinaryStringInfoInt32(str, local_wfg->local_wfg_trailor);

	/* Length */
	replaceStringInt32(str->data, str->len);

	return str;
}

/*
 * Translate binary stream of serialized LOCAL_WFG into LOCAL_WFG structure.
 */
static LOCAL_WFG *
DeserializeLocalWfG(char *buf)
{
	LOCAL_WFG	*wfg;
	int32		 len_wfg;
	int32		 remaining;
	int32		 ii;

	wfg = (LOCAL_WFG *)palloc0(sizeof(LOCAL_WFG));
	Extract32(buf, &len_wfg);
	remaining = len_wfg;
	/* Length after data length until nDeadlockInfo */
	checklen(remaining, 4 + 8 + 4 + 4 + 4, err);
	Extract32(buf, &wfg->local_wfg_magic);
	if (wfg->local_wfg_magic != WfG_LOCAL_MAGIC)
		goto err;
	Extract64(buf, &wfg->database_system_identifier);
	Extract32(buf, &wfg->local_wfg_flag);

	/* visitedProc */
	if (wfg->local_wfg_flag & WfG_HAS_VISITED_PROC)
	{
		Extract32(buf, &wfg->visitedProcPid);
		Extract32(buf, &wfg->visitedProcPgprocno);
		Extract32(buf, &wfg->visitedProcLxid);
	}

	Extract32(buf, &wfg->nDeadlockInfo);

	/* Allocate deadlock info */

	wfg->deadlock_info = (DEADLOCK_INFO *)palloc(sizeof(DEADLOCK_INFO) * wfg->nDeadlockInfo);
	checklen(remaining, 32 * wfg->nDeadlockInfo, err);
	for (ii = 0; ii < wfg->nDeadlockInfo; ii++)
	{
		/* Extract deadlock info */

		DEADLOCK_INFO *deadlock_info = &wfg->deadlock_info[ii];

		ExtractU32(buf, &deadlock_info->locktag.locktag_field1);
		ExtractU32(buf, &deadlock_info->locktag.locktag_field2);
		ExtractU32(buf, &deadlock_info->locktag.locktag_field3);
		ExtractU16(buf, &deadlock_info->locktag.locktag_field4);
		ExtractU8(buf, &deadlock_info->locktag.locktag_type);
		ExtractU8(buf, &deadlock_info->locktag.locktag_lockmethodid);
		Extract32(buf, &deadlock_info->lockmode);
		Extract32(buf, &deadlock_info->pid);
		Extract32(buf, &deadlock_info->pgprocno);
		ExtractU32(buf, &deadlock_info->txid);
	}
	if (wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
	{
		int32	dsnlen;
		ExternalLockInfo	*ext_lock;

		wfg->external_lock = ext_lock = (ExternalLockInfo *)palloc0(sizeof(ExternalLockInfo));
		checklen(remaining, 4 * 8, err);
		Extract32(buf, &ext_lock->pid);
		Extract32(buf, &ext_lock->pgprocno);
		ExtractU32(buf, &ext_lock->txnid);
		Extract32(buf, &ext_lock->serno);
		Extract32(buf, &ext_lock->target_pid);
		Extract32(buf, &ext_lock->target_pgprocno);
		ExtractU32(buf, &ext_lock->target_txn);
		Extract32(buf, &dsnlen);

		checklen(remaining, dsnlen, err);
		ext_lock->dsn = pstrdup(buf);
		buf += dsnlen;
	}
	checklen(remaining, 4, err);
	Extract32(buf, &wfg->local_wfg_trailor);
	if (wfg->local_wfg_trailor != WfG_LOCAL_MAGIC)
		goto err;
	return wfg;

err:
	if (wfg)
	{
		if (wfg->deadlock_info)
			pfree(wfg->deadlock_info);
		if (wfg->external_lock)
			FreeExternalLockProperties(wfg->external_lock);
		pfree(wfg);
	}
	return NULL;
}


/*
 * We use database system id to identify database instance.   This is created by initdb and does not change throughout
 * the database lifetime.   This is essentially the timestan when initdb created the database materials plus process id
 * of initdb.   We can assume this is unique enough throughout correction of databases in the scope.
 */
uint64
get_database_system_id(void)
{
	ControlFileData *controlfiledata;
	uint64			 system_identifier;
	bool			 crc_ok;

	controlfiledata = get_controlfile(DataDir, &crc_ok);
	system_identifier = controlfiledata->system_identifier;
	pfree(controlfiledata);
	return system_identifier;
}

/*
 * For test functions.
 */
const char *
locktagTypeName(LockTagType type)
{
	switch(type)
	{
		case LOCKTAG_RELATION:
			return "LOCKTAG_RELATION";
		case LOCKTAG_RELATION_EXTEND:
			return "LOCKTAG_RELATION_EXTEND";
		case LOCKTAG_PAGE:
			return "LOCKTAG_PAGE";
		case LOCKTAG_TUPLE:
			return "LOCKTAG_TUPLE";
		case LOCKTAG_TRANSACTION:
			return "LOCKTAG_TRANSACTION";
		case LOCKTAG_VIRTUALTRANSACTION:
			return "LOCKTAG_VIRTUALTRANSACTION";
		case LOCKTAG_SPECULATIVE_TOKEN:
			return "LOCKTAG_SPECULATIVE_TOKEN";
		case LOCKTAG_OBJECT:
			return "LOCKTAG_OBJECT";
		case LOCKTAG_USERLOCK:
			return "LOCKTAG_USERLOCK";
		case LOCKTAG_EXTERNAL:
			return "LOCKTAG_EXTERNAL";
		case LOCKTAG_ADVISORY:
			return "LOCKTAG_ADVISORY";
	}
	return "LOCKTAG_NO_SUCH_TYPE";
}

static GLOBAL_WFG *
addToGlobalWfg_int(GLOBAL_WFG *g_wfg, void *data, bool is_text, int size)
{
	Assert(data);

	if (g_wfg == NULL)
	{
		g_wfg = (GLOBAL_WFG *)palloc(sizeof(GLOBAL_WFG));
		g_wfg->global_wfg_magic = WfG_GLOBAL_MAGIC;
		g_wfg->nLocalWfg = 1;
		g_wfg->local_wfg = (void **)palloc(sizeof(void *));
		g_wfg->local_wfg[0] = data;
		g_wfg->is_text = (bool *)palloc(sizeof(bool));
		g_wfg->is_text[0] = is_text;
		g_wfg->txtsize = (int32 *)palloc(sizeof(int32));
		g_wfg->txtsize[0] = size;
		g_wfg->global_wfg_trailor = WfG_GLOBAL_MAGIC;
	}
	else
	{
		g_wfg->nLocalWfg++;
		g_wfg->local_wfg = (void **)repalloc(g_wfg->local_wfg, sizeof(void *) * g_wfg->nLocalWfg);
		g_wfg->local_wfg[g_wfg->nLocalWfg - 1] = data;
		g_wfg->is_text = (bool *)repalloc(g_wfg->is_text, sizeof(bool) * g_wfg->nLocalWfg);
		g_wfg->is_text[g_wfg->nLocalWfg - 1] = is_text;
		g_wfg->txtsize = (int32 *)repalloc(g_wfg->txtsize, sizeof(int32) * g_wfg->nLocalWfg);
		g_wfg->txtsize[g_wfg->nLocalWfg - 1] = size;
	}
	return g_wfg;
}

/*
 * Shoftcut when local_wfg is not in the text format.
 */
static GLOBAL_WFG *
AddToGlobalWfG(GLOBAL_WFG *g_wfg, LOCAL_WFG *local_wfg)
{
	return addToGlobalWfg_int(g_wfg, (void *)local_wfg, false, -1);
}

#if 0
GLOBAL_WFG *
AddToGlobalWfG_Stream(GLOBAL_WFG *g_wfg, char *local_wfg_s, int32 size)
{
	return addToGlobalWfg_int(g_wfg, (void *)local_wfg_s, true, size);
}
#endif

/*
 * Translate GLOBAL_WFG into binary stream.
 */
static StringInfo
SerializeGlobalWfG(GLOBAL_WFG *g_wfg)
{
	int			ii;
	StringInfo	str;

	str = makeStringInfo();

	/* Total Length (dummy) */

	appendBinaryStringInfoInt32(str, 0);

	/* Global Magic */
	appendBinaryStringInfoInt32(str, g_wfg->global_wfg_magic);

	/* Num of local WfG */
	appendBinaryStringInfoInt32(str, g_wfg->nLocalWfg);

	/* Local WfG */
	for (ii = 0; ii < g_wfg->nLocalWfg; ii++)
	{
		if (g_wfg->is_text[ii])
			appendBinaryStringInfo(str, g_wfg->local_wfg[ii], g_wfg->txtsize[ii]);
		else
		{
			StringInfo	local_wfg;

			if (!(local_wfg = SerializeLocalWfG((LOCAL_WFG *)g_wfg->local_wfg[ii])))
				elog(ERROR, "Local WfG serialization error.");
			appendBinaryStringInfo(str, local_wfg->data, local_wfg->len);
			if (local_wfg->data)
				pfree(local_wfg->data);
		}
	}

	/* Trailor */
	appendBinaryStringInfoInt32(str, g_wfg->global_wfg_trailor);

	/* Total Length */
	replaceStringInt32(str->data, str->len);

	return str;

}

/*
 * Translate binary stream global wait-for-graph into GLOBAL_WFG structure.
 */
static GLOBAL_WFG *
DeserializeGlobalWfG(char *buf)
{
	int			 ii;
	GLOBAL_WFG	*g_wfg;
	int32		 size;

	g_wfg = (GLOBAL_WFG *)palloc(sizeof(GLOBAL_WFG));

	/* Size */
	Extract32(buf, &size);
	if (size < 0)
		goto err;
	Extract32(buf, &g_wfg->global_wfg_magic);
	/* Magic */
	if (g_wfg->global_wfg_magic != WfG_GLOBAL_MAGIC)
		goto err;
	/* Local WFG num */
	Extract32(buf, &g_wfg->nLocalWfg);
	if (g_wfg->nLocalWfg < 0)
		goto err;
	g_wfg->is_text = (bool *)palloc(sizeof(bool) * g_wfg->nLocalWfg);
	g_wfg->txtsize = (int32 *)palloc(sizeof(int32) * g_wfg->nLocalWfg);
	g_wfg->local_wfg = (void **)palloc(sizeof(void *) * g_wfg->nLocalWfg);
	for (ii = 0; ii < g_wfg->nLocalWfg; ii++)
	{
		int32		 ss;
		LOCAL_WFG	*local_wfg;

		(void)extractInt32(buf, &ss);
		local_wfg = DeserializeLocalWfG(buf);
		if (local_wfg)
		{
			g_wfg->is_text[ii] = false;
			g_wfg->txtsize[ii] = -1;
			g_wfg->local_wfg[ii] = (void *)local_wfg;
		}
		else
		{
			g_wfg->is_text[ii] = true;
			g_wfg->txtsize[ii] = ss;
			g_wfg->local_wfg[ii] = (void *)palloc(ss);
			memcpy(g_wfg->local_wfg[ii], buf, ss);
		}
		buf += ss;
	}
	Extract32(buf, &g_wfg->global_wfg_trailor);

	return g_wfg;
err:
	if (g_wfg)
	{
		if (g_wfg->is_text)
			pfree(g_wfg->is_text);
		if (g_wfg->txtsize)
			pfree(g_wfg->txtsize);
		if (g_wfg->local_wfg)
			pfree(g_wfg->local_wfg);
		pfree(g_wfg);
	}
	return NULL;
}


/*
 * K.Suzuki
 *
 * Utility functions for serialization and deserialization of global/local WfG
 */
static void
appendBinaryStringInfoInt64(StringInfo str, int64 value)
{
	char buf[8];

	buf[0] = ((value & 0xff00000000000000) >> 56) & 0x00000000000000ff;
	buf[1] = (value & 0x00ff000000000000) >> 48;
	buf[2] = (value & 0x0000ff0000000000) >> 40;
	buf[3] = (value & 0x000000ff00000000) >> 32;
	buf[4] = (value & 0x00000000ff000000) >> 24;
	buf[5] = (value & 0x0000000000ff0000) >> 16;
	buf[6] = (value & 0x000000000000ff00) >> 8;
	buf[7] =  value & 0x00000000000000ff;

	appendBinaryStringInfo(str, buf, 8);
}

static void
appendBinaryStringInfoInt32(StringInfo str, int32 value)
{
	char buf[4];

	buf[0] = ((value & 0xff000000) >> 24) & 0x000000ff;
	buf[1] = (value & 0x00ff0000) >> 16;
	buf[2] = (value & 0x0000ff00) >> 8;
	buf[3] =  value & 0x000000ff;

	appendBinaryStringInfo(str, buf, 4);
}

static void
appendBinaryStringInfoInt16(StringInfo str, int16 value)
{
	char buf[2];

	buf[0] = (value >> 8) & 0x000000ff;
	buf[1] = value & 0x000000ff;

	appendBinaryStringInfo(str, buf, 2);
}

static void
appendBinaryStringInfoInt8(StringInfo str, int8 value)
{
	char buf;

	buf = value;

	appendBinaryStringInfo(str, &buf, 1);
}

static void
replaceStringInt32(char *s, int32 value)
{
	s[0] = ((value & 0xff000000) >> 24) & 0x000000ff;
	s[1] = (value & 0x00ff0000) >> 16;
	s[2] = (value & 0x0000ff00) >> 8;
	s[3] = value & 0x000000ff;
}

#if 0
static void
replaceStringInt16(char *s, int16 value)
{
	s[0] = (value >> 8);
	s[1] = value & 0x000000ff;
}
#endif

static char *
extractInt64(char *buf, int64 *value)
{

	*value  = ((int64)(*buf++) << 56) & 0xff00000000000000;
	*value |= ((int64)(*buf++) << 48) & 0x00ff000000000000;
	*value |= ((int64)(*buf++) << 40) & 0x0000ff0000000000;
	*value |= ((int64)(*buf++) << 32) & 0x000000ff00000000;
	*value |= ((int64)(*buf++) << 24) & 0x00000000ff000000;
	*value |= ((int64)(*buf++) << 16) & 0x0000000000ff0000;
	*value |= ((int64)(*buf++) << 8)  & 0x000000000000ff00;
	*value |= ((int64)(*buf++))       & 0x00000000000000ff;
	return buf;
}

#if 0
static char *
extractUint64(char *buf, uint64 *value)
{

	*value  = (uint64)(*buf++) << 56;
	*value |= (uint64)(*buf++) << 48;
	*value |= (uint64)(*buf++) << 40;
	*value |= (uint64)(*buf++) << 32;
	*value |= (uint64)(*buf++) << 24;
	*value |= (uint64)(*buf++) << 16;
	*value |= (uint64)(*buf++) << 8;
	*value |= (uint64)(*buf++);
	return buf;
}
#endif

static char *
extractInt32(char *buf, int32 *value)
{

	*value  = ((int32)(*buf++) << 24) & 0xff000000;
	*value |= ((int32)(*buf++) << 16) & 0x00ff0000;
	*value |= ((int32)(*buf++) << 8)  & 0x0000ff00;
	*value |= (int32)(*buf++)         & 0x000000ff;
	return buf;
}

static char *
extractUint32(char *buf, uint32 *value)
{

	*value  = ((uint32)(*buf++) << 24) & 0xff000000;
	*value |= ((uint32)(*buf++) << 16) & 0x00ff0000;
	*value |= ((uint32)(*buf++) << 8)  & 0x0000ff00;
	*value |= ((uint32)(*buf++))       & 0x000000ff;
	return buf;
}

#if 0
static char *
extractInt16(char *buf, int16 *value)
{

	*value = (int16)(*buf++) << 8;
	*value |= (int16)(*buf++);
	return buf;
}
#endif

static char *
extractUint16(char *buf, uint16 *value)
{

	*value  = ((uint16)(*buf++) << 8) & 0xff00;
	*value |= ((uint16)(*buf++))      & 0x00ff;
	return buf;
}

#if 0
static char *
extractInt8(char *buf, int8 *value)
{

	*value = (int32)(*buf++);
	return buf;
}
#endif

static char *
extractUint8(char *buf, uint8 *value)
{

	*value = (uint8)(*buf++);
	return buf;
}
/*
 *	K.Suzuki
 */

static char *
binary2text(char *input, int size)
{
	const char 	*hexadata = "0123456789abcdef";
	char		*output = (char *)palloc(size * 2 + 1);
	char		*o;
	char		*ic;
	int		 	 ii;

	o = output;
	output[size*2] = '\0';


	for (ii = 0, ic = input; ii < size; ii++, ic++)
	{
		int	cc;

		cc = *ic;
		*o++ = hexadata[ (cc & 0x000000ff) >> 4 ];
		*o++ = hexadata[  cc & 0x0000000f ];
	}
	return output;
}

static char *
hexa2bin(char *input, int insize, int *size)
{
	char	*output, *wk;
	int		 ll;
	char	 cc;

	if (insize <= 0)
		insize = strlen(input);
	if (insize % 2)
		return NULL;
	output = wk = (char *)palloc(insize/2);
	*size = insize/2;
	for (ll = *size; ll > 0; ll--)
	{
		if (*input >= '0' && *input <= '9')
			cc = *input - '0';
		else if (*input >= 'A' && *input <= 'F')
			cc = *input - 'A' + 10;
		else if (*input >= 'a' && *input <= 'f')
			cc = *input - 'a' + 10;
		else
			goto err;
		cc = cc << 4;
		input++;
		if (*input >= '0' && *input <= '9')
			cc |= *input - '0';
		else if (*input >= 'A' && *input <= 'F')
			cc |= *input - 'A' + 10;
		else if (*input >= 'a' && *input <= 'f')
			cc |= *input - 'a' + 10;
		else
			goto err;
		input++;
		*wk++ = cc;
	}
	return output;

err:
	if (output)
		pfree(output);
	return NULL;
}

/*
 * K.Suzuki:
 *
 * Entry point of global deadlock check.
 * Called just after DeadLockCheck() from proc.c.
 *
 * Be aware that this function does not assume the caller has acquired lock line
 * locks.
 *
 */
/*
 * Entry point from local proc.c.   We don't have global_wfg or returning_wfg entry
 * yet.
 */
DeadLockState
GlobalDeadlockCheck(PGPROC *proc)
{
	return GlobalDeadlockCheck_int(proc, NULL, NULL);
}

/*
 * This is the latter part of global deadlock check.
 *
 * At first, if a backend cannot acquire a LOCK within deadlock timeout, proc.c calls
 * DeadLockCheck().   If DeadLockCheck() finds local deadlock starting at the backend,
 * there are no difference from older version of DeadLockCheck().  Caller of DeadLockCheck()
 * need to acqire LWLock for all the lock lines for LOCK objects.
 *
 * After then, DeadLockCheck() may return DS_DEADLOCK_INFO.   This is an addition to the
 * return value.   When proc.c receives this return value, it suggests that wait-for-graph
 * exntends to remote transaction.
 *
 * To check wait-for-graph expanding to remote transaction, the caller must call GlobalDeadlockCheck().
 * Please note that the caller can release all the locklines.
 *
 * GlobalDeadlockCheck_int() is the bod of GLobalDeadlockCheck() called in the following wait-for-graph
 * check at remote databases.
 */
static DeadLockState
GlobalDeadlockCheck_int(PGPROC *proc, GLOBAL_WFG *global_wfg, RETURNED_WFG **rv)
{
	DeadLockState	 state;
	int				 nWfG;
	LOCAL_WFG		*local_wfg;
	DEADLOCK_INFO_BUP	*curBup;
	RETURNED_WFG	*returned_wfg = NULL;
	RETURNED_WFG	*returning_wfg;
	int				 ii;

	globalVisitedProcs = NULL;
	nGlobalVisitedProcs = 0;
	returning_wfg = NULL;

	/*
	 * Previous DeadLockCheck() may have found more than one possible piece of wait-for-graph
	 * spanning to remote transactions.
	 *
	 * We need to check all these possible wait-for-graph.
	 */
	for (curBup = deadlock_info_bup_head; curBup; curBup = curBup->next)
	{
		local_wfg = BuildLocalWfG(proc, curBup);
		if (!local_wfg || !check_local_wfg_is_stable(local_wfg))
			continue;
		nWfG = GlobalDeadlockCheckRemote(local_wfg, global_wfg, &returned_wfg);
		if (nWfG == 0)
			continue;
		if (nWfG < 0)
			elog(ERROR, "Error in remote wait-for-graph check.\n");
		if (deadlockCheckMode == DLCMODE_LOCAL)
		{
			/*
			 * If we are at the beginning database (ORIGIN), we need to recheck if the observerd
			 * wait-for-graph is static.   If not, it's not a part of a deadlock.
			 */
			for (ii = 0; ii < returned_wfg->nReturnedWfg; ii++)
			{
				ExternalLockInfo	*external_lock_info;
				LOCKTAG				*external_locktag;
				DeadLockState	 	 state2;

				external_locktag = &(curBup->deadlock_info[curBup->nDeadlock_info - 1].locktag);
				external_lock_info = GetExternalLockProperties(external_locktag);
				if (external_lock_info == NULL)
					elog(ERROR, "Inconsisitent internal status in global lock detection.");
				state2 = GlobalDeadlockRecheckRemote(0, returned_wfg->global_wfg_in_text[ii], external_lock_info);
				if (state2 == DS_HARD_DEADLOCK || state2 == DS_GLOBAL_ERROR)
				{
					/*
					 * K.Suzuki:
					 *
					 * Should we exit the loop when an error occurred?   We may be able to continue
					 * rechecking remaining condidates.
					 */
					free_returned_wfg(returned_wfg);
					state = state2;
					goto returning;
				}
			}
		}
		else
			/*
			 * If we are not at the begging database (ORIGIN), we return all the possible wait-for-graph
			 * for candidate deadlock to upstream.
			 */
			returning_wfg = add_returned_wfg(returning_wfg, returned_wfg, true);
	}
	if (deadlockCheckMode == DLCMODE_LOCAL)
		state = DS_NO_DEADLOCK;
	else
		state = returning_wfg ? DS_DEADLOCK_INFO : DS_NO_DEADLOCK;

returning:
	/* Cleanup global deadlock check object local to this database */
	if (globalVisitedProcs)
		pfree(globalVisitedProcs);
	nGlobalVisitedProcs = 0;
	globalVisitedProcs = NULL;

	if (globalVisitedExternalLock)
		pfree(globalVisitedExternalLock);
	nGlobalVisitedExternalLock = 0;
	globalVisitedExternalLock = NULL;

	clean_deadlock_info_bup();

	if (rv)
		*rv = returning_wfg;
	return state;
}


/*
 * K.Suzuki: 20200625
 *
 * この関数は、複数の可能なパスを返す可能性があることに注意。これをサポートするように
 * 書き換える必要がある。
 */

/*
 ********************************************************************************************
 *
 * K.Suzuki: Global deadlock detection part
 *
 ********************************************************************************************
 */

/*
 * Called from GlobalDeadlockCheck_int() and takes care of checking wait-for-graph at
 * downstream.
 *
 * If This is the start point to detect global deadlock, specify NULL to global_wfg.
 * Otherwise, specify received global wfg as global_wfg.
 *
 * The caller must check that local_wfg is stable and it terminates with LOCKTAG_EXTERNAL
 */
#if 1
int				 gdd_debug_remote_pid;		/* For debug only */
#endif

static int
GlobalDeadlockCheckRemote(LOCAL_WFG *local_wfg, GLOBAL_WFG *global_wfg, RETURNED_WFG **returning_wfg)
{
	GLOBAL_WFG		*new_global_wfg;
	StringInfo		 global_wfg_bin;
	char			*global_wfg_text = NULL;
	StringInfoData	 cmd;
	char			*wfg_in_fname;
	char			*cmd_out_fname;
	FILE			*wfg_in;
#ifndef GDD_DEBUG
	int				 cmd_ret;
#endif
	char			*dsn_downstream;

	if (!(local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
		return DS_NO_DEADLOCK;

	initStringInfo(&cmd);
	*returning_wfg = NULL;

	dsn_downstream = local_wfg->external_lock->dsn;
	new_global_wfg = copy_global_wfg(global_wfg);

	/*
	 * Build global wait-for-graph and translate to string to send to the downstream database.
	 */
	new_global_wfg = AddToGlobalWfG(new_global_wfg, (void *)local_wfg);
	global_wfg_bin = SerializeGlobalWfG(new_global_wfg);
	global_wfg_text = binary2text(global_wfg_bin->data, global_wfg_bin->len);
#ifdef GDD_DEBUG
	{
		char	   *global_wfg_bin_test;
		int			size;

		global_wfg_bin_test = hexa2bin(global_wfg_text, strlen(global_wfg_text), &size);
		global_wfg_test = DeserializeGlobalWfG(global_wfg_bin_test);
	}
#endif
	pfree(global_wfg_bin);
	/*
	 *-------------------------------------------------------------------------------------
	 * Check WfG cycle involving remote transactions.
	 *
	 * Because we cannot use libpq-fe in the backend, we use small exernal worker.
	 *--------------------------------------------------------------------------------------
	 */
	wfg_in_fname = build_worker_file_name(true);
	cmd_out_fname = build_worker_file_name(false);
	appendStringInfo(&cmd, "%s c %s %s %s", WORKER_NAME,
											  normalize_str(dsn_downstream),
											  normalize_str(wfg_in_fname),
											  normalize_str(cmd_out_fname));
	wfg_in = AllocateFile(wfg_in_fname, "w");
	fwrite(global_wfg_text, strlen(global_wfg_text), 1, wfg_in);
	FreeFile(wfg_in);
	/* Here, in the debug, the command should be invoked manually for debug */
#ifndef GDD_DEBUG
	cmd_ret = system(cmd.data);
	if (!WIFEXITED(cmd_ret) || (WEXITSTATUS(cmd_ret) != 0))
		elog(ERROR, "Global deadlock detection worker error.");
#endif
	pfree(cmd.data);
	unlink(wfg_in_fname);
	*returning_wfg = read_returned_wfg(cmd_out_fname);
	unlink(cmd_out_fname);
	return (*returning_wfg)->nReturnedWfg;
}

/*
 * Work function for pg_global_deadlock_check_from_remote() SQL function.
 */
static RETURNED_WFG *
globalDeadlockCheckFromRemote(char *global_wfg_text, DeadLockState *state)
{
	char		    	*global_wfg_stream;
	GLOBAL_WFG 			*global_wfg;
	PGPROC				*pgproc_in_passed_external_lock;
	ExternalLockInfo	*passed_external_lock;		/* External lock passed from upstream */
	RETURNED_WFG	 	*returning_global_wfg = NULL;
	int				 	size;
	DEADLOCK_INFO_BUP	*curr_bup;

	/*
	 * Deserialize received Global WFG from upstream
	 */
	*state = DS_NO_DEADLOCK;

	global_wfg_stream = hexa2bin(global_wfg_text, strlen(global_wfg_text), &size);
	global_wfg = DeserializeGlobalWfG(global_wfg_stream);
	pfree(global_wfg_stream);
	if (global_wfg == NULL)
	{
		*state = DS_GLOBAL_ERROR;
		elog(WARNING, "The input parameter does not contain valid wait-for-graph data.");
		goto returning;
	}

	/* Determine deadlock check mode */
	deadlockCheckMode = globalDeadlockCheckMode(global_wfg);
	if (deadlockCheckMode == DLCMODE_ERROR)
	{
		*state = DS_GLOBAL_ERROR;
		elog(WARNING, "The input parameter does not contain valid wait-for-graph data.");
		goto returning;
	}

	/*
	 * Passed_exernal_lock indicates EXTERNAL LOCK of direct upstream database, connecting to the current database.
	 */
	passed_external_lock = ((LOCAL_WFG *)(&global_wfg->local_wfg[global_wfg->nLocalWfg - 1]))->external_lock;

	/*
	 * Check if the direct upstream's external lock is stable
	 */
	pgproc_in_passed_external_lock = find_pgproc_pgprocno(passed_external_lock->target_pgprocno);

	if (pgproc_in_passed_external_lock == NULL ||
		pgproc_in_passed_external_lock->pid != passed_external_lock->target_pgprocno ||
		pgproc_in_passed_external_lock->lxid != passed_external_lock->target_txn)
	{
		/*
		 * Process or transaction status changed from EXTERNAL LOCK.   This WfG is not stable and is not
		 * a part of a global deadlock.
		 */
		*state = DS_NO_DEADLOCK;
		goto returning;
	}

	/*
	 * Now the status is stable so far and this WfG may be a part of a global deadlock.
	 */
	/*
	 * Check local deadlock.
	 */
	hold_all_lockline();
	*state = DeadLockCheck_int(pgproc_in_passed_external_lock);
	release_all_lockline();

	if (*state != DS_DEADLOCK_INFO)
		goto returning;

	/* Okay some deadlock candidate information was found */
	for (curr_bup = deadlock_info_bup_head; curr_bup; curr_bup = curr_bup->next)
	{
		LOCAL_WFG	*local_wfg;
		GLOBAL_WFG	*global_wfg_here;

		/*
		 * K.Suzuki
		 *
		 * returning wfg に情報追加するとき、shallow copy されていても大丈夫か確認すること
		 */
		if (curr_bup->state == DS_HARD_DEADLOCK)
		{
			/* 今のコンテクストを WfG に直して returning wfg に追加する */
			*state = DS_DEADLOCK_INFO;
			local_wfg = BuildLocalWfG(NULL, curr_bup);
			global_wfg_here = AddToGlobalWfG(global_wfg, local_wfg);
			returning_global_wfg = addGlobalWfgToReturnedWfg(returning_global_wfg, curr_bup->state, global_wfg_here);
		}
		else if (curr_bup->state == DS_DEADLOCK_INFO)
		{
			RETURNED_WFG	*returned_wfg = 0;

			/* 更にこの先をチェックして returning wfg に追加する */
			*state = GlobalDeadlockCheck_int(pgproc_in_passed_external_lock, global_wfg, &returned_wfg);
			if (*state != DS_DEADLOCK_INFO)
				continue;
			returning_global_wfg = add_returned_wfg(returning_global_wfg, returned_wfg, true);
		}
	}
returning:
	if (returning_global_wfg)
		*state = DS_DEADLOCK_INFO;
	return returning_global_wfg;
}

/*
 * K.Suzuki: 20200625
 *
 * 複数の WfG パスが返る可能性がある。これをサポートするように書き換える必要がある
 */
/*
 * Function name
 *
 *	SQL function name: pg_global_deadlock_check_from_remote(IN wfg text, OUT record)
 *
 *  Return value is set of tuple: (dead_lock_state int, wfg text)
 */

Datum
pg_global_deadlock_check_from_remote(PG_FUNCTION_ARGS)
{
#define CHARLEN 16
	RETURNED_WFG	 	*returning_global_wfg;
	DeadLockState		 state;


	/* Used to return the result */
	FuncCallContext	*funcctx;
	TupleDesc		 tupdesc;
	AttInMetadata	*attinmeta;
	HeapTupleData	 tupleData;
	HeapTuple		 tuple = &tupleData;
	char			*values[2];
	char			 state_for_tuple[CHARLEN];
	int				 ii;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	 oldcontext;
		char			*global_wfg_text;

		funcctx = SRF_FIRSTCALL_INIT();

		/* Initialize itelating function */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
		global_wfg_text = PG_GETARG_CSTRING(0);

		returning_global_wfg = globalDeadlockCheckFromRemote(global_wfg_text, &state);

		funcctx->user_fctx = returning_global_wfg;
		funcctx->max_calls = returning_global_wfg ? returning_global_wfg->nReturnedWfg : 0;

		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	values[0] = state_for_tuple;
	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum	result;

		returning_global_wfg = funcctx->user_fctx;
		attinmeta = funcctx->attinmeta;
		ii = 0;
		snprintf(values[ii++], CHARLEN, "%d", returning_global_wfg->state[funcctx->call_cntr]);
		values[ii] = returning_global_wfg->global_wfg_in_text[funcctx->call_cntr];
		tuple = BuildTupleFromCStrings(attinmeta, values);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
	SRF_RETURN_DONE(funcctx);
#undef CHARLEN
}

static DeadLockState
read_returned_state(char *fname)
{
	FILE	*cmd_out;
	char	 cc;
	int		 state = DS_GLOBAL_ERROR;

	cmd_out = AllocateFile(fname, "r");
	while((cc = fgetc(cmd_out)) < '0' || cc > '9');
	if (cc < 0)
		return state;
	state = cc - '0';
	while((cc = fgetc(cmd_out)) >= '0' && cc <= '9')
		state = state * 10 + cc - '0';
	return (DeadLockState)state;
}
static DeadLockState
GlobalDeadlockRecheckRemote(int	pos, char *global_wfg_text, ExternalLockInfo *downstream)
{
	StringInfoData	 cmd;
	DeadLockState	 state;
	char			*wfg_in_fname;
	FILE			*wfg_in;
	char			*cmd_out_fname;
#ifndef GDD_DEBUG
	int				 cmd_ret;
#endif
	char			*dsn_downstream;
	ExternalLockInfo	*external_lock_info;
	GLOBAL_WFG		*g_wfg;
	char			*g_wfg_stream;
	int				 g_wfg_stream_size;

	g_wfg_stream = hexa2bin(global_wfg_text, strlen(global_wfg_text), &g_wfg_stream_size);
	g_wfg = DeserializeGlobalWfG(g_wfg_stream);
	pfree(g_wfg_stream);

	external_lock_info = ((LOCAL_WFG *)g_wfg->local_wfg[pos])->external_lock;
	dsn_downstream = external_lock_info->dsn;

	wfg_in_fname = build_worker_file_name(true);
	cmd_out_fname = build_worker_file_name(false);

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "%s r %d %s %s %s", WORKER_NAME, pos,
											   normalize_str(dsn_downstream),
											   normalize_str(wfg_in_fname),
											   normalize_str(cmd_out_fname));

	wfg_in = AllocateFile(wfg_in_fname, "w");
	fwrite(global_wfg_text, strlen(global_wfg_text), 1, wfg_in);
	FreeFile(wfg_in);
	/* Here, in the debug, the command should be invoked manually for debug */
#ifndef GDD_DEBUG
	cmd_ret = system(cmd.data);
	if (!WIFEXITED(cmd_ret) || (WEXITSTATUS(cmd_ret) != 0))
		elog(ERROR, "Global deadlock detection worker error.");
#endif
	unlink(wfg_in_fname);
	state = read_returned_state(cmd_out_fname);
	unlink(cmd_out_fname);
	return state;
}

/*
 * Function
 *
 * SQL function name: pg_global_deadlock_recheck(IN text, OUT int)
 *
 * Return value is one integer: DeadlockState
 */


Datum
pg_global_deadlock_recheck_from_remote(PG_FUNCTION_ARGS)
{
	int64		 database_system_id;
	char		*global_wfg_text;
	char		*global_wfg_stream;
	int			 size;
	int			 my_pos;
	GLOBAL_WFG	*global_wfg;
	LOCAL_WFG	*local_wfg_caller;
	LOCAL_WFG	*local_wfg_here;
	ExternalLockInfo	*caller_external_lock;
	DeadLockState	state;
	PGPROC		*target_proc_here;

	
	/*
	 * Deserialize recived global Wfg.  The first local WfG contains WfG of the caller, including
	 * external lock pointint this.   The second local WfG is for this.   If it contains external
	 * lock, we need to check further nodes.   If not, this must be the final point of the deadlock
	 * so we must have the same deadlock information.
	 */

	my_pos = PG_GETARG_INT32(0);
	global_wfg_text = PG_GETARG_CSTRING(1);
	global_wfg_stream = hexa2bin(global_wfg_text, strlen(global_wfg_text), &size);
	global_wfg = DeserializeGlobalWfG(global_wfg_stream);
	pfree(global_wfg_stream);
	global_wfg_stream = NULL;
	if (global_wfg == NULL)
	{
		state = DS_GLOBAL_ERROR;
		elog(WARNING, "The input parameter does not contain valid wait-for-graph data.");
		goto returning;
	}
	database_system_id = get_database_system_id();

	/*
	 * Setup local_wfg for upstream and current database.
	 * Also setup if this is the final node to recheck.
	 *
	 * Please note that nLocalWfg == 2 happens only when two databases are involved in
	 * candidate global WfG.   In this case, caller is the origin.
	 */
	Assert(global_wfg->nLocalWfg >= 2);
	Assert(my_pos <= global_wfg->nLocalWfg);

	local_wfg_caller = global_wfg->local_wfg[my_pos - 1];
	local_wfg_here = global_wfg->local_wfg[my_pos];

	caller_external_lock = local_wfg_caller->external_lock;

	/* Check if caller links here and it's stable */
	Lock_PgprocArray();
	target_proc_here = find_pgproc_pgprocno(caller_external_lock->target_pgprocno);
	Unlock_PgprocArray();
	state = DS_NO_DEADLOCK;
	if (target_proc_here == NULL ||
		target_proc_here->pid != caller_external_lock->target_pid ||
		target_proc_here->lxid != caller_external_lock->target_txn)
	{
		/* The target backend from upstream databae is not stable */
		goto returning;
	}
	if (database_system_id != local_wfg_here->database_system_identifier)
		/* Database system ID of the current database in upstream external is not correct */
		goto returning;

	/* Check if this database's wfg is stable */
	if(!check_local_wfg_is_stable(local_wfg_here))
		/* Wait-for-graph local to this database is not stable */
		goto returning;

	if (my_pos + 1 >= global_wfg->nLocalWfg)
		/* This database is the final database in global wait-for-graph */
		goto returning;

	/* Recheck remote */
	state = GlobalDeadlockRecheckRemote(my_pos + 1, global_wfg_text, local_wfg_here->external_lock);

returning:
	if (global_wfg_text)
		pfree(global_wfg_text);
	free_global_wfg(global_wfg);
	PG_RETURN_INT32(state);
}

/*
 * Function
 *
 * SQL function name: pg_global_deadlock_check_describe_backend(IN int, OUT record)
 *
 * Return value is one integer: DeadlockState
 */


Datum
pg_global_deadlock_check_describe_backend(PG_FUNCTION_ARGS)
{
#define CHARLEN 32
#define NCOLUMN 3
	int32	 pgprocno;
	int32	 lxid;
	int32	 pid;
	PGPROC	*proc;
	int		 ii;

	/* Used to return the result */
	TupleDesc		 tupd;
	HeapTupleData	 tupleData;
	HeapTuple		 tuple = &tupleData;
	char			*values[NCOLUMN];
	char			 Values[NCOLUMN][CHARLEN];
	Datum			 result;

	pid = PG_GETARG_INT32(0);
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	if (pid < 0)
		proc = MyProc;
	else
	{
		proc = find_pgproc(pid);
		if (proc == NULL)
		{
			LWLockRelease(ProcArrayLock);
			elog(ERROR, "Specified pid %d is not in backend.", pid);
		}
	}
	pgprocno = proc->pgprocno;
	lxid = proc->lxid;
	LWLockRelease(ProcArrayLock);

	for (ii = 0; ii < NCOLUMN; ii++)
		values[ii] = &Values[ii][0];
	tupd = CreateTemplateTupleDesc(NCOLUMN);
	ii = 1;
	TupleDescInitEntry(tupd, ii++, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "pgprocno", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "lxid", INT4OID, -1, 0);
	ii = 0;
	snprintf(values[ii++], CHARLEN, "%d", pid);
	snprintf(values[ii++], CHARLEN, "%d", pgprocno);
	snprintf(values[ii++], CHARLEN, "%d", lxid);
	tuple =  BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);

	PG_RETURN_DATUM(result);

#undef NCOLUMN
#undef CHARLEN
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
	int	ii;

	for (ii = 0; ii < NUM_LOCK_PARTITIONS; ii++)
		LWLockRelease(LockHashPartitionLockByIndex(ii));
}

#if 0
/*
 * Compare if the two external lock information is the same.
 *
 * Check entries except for dsn.
 */
static bool
external_lock_is_same(ExternalLockInfo *one, ExternalLockInfo *two)
{
	if (one->pid != two->pid || one->pgprocno != two->pgprocno ||
		one->txnid != two->txnid || one->serno != two->serno ||
		one->target_pid != two->target_pid || one->target_pgprocno != two->target_pgprocno ||
		one->target_txn != two->target_txn)
		return false;
	return true;
}
#endif

DEADLOCK_INFO *
GetDeadLockInfo(int32 *nInfo)
{
	*nInfo = nDeadlockDetails;
	return deadlockDetails;
}

/*
 * Caller should acquire LWLock for ProcArrayLock.
 */
static PGPROC *
find_pgproc(int pid)
{
    int ii;

    for (ii = 0; ii < ProcGlobal->allProcCount; ii++)
    {
        if (ProcGlobal->allProcs[ii].pid == pid)
        {
            return &ProcGlobal->allProcs[ii];
        }
    }
    return NULL;
}

static PGPROC *
find_pgproc_pgprocno(int pgprocno)
{
	if (pgprocno < 0)
		return MyProc;
	if (pgprocno >= ProcGlobal->allProcCount)
		elog(ERROR, "Pgprocno is out of bounds. Max should be %d.", ProcGlobal->allProcCount);
	return &ProcGlobal->allProcs[pgprocno];
}
/*
 * Deadlock Info Backup code: For one scan, there could be more than one possible WfG path going
 * remote.
 *
 * Information should be stored in currTransactionContext.
 */
static void
backup_deadlock_info(DeadLockState state)
{
	DEADLOCK_INFO_BUP	*info;
	MemoryContext		 oldctx;

	oldctx = MemoryContextSwitchTo(CurTransactionContext);

	info = (DEADLOCK_INFO_BUP *)palloc(sizeof(DEADLOCK_INFO_BUP) + sizeof(DEADLOCK_INFO) * nDeadlockDetails);
	info->nDeadlock_info = nDeadlockDetails;
	info->state = state;
	info->next = NULL;
	memcpy(&info->deadlock_info[0], deadlockDetails, sizeof(DEADLOCK_INFO) * nDeadlockDetails);
	if (deadlock_info_bup_head == NULL)
		deadlock_info_bup_head = deadlock_info_bup_tail = info;
	else
	{
		deadlock_info_bup_tail->next = info;
		deadlock_info_bup_tail = info;
	}

	MemoryContextSwitchTo(oldctx);
}

static void
clean_deadlock_info_bup_recursive(DEADLOCK_INFO_BUP *info)
{
	if (info->next != NULL)
		clean_deadlock_info_bup_recursive(info->next);
	pfree(info);
}

static void
clean_deadlock_info_bup(void)
{
	MemoryContext	oldctx;

	if (deadlock_info_bup_head == NULL)
		return;

	oldctx = MemoryContextSwitchTo(CurTransactionContext);

	clean_deadlock_info_bup_recursive(deadlock_info_bup_head);
	deadlock_info_bup_head = deadlock_info_bup_tail = NULL;

	MemoryContextSwitchTo(oldctx);
}

/*
 * Check if wait-for-graph represents the current local status
 *
 * Local WFG must be for the instance the backend is running.
 */
static bool
check_local_wfg_is_stable(LOCAL_WFG *localWfG)
{
	DEADLOCK_INFO	*info;
	int				 nDeadlockInfo;
	int				 ii;
	PGPROC			*proc;

	Assert(localWfG != NULL);

	info = localWfG->deadlock_info;
	nDeadlockInfo = localWfG->nDeadlockInfo;
	LWLockAcquire(ProcArrayLock, LW_SHARED);

	for (ii = 0; ii < nDeadlockInfo; ii++)
	{
		proc = find_pgproc_pgprocno(info[ii].pgprocno);
		if (proc == NULL)
			goto instable;
		if (proc->pid != info[ii].pid)
			goto instable;
		if (proc->waitLockMode != info[ii].lockmode)
			goto instable;
		if (memcmp(&(proc->waitLock->tag), &info[ii].locktag, sizeof(LOCKTAG)))
			goto instable;
		if (proc->lxid != info[ii].txid)
			goto instable;
	}
	LWLockRelease(ProcArrayLock);
	return true;

instable:
	LWLockRelease(ProcArrayLock);
	return false;
}

/*
 * K.Suzuki:
 *
 * 以下２つ、データコピーが shallow copy 担っていないか確認すること。
 * shallow copy だと、呼び出し側が元の記憶域を再利用すると困ったことになる。
 * あるいは、shallow copy しても大丈夫なように呼び出し側を工夫する必要がある。
 */
/* returned wfg に新たな returned wfg を追加する */
static RETURNED_WFG *
add_returned_wfg(RETURNED_WFG *dest, RETURNED_WFG *src, bool clean_opt)
{
	RETURNED_WFG *rv;
	int			  ii;

	rv = (dest == NULL) ? palloc0(sizeof(RETURNED_WFG)) : dest;
	rv->global_wfg_in_text = repalloc(rv->global_wfg_in_text, sizeof(char *) * (rv->nReturnedWfg + src->nReturnedWfg));
	for (ii = 0; ii < src->nReturnedWfg; ii++)
		rv->global_wfg_in_text[dest->nReturnedWfg + ii] = src->global_wfg_in_text[ii];
	rv->nReturnedWfg += src->nReturnedWfg;
	if (clean_opt)
		pfree(src);
	return rv;
}

/* Reterned wfg に 新たな global wfg を追加する */
static RETURNED_WFG *
addGlobalWfgToReturnedWfg(RETURNED_WFG *returned_wfg, DeadLockState state, GLOBAL_WFG *global_wfg)
{
	StringInfo		 global_wfg_strinfo;
	char			*global_wfg_txt;

	if (returned_wfg == NULL)
		returned_wfg = palloc0(sizeof(RETURNED_WFG));
	global_wfg_strinfo = SerializeGlobalWfG(global_wfg);
	global_wfg_txt = binary2text(global_wfg_strinfo->data, global_wfg_strinfo->len);
	pfree(global_wfg_strinfo->data);
	pfree(global_wfg_strinfo);
	returned_wfg->nReturnedWfg++;
	returned_wfg->state = repalloc(returned_wfg->state, sizeof(DeadLockState) * returned_wfg->nReturnedWfg);
	returned_wfg->state[returned_wfg->nReturnedWfg - 1] = state;
	returned_wfg->global_wfg_in_text = repalloc(returned_wfg->global_wfg_in_text, sizeof(char *) * returned_wfg->nReturnedWfg);
	returned_wfg->global_wfg_in_text[returned_wfg->nReturnedWfg - 1] = global_wfg_txt;
	return returned_wfg;
}


/* global wfg から自ノードの local wfg を抽出してこの中のproc を visited proc に追加する */
/*
 * K.Suzuki 20200626:
 * これは visited proc を追加するのではなく、external lockのlocktag と externla locktag info を追加すべきである
 */

static DeadlockCheckMode
globalDeadlockCheckMode(GLOBAL_WFG *global_wfg)
{
	uint64	my_database_system_id;
	int		ii;
	DeadlockCheckMode	rv = DLCMODE_LOCAL;

	my_database_system_id = get_database_system_id();
	globalVisitedProcs = NULL;
	nGlobalVisitedProcs = 0;

	globalVisitedExternalLock = NULL;
	nGlobalVisitedExternalLock = 0;

	visitedOriginPid = -1;
	visitedOriginPgprocno = -1;
	visitedOriginTxid = 0;

	if (global_wfg == NULL)
		return DLCMODE_LOCAL;
	if (global_wfg->nLocalWfg <= 0 ||
		global_wfg->is_text[0] == true ||
		global_wfg->is_text[global_wfg->nLocalWfg - 1] == true)
	{
		elog(WARNING, "The first or last local wait-for-graph was not compatible and could not deserialize.");
		return DLCMODE_ERROR;
	}
	rv = DLCMODE_GLOBAL_NEW;
	for (ii = 0; ii < global_wfg->nLocalWfg; ii++)
	{
		LOCAL_WFG *local_wfg;

		if (global_wfg->is_text[ii] == true)
			continue;
		local_wfg = global_wfg->local_wfg[ii];
		if (local_wfg->database_system_identifier != my_database_system_id)
			continue;
		if (!local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
		{
			nGlobalVisitedProcs = 0;
			if (globalVisitedProcs)
				pfree(globalVisitedProcs);
			nGlobalVisitedExternalLock = 0;
			if (globalVisitedExternalLock)
				pfree(globalVisitedExternalLock);
			elog(WARNING, "Some of local wait-for-graph does not terminate with external lock.");
			return DLCMODE_ERROR;
		}
		if (ii == 0)
		{
			DEADLOCK_INFO	*info;
			PGPROC	*proc_wk;

			info = &local_wfg->deadlock_info[0];
			proc_wk = find_pgproc(info->pgprocno);
			if (proc_wk == NULL || proc_wk->pid != info->pid || proc_wk->lxid != info->txid)
				continue;

			nGlobalVisitedProcs++;
			globalVisitedProcs = (PGPROC **)repalloc(globalVisitedProcs, sizeof(PGPROC *) * nGlobalVisitedProcs);
			globalVisitedProcs[nGlobalVisitedProcs - 1] = proc_wk;

			visitedOriginPid = local_wfg->visitedProcPid;
			visitedOriginPgprocno = local_wfg->visitedProcPgprocno;
			visitedOriginTxid = local_wfg->visitedProcLxid;

			rv = DLCMODE_GLOBAL_ORIGIN;
		}
		else if (rv == DLCMODE_GLOBAL_NEW)
			rv = DLCMODE_GLOBAL_AGAIN;
		/* Setup EXTERNAL LOCK going out from this node in the global wfg */
		nGlobalVisitedExternalLock++;
		globalVisitedExternalLock[nGlobalVisitedExternalLock - 1]
			= (ExternalLockInfo *)repalloc(globalVisitedExternalLock,
										   sizeof(ExternalLockInfo *) * nGlobalVisitedExternalLock);
		globalVisitedExternalLock[nGlobalVisitedExternalLock -1] = local_wfg->external_lock;
	}
	return rv;
}

static void
free_returned_wfg(RETURNED_WFG *returned_wfg)
{
	if (!returned_wfg)
		return;
	if (returned_wfg->state)
		pfree(returned_wfg->state);
	if (returned_wfg->global_wfg_in_text)
		pfree(returned_wfg->global_wfg_in_text);
	pfree(returned_wfg);
	return;
}

static void
free_local_wfg(LOCAL_WFG *local_wfg)
{
	if (!local_wfg)
		return;
	if (local_wfg->deadlock_info)
		pfree(local_wfg->deadlock_info);
	if (local_wfg->external_lock)
	{
		if (local_wfg->external_lock->dsn)
			pfree(local_wfg->external_lock->dsn);
		pfree(local_wfg->external_lock);
	}
	pfree(local_wfg);
}

static void
free_global_wfg(GLOBAL_WFG *global_wfg)
{
	int ii;

	if (!global_wfg)
		return;
	for (ii = 0; ii < global_wfg->nLocalWfg; ii++)
	{
		if (global_wfg->is_text[ii])
			pfree(global_wfg->local_wfg[ii]);
		else
			free_local_wfg((LOCAL_WFG *)global_wfg->local_wfg[ii]);
	}
	pfree(global_wfg->is_text);
	pfree(global_wfg->local_wfg);
	pfree(global_wfg);
}

static char *
build_worker_file_name(bool input_to_command)
{
	StringInfoData	fname;

	initStringInfo(&fname);
	appendStringInfo(&fname, "%s/pg_external_locks/wfg_%d.%s",
							 DataDir, MyProc->pid,
							 input_to_command ? "in" : "out");
	return fname.data;
}

static RETURNED_WFG *
read_returned_wfg(char *fname)
{
	FILE	*wfg_out;
	char	 c;
	int		 ii;
	RETURNED_WFG *returning_wfg;

	wfg_out = AllocateFile(fname, "r");
	while ((c = fgetc(wfg_out)) < '0' && c > '9')
	{
		if (c < 0)
			return NULL;
	}
	returning_wfg = (RETURNED_WFG *)palloc(sizeof(RETURNED_WFG));
	returning_wfg->nReturnedWfg = c - '0';
	while ((c = fgetc(wfg_out)) >= '0' && c <= '9')
		returning_wfg->nReturnedWfg = returning_wfg->nReturnedWfg * 10 + c - '0';
	if (c < 0)
	{
		pfree(returning_wfg);
		return NULL;
	}
	while (c != '\n')
	{
		c = fgetc(wfg_out);
		if (c < 0)
		{
			pfree(returning_wfg);
			return NULL;
		}
	}
	returning_wfg->state = (DeadLockState *)palloc0(sizeof(DeadLockState) * returning_wfg->nReturnedWfg);
	returning_wfg->global_wfg_in_text = (char **)palloc0(sizeof(char *) * returning_wfg->nReturnedWfg);
	for (ii = 0; ii < returning_wfg->nReturnedWfg; ii++)
	{
		int				state_i;
		int				sign = 1;
		StringInfoData	wfg_text;

		while(((c = fgetc(wfg_out)) != '-') || (c < '0' || c > '9'))
		{
			if (c < 0)
			{
				free_returned_wfg(returning_wfg);
				return NULL;
			}
		}
		if (c == '-')
		{
			sign = -1;
			state_i = 0;
		}
		else
			state_i = c - '0';
		while ((c = fgetc(wfg_out)) >= '0' && c <= '9')
			state_i = state_i * 10 + c - '0';
		state_i *= sign;
		while (c != ',')
			c = fgetc(wfg_out);
		while ((c = fgetc(wfg_out)) == ' ' || c == '\t');
		initStringInfo(&wfg_text);
		appendStringInfoChar(&wfg_text, c);
		while((c = fgetc(wfg_out)) != '\n')
			appendStringInfoChar(&wfg_text, c);
		returning_wfg->state[ii] = (DeadLockState)state_i;
		returning_wfg->global_wfg_in_text[ii] = wfg_text.data;
	}
	FreeFile(wfg_out);
	return returning_wfg;
}

static LOCAL_WFG *
copy_local_wfg(LOCAL_WFG *l_wfg)
{
	LOCAL_WFG	*copied;

	copied = (LOCAL_WFG *)palloc(sizeof(LOCAL_WFG));
	memcpy(copied, l_wfg, sizeof(LOCAL_WFG));
	copied->deadlock_info = (DEADLOCK_INFO *)palloc(sizeof(DEADLOCK_INFO) * l_wfg->nDeadlockInfo);
	memcpy(copied->deadlock_info, l_wfg->deadlock_info, sizeof(DEADLOCK_INFO) * l_wfg->nDeadlockInfo);
	copied->external_lock = (ExternalLockInfo *)palloc(sizeof(ExternalLockInfo));
	memcpy(copied->external_lock, l_wfg->external_lock, sizeof(ExternalLockInfo));
	copied->external_lock->dsn = pstrdup(l_wfg->external_lock->dsn);
	return copied;
}

static GLOBAL_WFG *
copy_global_wfg(GLOBAL_WFG *g_wfg)
{
	GLOBAL_WFG	*copied;
	int			 ii;

	if (g_wfg == NULL)
		return NULL;

	copied = (GLOBAL_WFG *)palloc(sizeof(GLOBAL_WFG));

	copied->global_wfg_magic = g_wfg->global_wfg_magic;
	copied->nLocalWfg = g_wfg->nLocalWfg;
	copied->is_text = (bool *)palloc(sizeof(bool) * g_wfg->nLocalWfg);
	memcpy(copied->is_text, g_wfg->is_text, sizeof(bool) * g_wfg->nLocalWfg);
	for (ii = 0; ii < g_wfg->nLocalWfg; ii++)
	{
		if (copied->is_text[ii] == true)
		{
			copied->local_wfg[ii] = pstrdup((char *)(g_wfg->local_wfg[ii]));
			copied->txtsize[ii] = strlen((char *)(g_wfg->local_wfg[ii]));
		}
		else
			copied->local_wfg[ii] = copy_local_wfg((LOCAL_WFG *)(g_wfg->local_wfg[ii]));
	}
	return copied;
}

char *
normalize_str(char *src)
{
	StringInfoData	data;

	initStringInfo(&data);
	appendStringInfoChar(&data, '\'');
	while(*src)
	{
		if ((*src) == '\'')
			appendStringInfoChar(&data, '\\');
		appendStringInfoChar(&data, *src++);
	}
	appendStringInfoChar(&data, '\'');
	return data.data;
}
