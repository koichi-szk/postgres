/*-------------------------------------------------------------------------
 *
 * deadlock.c
 *	  POSTGRES deadlock detection code
 *
 * See src/backend/storage/lmgr/README for a description of the deadlock
 * detection and resolution algorithms.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
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
#undef	GDD_DEBUG

#include "postgres.h"

#include <unistd.h>

#include "catalog/pg_control.h"
#include "catalog/pg_type_d.h"
#include "common/controldata_utils.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/global_deadlock.h"
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
	DLCMODE_LOCAL,				/* Invoked locally                                          */
	DLCMODE_GLOBAL_NEW,			/* Part of global deadlock detection.                       */
								/* 		First visit to the database in External Lock path   */
	DLCMODE_GLOBAL_AGAIN,		/* Part of global deadlock detection.                       */
								/* 		Visited this database in the past but it is         */
								/* 		not the origin.                                     */
	DLCMODE_GLOBAL_ORIGIN,		/* Part of global deadlock detection.                       */
								/*		Invoked in the origin database                      */
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
 * Functions for local deadlock detection and and wait-for-graph extraction
 */
static DeadLockState DeadLockCheckRecurse(PGPROC *proc);
static int           TestConfiguration(PGPROC *startProc);
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
#ifdef DEBUG_DEADLOCK
static void PrintLockQueue(LOCK *lock, const char *info);

/*
 * Functions for global lock detection
 */
#endif

#define WORKER_NAME	"pg_gdd_check_worker"

static DeadLockState DeadLockCheck_int(PGPROC *proc);
static int GlobalDeadlockCheckRemote(LOCAL_WFG *local_wfg, GLOBAL_WFG *global_wfg, RETURNED_WFG **returning_wfg);
static DeadLockState GlobalDeadlockCheck_int(PGPROC *proc, GLOBAL_WFG *global_wfg, RETURNED_WFG **rv);
static DeadlockCheckMode globalDeadlockCheckMode(GLOBAL_WFG *global_wfg);
static GLOBAL_WFG *addToGlobalWfG(GLOBAL_WFG *g_wfg, LOCAL_WFG *local_wfg);
static void clean_deadlock_info_bup_recursive(DEADLOCK_INFO_BUP *info);
static void clean_deadlock_info_bup(void);
static bool check_local_wfg_is_stable(LOCAL_WFG *localWfG);
static void backup_deadlock_info(DeadLockState state);
static void free_returned_wfg(RETURNED_WFG *returned_wfg);
static RETURNED_WFG *add_returned_wfg(RETURNED_WFG *dest, RETURNED_WFG *src, bool clean_opt);
static RETURNED_WFG *addGlobalWfgToReturnedWfg(RETURNED_WFG *returned_wfg, DeadLockState state, GLOBAL_WFG *global_wfg);
static RETURNED_WFG *globalDeadlockCheckFromRemote(char *global_wfg_text, DeadLockState *state);
static void free_local_wfg(LOCAL_WFG *local_wfg);
static PGPROC *find_pgproc(int pid);
static PGPROC *find_pgproc_pgprocno(int pgprocno);
static LOCAL_WFG *copy_local_wfg(LOCAL_WFG *l_wfg);
static GLOBAL_WFG *copy_global_wfg(GLOBAL_WFG *g_wfg);
static bool external_lock_already_in_gwfg(GLOBAL_WFG *global_wfg, DEADLOCK_INFO_BUP *dl_info_bup);
static void *gdd_repalloc(void *pointer, Size size);
static void DeadLockReport_int(void) pg_attribute_noreturn();
static void GlobalDeadlockReport_int(void) pg_attribute_noreturn();
static void	add_backend_activities_local_wfg(LOCAL_WFG *local_wfg);
#ifdef GDD_DEBUG
void free_global_wfg(GLOBAL_WFG *global_wfg);
static void print_global_wfg(StringInfo out, GLOBAL_WFG *g_wfg);
static void print_local_wfg(StringInfo out, LOCAL_WFG *l_wfg, int idx, int total);
static void print_deadlock_info(StringInfo out, DEADLOCK_INFO *info, int idx, int total);
static void print_external_lock_info(StringInfo out, ExternalLockInfo *e);
#endif /* GDD_DEBUG */
static char *getAtoInt64(char *buf, int64 *value);
static char *getAtoInt32(char *buf, int32 *value);
static char *getAtoUint64(char *buf, uint64 *value);
static char *getAtoUint32(char *buf, uint32 *value);
static char *getAtoUint16(char *buf, uint16 *value);
static char *getAtoUint8(char *buf, uint8 *value);
static char *getHexaToInt(char *buf, int *value);
static char *getHexaToLong(char *buf, long *value);
static char *getString(char *buf, char **value);
static char *findChar(char *buf, char c);
static bool CloseScan(char *buf);
static char *normalizeString(char *buf);
static char *SerializeLocalWfG(LOCAL_WFG *local_wfg);
static char *DeserializeLocalWfG(char *buf, LOCAL_WFG **local_wfg);
static char *SerializeGlobalWfG(GLOBAL_WFG *g_wfg);
static GLOBAL_WFG * DeserializeGlobalWfG(char *buf);

#define Lock_PgprocArray()	LWLockAcquire(ProcArrayLock, LW_SHARED)
#define Unlock_PgprocArray() LWLockRelease(ProcArrayLock)
#define Lock_Pgproc(p)		LWLockAcquire(&(p)->fpInfoLock, LW_SHARED)
#define Unlock_Pgproc(p)	LWLockRelease(&(p)->fpInfoLock)

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

/* Database System Identifier of the current database */
static uint64 my_database_system_identifier = 0;

/* Global wait-for-graph for deadlock report */
static GLOBAL_WFG *global_deadlock_info = NULL;

/* Entry point for remote database WfG check */
PGDLLIMPORT gdd_check_fn	*pg_gdd_check_func = NULL;

#ifdef GDD_DEBUG
#define	GDD_ARRAY_MAX 128
#endif /* GDD_DEBUG */

/* Macros for serialize/deserialize WfG */
#define FindChar(b, c) do{b = findChar(b, c); if (b == NULL) return NULL;}while(0)
#define GetHexaToInt(b, v) do{b = getHexaToInt(b, v); if (b == NULL) return NULL;}while(0)
#define GetHexaToLong(b, v) do{b = getHexaToLong(b, v); if (b == NULL) return NULL;}while(0)
#define GetAtoInt64(b, v) do{b = getAtoInt64(b, v); if (b == NULL) return NULL;}while(0)
#define GetAtoInt32(b, v) do{b = getAtoInt32(b, v); if (b == NULL) return NULL;}while(0)
#define GetAtoUint64(b, v) do{b = getAtoUint64(b, v); if (b == NULL) return NULL;}while(0)
#define GetAtoUint32(b, v) do{b = getAtoUint32(b, v); if (b == NULL) return NULL;}while(0)
#define GetAtoUint16(b, v) do{b = getAtoUint16(b, v); if (b == NULL) return NULL;}while(0)
#define GetAtoUint8(b, v) do{b = getAtoUint8(b, v); if (b == NULL) return NULL;}while(0)
#define GetString(b, v) do{b = getString(b, v); if (b == NULL) return NULL;}while(0)

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

	global_deadlock_info = NULL;

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

	nEdges = TestConfiguration(proc);
	if (nEdges == -2)		/* K.Suzuki external lock was detected. */
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
					elog(ERROR, "Inconsistent internal state in deadlock check (1), state: %d.", state);
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
					elog(ERROR, "Inconsistent internal state in deadlock check (2), state: %d.", state);
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
	int			i;

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
		else if (checkProc->waitLock == NULL)
			return DS_NO_DEADLOCK;
	}
	if (deadlockCheckMode == DLCMODE_LOCAL)
	{
		/*
		 * Check if there's local wait-for-graph cycle.  This is done only at
		 * LOCAL check mode.
		 */
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
				return DS_NO_DEADLOCK;
			}
		}
	}
	/* Mark proc as seen */
	Assert(nVisitedProcs < MaxBackends);
	visitedProcs[nVisitedProcs++] = checkProc;

	/*
	 * If the process is waiting, there is an outgoing waits-for edge to each
	 * process that blocks it.
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
	if (checkProc->links.next != NULL && checkProc->waitLock != NULL)

	{
		DeadLockState	state;

		state = FindLockCycleRecurseMember(checkProc, checkProc, depth, softEdges, nSoftEdges);
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

		if (memberProc->links.next != NULL && memberProc->waitLock != NULL && memberProc != checkProc)
		{
			DeadLockState	state;

			state = FindLockCycleRecurseMember(memberProc, checkProc, depth, softEdges, nSoftEdges);
			if (state == DS_HARD_DEADLOCK)
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

	/*
	 * The relation extension or page lock can never participate in actual
	 * deadlock cycle.  See Asserts in LockAcquireExtended.  So, there is no
	 * advantage in checking wait edges from them.
	 */
	if (LOCK_LOCKTAG(*lock) == LOCKTAG_RELATION_EXTEND ||
		(LOCK_LOCKTAG(*lock) == LOCKTAG_PAGE))
		return false;

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
					 * Note we read statusFlags without any locking.  This is
					 * OK only for checking the PROC_IS_AUTOVACUUM flag,
					 * because that flag is set at process start and never
					 * reset.  There is logic elsewhere to avoid canceling an
					 * autovacuum that is working to prevent XID wraparound
					 * problems (which needs to read a different statusFlags
					 * bit), but we don't do that here to avoid grabbing
					 * ProcArrayLock.
					 */
					if (checkProc == MyProc &&
						proc->statusFlags & PROC_IS_AUTOVACUUM)
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
				info->pid = checkProc->pid;
				info->pgprocno = checkProc->pgprocno;
				info->txid = checkProc->lxid;


				/* This proc soft-blocks checkProc */
				state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
				if (state == DS_HARD_DEADLOCK)
				{
					/*
					 * Add this edge to the list of soft edges in the cycle
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
				info->pid = checkProc->pid;
				info->pgprocno = checkProc->pgprocno;
				info->txid = checkProc->lxid;


				/* This proc soft-blocks checkProc */
				state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
				if (state == DS_HARD_DEADLOCK)
				{
					/*
					 * Add this edge to the list of soft edges in the cycle
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
 */
void
DeadLockReport(void)
{
	if (global_deadlock_info == NULL)
		DeadLockReport_int();
	else
		GlobalDeadlockReport_int();
}

static void
DeadLockReport_int(void)
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
	appendBinaryStringInfo(&logbuf, clientbuf.data, clientbuf.len);

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
	if (info->deadlock_info[info->nDeadlock_info -1].locktag.locktag_type == LOCKTAG_EXTERNAL)
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
	local_wfg->backend_activity = (char **)palloc0(sizeof(char *) * info->nDeadlock_info);
	if (local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
		local_wfg->external_lock = GetExternalLockProperties(&info->deadlock_info[info->nDeadlock_info -1].locktag);
	else
		local_wfg->external_lock = NULL;

	add_backend_activities_local_wfg(local_wfg);

	return local_wfg;
}



/*
 * We use database system id to identify a database instance.   This is created by initdb and does not change
 * throughout the database lifetime.   This is essentially the timestan when initdb created the database
 * materials plus process id of initdb.   We can assume this is unique enough throughout correction of
 * databases in the scope.
 *
 * K.Suzuki: When database is cloned with pg_basebackup, cold copy or any similar way, database data directory
 *           will be simply copied and we have a risk different database has the same database system id.
 *           To avoid this, we will provide two ways to specify different identification method:
 *           Offline utility to update database system identifier (pg_database_system_identifier), which
 *           must run while the daabase is not active.
 */
uint64
get_database_system_id(void)
{
	ControlFileData *controlfiledata;
	bool			 crc_ok;

	if (my_database_system_identifier)
		return my_database_system_identifier;
	controlfiledata = get_controlfile(DataDir, &crc_ok);
	my_database_system_identifier = controlfiledata->system_identifier;
	pfree(controlfiledata);
	return my_database_system_identifier;
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
		case LOCKTAG_DATABASE_FROZEN_IDS:
			return "LOCKTAG_DATABASE_FROZEN_IDS";
	}
	return "LOCKTAG_NO_SUCH_TYPE";
}

static GLOBAL_WFG *
addToGlobalWfG(GLOBAL_WFG *global_wfg, LOCAL_WFG *local_wfg)
{
	Assert(local_wfg);

	if (global_wfg == NULL)
	{
		global_wfg = (GLOBAL_WFG *)palloc(sizeof(GLOBAL_WFG));
		global_wfg->nLocalWfg = 1;
		global_wfg->local_wfg = (LOCAL_WFG **)palloc(sizeof(void *));
		global_wfg->local_wfg[0] = local_wfg;
	}
	else
	{
		global_wfg->nLocalWfg++;
		global_wfg->local_wfg = (LOCAL_WFG **)gdd_repalloc(global_wfg->local_wfg, sizeof(void *) * global_wfg->nLocalWfg);
		global_wfg->local_wfg[global_wfg->nLocalWfg - 1] = local_wfg;
	}
	return global_wfg;
}

/*
 * Entry point of global deadlock check.
 *
 * Called just after DeadLockCheck() from proc.c.
 *
 * Be aware that this function does not assume the caller has acquired lock line
 * locks.   It is highly recommended to release all the lockline of LWLocks.
 *
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
#ifdef GDD_DEBUG
	StringInfoData	out;
#endif /* GDD_DEBUG */

	globalVisitedProcs = NULL;
	nGlobalVisitedProcs = 0;
	returning_wfg = NULL;

#ifdef GDD_DEBUG
	initStringInfo(&out);
	appendStringInfo(&out, "\n=== %s, line: %d ================================================", __func__, __LINE__);
	print_global_wfg(&out, global_wfg);
	elog(DEBUG1, "%s", out.data);
	resetStringInfo(&out);
#endif /* GDD_DEBUG */
	/*
	 * Initialize .so for remote database WfG check
	 */
	if (pg_gdd_check_func == NULL)
		load_file("libpqgddcheckremote", false);

	/*
	 * Previous DeadLockCheck() may have found more than one possible piece of wait-for-graph
	 * spanning to remote transactions.
	 *
	 * We need to check all these possible wait-for-graph.
	 */
	for (curBup = deadlock_info_bup_head; curBup; curBup = curBup->next)
	{
		if (curBup->state == DS_HARD_DEADLOCK)
			/*
			 * This status should appear only in downstream databae, where this state has already
			 * been handled in globalDeadlockCheckFromRemote().
			 */
			continue;
		/*
		 * Check if EXTERANL lock of current deadlock info is not in the global wfg
		 */
		if (external_lock_already_in_gwfg(global_wfg, curBup))
			continue;
		local_wfg = BuildLocalWfG(proc, curBup);
#ifdef GDD_DEBUG
		appendStringInfo(&out, "\n=== %s, line: %d ================================================", __func__, __LINE__);
		print_local_wfg(&out, local_wfg, 0, 1);
		elog(DEBUG1, "%s", out.data);
		resetStringInfo(&out);
#endif /* GDD_DEBUG */
		if (!local_wfg || !check_local_wfg_is_stable(local_wfg))
			continue;
		nWfG = GlobalDeadlockCheckRemote(local_wfg, global_wfg, &returned_wfg);
		if (nWfG == 0)
			continue;
		if (nWfG < 0)
			elog(ERROR, "Error in remote wait-for-graph check.\n");
		/*
		 * Check of local wait-for-graph is stable
		 */
		if (!check_local_wfg_is_stable(local_wfg))
		{
			/* Checked WFG is not deadlock candidate. */
			free_local_wfg(local_wfg);
			continue;
		}
		if (deadlockCheckMode == DLCMODE_LOCAL)
		{
			/*
			 * If we are at the beginning database (ORIGIN), we need to recheck if the observerd
			 * wait-for-graph is static.   If not, it's not a part of a deadlock.
			 *
			 * Now nReturnedWfg is at most one.
			 */
			for (ii = 0; ii < returned_wfg->nReturnedWfg; ii++)
			{
				if (returned_wfg->state[ii] == DS_HARD_DEADLOCK || returned_wfg->state[ii] == DS_GLOBAL_ERROR)
				{
					if (returned_wfg->state[ii] == DS_HARD_DEADLOCK)
					{
						MemoryContext	oldctx;

						oldctx = MemoryContextSwitchTo(CurTransactionContext);
						global_deadlock_info = DeserializeGlobalWfG(returned_wfg->global_wfg_in_text[ii]);
						MemoryContextSwitchTo(oldctx);
					}
					else
						global_deadlock_info = NULL;
					free_returned_wfg(returned_wfg);
					state = returned_wfg->state[ii];
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

	/*
	 * Cleanup global deadlock check object local to this database
	 */
#ifdef GDD_DEBUG
	pfree(out.data);
#endif /* GDD_DEBUG */
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
 ********************************************************************************************
 *
 * K.Suzuki: Global deadlock detection functions
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
#ifdef GDD_DEBUG
int		gdd_debug_remote_pid;		/* For debug only */
#endif /* GDD_DEBUG */

#define	GDD_CHECK_BY_FUNC	true

static int
GlobalDeadlockCheckRemote(LOCAL_WFG *local_wfg, GLOBAL_WFG *global_wfg, RETURNED_WFG **returning_wfg)
{
	GLOBAL_WFG		*new_global_wfg;
	char			*global_wfg_text = NULL;
	StringInfoData	 cmd;
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
	new_global_wfg = addToGlobalWfG(new_global_wfg, (void *)local_wfg);
	global_wfg_text = SerializeGlobalWfG(new_global_wfg);

	*returning_wfg = pg_gdd_check_func(dsn_downstream, global_wfg_text);

	return (*returning_wfg)->nReturnedWfg;
}

/*
 * Work function for pg_global_deadlock_check_from_remote() SQL function.
 */
static RETURNED_WFG *
globalDeadlockCheckFromRemote(char *global_wfg_text, DeadLockState *state)
{
	GLOBAL_WFG 			*global_wfg;
	PGPROC				*pgproc_in_passed_external_lock;
	ExternalLockInfo	*passed_external_lock;			/* External lock passed from upstream */
	RETURNED_WFG	 	*returning_global_wfg = NULL;	/* To be returned to the upstream */
	RETURNED_WFG		*returned_wfg = NULL;			/* Returned from the downstream */
	DEADLOCK_INFO_BUP	*curr_bup;

	*state = DS_NO_DEADLOCK;

	/*
	 * Deserialize received Global WFG from upstream
	 */
	global_wfg = DeserializeGlobalWfG(global_wfg_text);
	if (global_wfg == NULL)
	{
		*state = DS_GLOBAL_ERROR;
		elog(WARNING, "The input parameter does not contain valid wait-for-graph data.");
		goto returning;
	}

	/*
	 * Determine deadlock check mode
	 */
	deadlockCheckMode = globalDeadlockCheckMode(global_wfg);
	if (deadlockCheckMode == DLCMODE_ERROR)
	{
		*state = DS_GLOBAL_ERROR;
		elog(WARNING, "The input parameter does not contain valid wait-for-graph data.");
		goto returning;
	}

	/*
	 * Passed_exernal_lock indicates EXTERNAL LOCK of direct upstream database, connecting to the current database.
	 * This is used to check if the external lock is stable, the target PGPROC, pid and txn did not change.
	 */
	passed_external_lock = global_wfg->local_wfg[global_wfg->nLocalWfg - 1]->external_lock;

	/*
	 * Check if the direct upstream's external lock is stable
	 */
	pgproc_in_passed_external_lock = find_pgproc_pgprocno(passed_external_lock->target_pgprocno);

	Lock_PgprocArray();
	Lock_Pgproc(pgproc_in_passed_external_lock);
	if (pgproc_in_passed_external_lock == NULL ||
		pgproc_in_passed_external_lock->pid != passed_external_lock->target_pid ||
		pgproc_in_passed_external_lock->pgprocno != passed_external_lock->target_pgprocno || /* K.Suzuki: maybe this is not needed */
		pgproc_in_passed_external_lock->lxid != passed_external_lock->target_txn)
	{
		/*
		 * Process or transaction status changed from EXTERNAL LOCK.   This WfG is not stable and is not
		 * a part of a global deadlock.
		 */
		Unlock_Pgproc(pgproc_in_passed_external_lock);
		Unlock_PgprocArray();
		*state = DS_NO_DEADLOCK;
		goto returning;
	}
	Unlock_Pgproc(pgproc_in_passed_external_lock);
	Unlock_PgprocArray();

	/*
	 * Now the status is stable so far and this WfG may be a part of a global deadlock.
	 */
	/*
	 * Check local deadlock.
	 */
	hold_all_lockline();
	/*
	 * Check of local wait-for-graph begging at the proc in the external lock.
	 * Here, all the candidate wait-for-graph is stored in deadlock info backup
	 * chain.
	 */
	*state = DeadLockCheck_int(pgproc_in_passed_external_lock);
	release_all_lockline();

	if (*state != DS_DEADLOCK_INFO)
		/* Local state from this proc is "no deadlock" or "local deadlock". Nothing to do here. */
		goto returning;

	/*
	 * Find if global deadlock is found in this database
	 */
	for (curr_bup = deadlock_info_bup_head; curr_bup; curr_bup = curr_bup->next)
	{
		LOCAL_WFG	*local_wfg;
		GLOBAL_WFG	*global_wfg_here;

		if (curr_bup->state == DS_HARD_DEADLOCK)
		{
			*state = DS_DEADLOCK_INFO;
			local_wfg = BuildLocalWfG(NULL, curr_bup);

			if (check_local_wfg_is_stable(local_wfg))
			{
				/*
				 * Add local wait-for-graph in this database to the global wait-for-graph to be returned to
				 * upstream database.
				 */
				/*
				 * Add activity of each process involved in this wait-for-graph, using pgstat_get_backend_current_activity(pid, false)
				 */
				add_backend_activities_local_wfg(local_wfg);
				global_wfg_here = addToGlobalWfG(global_wfg, local_wfg);
				returning_global_wfg = addGlobalWfgToReturnedWfg(returning_global_wfg, curr_bup->state, global_wfg_here);
				goto returning;
			}
			else
			{
				pfree(local_wfg);
				continue;
			}
		}
	}
	/*
	 * No global deadlock is found in this database.
	 *
	 * Then continue further global wait-for-graph search to other downstream databae.
	 */
	*state = GlobalDeadlockCheck_int(pgproc_in_passed_external_lock, global_wfg, &returned_wfg);
	if (*state != DS_DEADLOCK_INFO)
		goto returning;
	returning_global_wfg = add_returned_wfg(returning_global_wfg, returned_wfg, true);
returning:
	if (returning_global_wfg)
		*state = DS_DEADLOCK_INFO;
	return returning_global_wfg;
}

/*
 * SQL functions
 *
 *	SQL function name: pg_global_deadlock_check_from_remote(IN wfg text, OUT record)
 *
 *  This is called from global deadlock check worker function through libpq.
 *
 *  Return value is set of tuple: (dead_lock_state int, wfg text)
 *  It started to return more than one tuple but at present, this returns just one
 *  tuple.
 */

Datum
pg_global_deadlock_check_from_remote(PG_FUNCTION_ARGS)
{
#define CHARLEN 16
#define COLNUM	2
	RETURNED_WFG	 	*returning_global_wfg;
	DeadLockState		 state;


	/* Used to return the result */
	FuncCallContext	*funcctx;
	TupleDesc		 tupdesc;
	AttInMetadata	*attinmeta;
	HeapTupleData	 tupleData;
	HeapTuple		 tuple = &tupleData;
	char			*values[COLNUM];
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
#undef COLNUM
}

/*
 * SQl Function
 *
 * SQL function name: pg_global_deadlock_check_describe_backend(IN int, OUT record)
 *
 * Input parameter is pid of the downstream database transaction backend.
 * Output tuple is (pid int, pgprocno int, lxid int)
 *
 * Application using global deadlock detection can issue this function
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
	Lock_PgprocArray();
	if (pid < 0)
		proc = MyProc;
	else
	{
		proc = find_pgproc(pid);
		if (proc == NULL)
		{
			Unlock_PgprocArray();
			elog(ERROR, "Specified pid %d is not in backend.", pid);
		}
	}
	pgprocno = proc->pgprocno;
	lxid = proc->lxid;
	Unlock_PgprocArray();

	for (ii = 0; ii < NCOLUMN; ii++)
		values[ii] = &Values[ii][0];
	tupd = CreateTemplateTupleDesc(NCOLUMN);
	ii = 1;
	TupleDescInitEntry(tupd, ii++, "pid", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "pgprocno", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, ii++, "lxid", NUMERICOID, -1, 0);
	ii = 0;
	snprintf(values[ii++], CHARLEN, "%d", pid);
	snprintf(values[ii++], CHARLEN, "%d", pgprocno);
	snprintf(values[ii++], CHARLEN, "%u", lxid);
	tuple =  BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);

	PG_RETURN_DATUM(result);

#undef NCOLUMN
#undef CHARLEN
}
	
/*
 * This check if external lock found in local database wait-for-graph is
 * already in the global wait-for-graph.
 *
 * This works when DeadlockCheckMode is DLCMODE_GLOBAL_AGAIN or DLCMODE_GLOBAL_ORIGIN.
 * In these modes, we should not check wait-for-graph beyond already-checked
 * external lock.
 */
static bool
external_lock_already_in_gwfg(GLOBAL_WFG *global_wfg, DEADLOCK_INFO_BUP *dl_info_bup)
{
	int			ii;
	uint64		my_system_identifier;

	if (global_wfg == NULL)
		return false;

	my_system_identifier = get_database_system_id();

	if (dl_info_bup->state != DS_EXTERNAL_LOCK)
		return false;
	for (ii = 0; ii < global_wfg->nLocalWfg; ii++)
	{
		LOCAL_WFG			*local_wfg;
		ExternalLockInfo	*external_lock;
		DEADLOCK_INFO		*dl_info;
		LOCKTAG				*dl_locktag;

		local_wfg = (LOCAL_WFG *)(global_wfg->local_wfg[ii]);
		if (local_wfg->database_system_identifier != my_system_identifier)
			continue;
		if (!(((LOCAL_WFG *)(global_wfg->local_wfg[ii]))->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
			continue;
		external_lock = local_wfg->external_lock;
		dl_info = &dl_info_bup->deadlock_info[dl_info_bup->nDeadlock_info - 1];
		dl_locktag = &dl_info->locktag;
		/* Check locktag against external lock in local wfg */
		if (dl_locktag->locktag_field1 != external_lock->pgprocno)
			continue;
		if (dl_locktag->locktag_field2 != external_lock->pid)
			continue;
		if (dl_locktag->locktag_field3 != external_lock->txnid)
			continue;
		if (dl_locktag->locktag_field4 != external_lock->serno)
			continue;
		if (dl_locktag->locktag_type != LOCKTAG_EXTERNAL)
			continue;
		if (dl_locktag->locktag_lockmethodid != DEFAULT_LOCKMETHOD)
			continue;
		return true;
	}
	return false;
}

/*
 * Acquire all the LWLocks for LOCK
 */
static void
hold_all_lockline(void)
{
	int ii;

	LWLockAcquire(LocalDeadLockCheckLock, LW_EXCLUSIVE);
	for (ii = 0; ii < NUM_LOCK_PARTITIONS; ii++)
		LWLockAcquire(LockHashPartitionLockByIndex(ii), LW_EXCLUSIVE);
}

/*
 * Release all the LWLocks for LOCK
 */
static void
release_all_lockline(void)
{
	int	ii;

	LWLockRelease(LocalDeadLockCheckLock);
	for (ii = 0; ii < NUM_LOCK_PARTITIONS; ii++)
		LWLockRelease(LockHashPartitionLockByIndex(ii));
}


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

/*
 * Caller does not have to acquire LWLock for ProcArrayLock.
 * To refer to the value of PGPROC found, the caller should acquire appropriate
 * LWLock for ProcArrayLock.
 */
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
	Lock_PgprocArray();

	for (ii = 0; ii < nDeadlockInfo; ii++)
	{
		proc = find_pgproc_pgprocno(info[ii].pgprocno);
		if (proc == NULL)
			goto instable;			/* No PGPROC found */

		Lock_Pgproc(proc);
		if ((proc->pid != info[ii].pid) ||
			(proc->waitLockMode != info[ii].lockmode) ||
			(memcmp(&(proc->waitLock->tag), &info[ii].locktag, sizeof(LOCKTAG))) ||
			(proc->lxid != info[ii].txid))
		{
			/*
			 * One of the following condition applies
			 * - PGPROC is running different process
			 * - PGPROC is waiting with different lock mode
			 * - PGPROC is waiting for different LOCK
			 * - PGPROC is running different TXN
			 */
			Unlock_Pgproc(proc);
			goto instable;
		}
		Unlock_Pgproc(proc);
	}
	Unlock_PgprocArray();
	return true;

instable:
	Unlock_PgprocArray();
	return false;
}

/*
 * Add new returne_wfg member to specified returned_wfg
 */
static RETURNED_WFG *
add_returned_wfg(RETURNED_WFG *dest, RETURNED_WFG *src, bool clean_opt)
{
	int		ii, jj;
	int		old_num;
	int		new_num;

	if ((src == NULL) || (src->nReturnedWfg == 0))
		return dest;
	if (dest == NULL)
		dest = palloc0(sizeof(RETURNED_WFG));
	old_num = dest->nReturnedWfg;
	new_num = old_num + src->nReturnedWfg;

	dest->state = (DeadLockState *)gdd_repalloc(dest->state, sizeof(DeadLockState) * new_num);
	dest->global_wfg_in_text = (char **)gdd_repalloc(dest->global_wfg_in_text, sizeof(char *) * new_num);
	for (ii = old_num, jj = 0; ii < new_num; ii++, jj++)
	{
		dest->state[ii] = src->state[jj];
		dest->global_wfg_in_text[ii] = pstrdup(src->global_wfg_in_text[jj]);
	}
	dest->nReturnedWfg = new_num;
	if (clean_opt)
		free_returned_wfg(src);
	return dest;
}

/*
 * Add new global wfg to specified returned wfg
 */
static RETURNED_WFG *
addGlobalWfgToReturnedWfg(RETURNED_WFG *returned_wfg, DeadLockState state, GLOBAL_WFG *global_wfg)
{
	char			*global_wfg_txt;

	if (returned_wfg == NULL)
		returned_wfg = palloc0(sizeof(RETURNED_WFG));
	global_wfg_txt = SerializeGlobalWfG(global_wfg);
	returned_wfg->nReturnedWfg++;
	returned_wfg->state = gdd_repalloc(returned_wfg->state, sizeof(DeadLockState) * returned_wfg->nReturnedWfg);
	returned_wfg->state[returned_wfg->nReturnedWfg - 1] = state;
	returned_wfg->global_wfg_in_text = gdd_repalloc(returned_wfg->global_wfg_in_text, sizeof(char *) * returned_wfg->nReturnedWfg);
	returned_wfg->global_wfg_in_text[returned_wfg->nReturnedWfg - 1] = global_wfg_txt;
	return returned_wfg;
}


/*
 * Determine the mode of global deadlock check and extract self node's local wait-for-graph and add backend informaton
 * to visited proc.
 *
 * This mode affects the behavior of DeadLockCheck().
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
	if (global_wfg->nLocalWfg <= 0 )
	{
		elog(WARNING, "The first or last local wait-for-graph was not compatible and could not deserialize.");
		return DLCMODE_ERROR;
	}
	rv = DLCMODE_GLOBAL_NEW;
	for (ii = 0; ii < global_wfg->nLocalWfg; ii++)
	{
		LOCAL_WFG *local_wfg;

		local_wfg = (LOCAL_WFG *)global_wfg->local_wfg[ii];
		if (local_wfg->database_system_identifier != my_database_system_id)
			continue;
		if (!(local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
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
			proc_wk = find_pgproc_pgprocno(info->pgprocno);
			if (proc_wk == NULL || proc_wk->pid != info->pid || proc_wk->lxid != info->txid)
				continue;

			nGlobalVisitedProcs++;
			globalVisitedProcs = (PGPROC **)gdd_repalloc(globalVisitedProcs, sizeof(PGPROC *) * nGlobalVisitedProcs);
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
		globalVisitedExternalLock
			= (ExternalLockInfo **)gdd_repalloc(globalVisitedExternalLock,
										   sizeof(ExternalLockInfo *) * nGlobalVisitedExternalLock);
		globalVisitedExternalLock[nGlobalVisitedExternalLock -1] = local_wfg->external_lock;
	}
	return rv;
}

static void
free_local_wfg(LOCAL_WFG *local_wfg)
{
	if (!local_wfg)
		return;
	if (local_wfg->backend_activity)
	{
		int	ii;

		for (ii = 0; ii < local_wfg->nDeadlockInfo; ii++)
		{
			if (local_wfg->backend_activity[ii])
				pfree(local_wfg->backend_activity[ii]);
		}
		pfree(local_wfg->backend_activity);
	}
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

#ifdef GDD_DEBUG
void
free_global_wfg(GLOBAL_WFG *global_wfg)
{
	int ii;

	if (!global_wfg)
		return;
	for (ii = 0; ii < global_wfg->nLocalWfg; ii++)
		free_local_wfg(global_wfg->local_wfg[ii]);
	pfree(global_wfg->local_wfg);
	pfree(global_wfg);
}
#endif

static void
add_backend_activities_local_wfg(LOCAL_WFG *local_wfg)
{
	int	ii;
	
	for (ii = 0; ii < local_wfg->nDeadlockInfo; ii++)
		local_wfg->backend_activity[ii] = pstrdup(pgstat_get_backend_current_activity(local_wfg->deadlock_info[ii].pid, false));
}

static void
free_returned_wfg(RETURNED_WFG *r_wfg)
{
	int	ii;

	if (r_wfg == NULL)
		return;
	if (r_wfg->state)
		pfree(r_wfg->state);
	if (r_wfg->global_wfg_in_text)
	{
		for (ii = 0; ii < r_wfg->nReturnedWfg; ii++)
		{
			if (r_wfg->global_wfg_in_text[ii])
				pfree(r_wfg->global_wfg_in_text[ii]);
		}
		pfree(r_wfg->global_wfg_in_text);
	}
	pfree(r_wfg);
}

static LOCAL_WFG *
copy_local_wfg(LOCAL_WFG *l_wfg)
{
	LOCAL_WFG	*copied;
	int	ii;

	copied = (LOCAL_WFG *)palloc(sizeof(LOCAL_WFG));
	memcpy(copied, l_wfg, sizeof(LOCAL_WFG));
	copied->deadlock_info = (DEADLOCK_INFO *)palloc(sizeof(DEADLOCK_INFO) * l_wfg->nDeadlockInfo);
	memcpy(copied->deadlock_info, l_wfg->deadlock_info, sizeof(DEADLOCK_INFO) * l_wfg->nDeadlockInfo);
	copied->external_lock = (ExternalLockInfo *)palloc(sizeof(ExternalLockInfo));
	memcpy(copied->external_lock, l_wfg->external_lock, sizeof(ExternalLockInfo));
	copied->external_lock->dsn = pstrdup(l_wfg->external_lock->dsn);
	copied->backend_activity = (char **)palloc0(sizeof(char *) * l_wfg->nDeadlockInfo);
	for (ii = 0; ii < l_wfg->nDeadlockInfo; ii++)
	{
		if (l_wfg->backend_activity[ii])
			copied->backend_activity[ii] = pstrdup(l_wfg->backend_activity[ii]);
	}
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

	copied->nLocalWfg = g_wfg->nLocalWfg;
	copied->local_wfg = (LOCAL_WFG **)palloc(sizeof(LOCAL_WFG *) * g_wfg->nLocalWfg);
	for (ii = 0; ii < g_wfg->nLocalWfg; ii++)
		copied->local_wfg[ii] = copy_local_wfg((g_wfg->local_wfg[ii]));
	return copied;
}

#ifdef GDD_DEBUG
static void
print_global_wfg(StringInfo out, GLOBAL_WFG *g_wfg)
{
	int 	 ii;
	int		 arraymax;

	appendStringInfo(out, "\n**** STARTING GLOBAL_WFG %016lx ***************************", (uint64)g_wfg);
	if (g_wfg == NULL)
	{
		appendStringInfo(out, "\nNo global wait-for-graph.");
		return;
	}
	appendStringInfo(out, "\nnLocalWfg: %d", g_wfg->nLocalWfg);
	if (g_wfg->nLocalWfg > GDD_ARRAY_MAX)
	{
		appendStringInfo(out, "\n** nLocalWfg: %d exceeds max value for debug. Printing only first %d **",
				g_wfg->nLocalWfg, GDD_ARRAY_MAX);
		arraymax = GDD_ARRAY_MAX;
	}
	else
		arraymax = g_wfg->nLocalWfg;
	for (ii = 0; ii < arraymax; ii++)
	{
		appendStringInfo(out, "\nlocal_wfg[%d] **********", ii);
		print_local_wfg(out, g_wfg->local_wfg[ii], ii, g_wfg->nLocalWfg);
	}
	appendStringInfo(out, "**** END OF GLOBAL_WFG %016lx ***********************\n", (uint64)g_wfg);
}

static void
print_local_wfg(StringInfo out, LOCAL_WFG *l_wfg, int idx, int total)
{
	int ii;
	int	arraymax;

	appendStringInfo(out, "\n**** STARTING LOCAL_WFG %016lx idx: %d/%d ******************",
			   (uint64)l_wfg, idx, total);
	appendStringInfo(out, "\nmagic: 0x%08x, flag: 0x%08x, database_syste_identifier: 0x%016lx",
			   WfG_LOCAL_MAGIC, l_wfg->local_wfg_flag, l_wfg->database_system_identifier);
	appendStringInfo(out, "\nvisitedProcPid: %d, visitedProcPgorocni: %d, visitedProcLxid: %u",
			   l_wfg->visitedProcPid, l_wfg->visitedProcPgprocno, l_wfg->visitedProcLxid);
	appendStringInfo(out, "\nnDeadlockInfo: %d", l_wfg->nDeadlockInfo);
	if (l_wfg->nDeadlockInfo > GDD_ARRAY_MAX)
	{
		appendStringInfo(out, "\n** nDeadlockInfo: %d exceeds max value for debug. Printing only first %d **",
				   l_wfg->nDeadlockInfo, GDD_ARRAY_MAX);
		arraymax = GDD_ARRAY_MAX;
	}
	else
		arraymax = l_wfg->nDeadlockInfo;

	for (ii = 0; ii < arraymax; ii++)
		print_deadlock_info(out, &(l_wfg->deadlock_info[ii]), ii, l_wfg->nDeadlockInfo);
	if (l_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
		print_external_lock_info(out, l_wfg->external_lock);
	else
		appendStringInfo(out, "\n====== EXTERNAL_LOCK: NULL: Not set ========");
	appendStringInfo(out, "\n**** END OF LOCAL_WFG %016lx idx: %d ******************", (uint64)l_wfg, idx);
}

static void
print_external_lock_info(StringInfo out, ExternalLockInfo *e)
{
	appendStringInfo(out, "====== EXTERNAL_LOCK: %016ld: Not set ========\n", (uint64)e);
	appendStringInfo(out, "pid: %d, pgprocno: %d, txnid: %u, serno: %d,\n", e->pid, e->pgprocno, e->txnid, e->serno);
	appendStringInfo(out, "dsn: '%s'\n", e->dsn);
	appendStringInfo(out, "target_pid: %d, target_pgprocno: %d, target_txn: %u\n", e->target_pid, e->target_pgprocno, e->target_txn);
}


static void
print_deadlock_info(StringInfo out, DEADLOCK_INFO *info, int idx, int total)
{
	appendStringInfo(out, "---- STARTING DEADLOCK_INFO %016lx idx: %d/%d ------------------\n",
			   		 (uint64)info, idx, total);
	appendStringInfo(out, "LOCKTAG: field1: %d, field2: %d, field3: %d, field4: %d, type: %d (%s), methodid: %d\n",
			   		 info->locktag.locktag_field1,
			   		 info->locktag.locktag_field2,
			   		 info->locktag.locktag_field3,
			   		 info->locktag.locktag_field4,
			   		 info->locktag.locktag_type,
			   		 locktagTypeName(info->locktag.locktag_type),
			   		 info->locktag.locktag_lockmethodid);
	appendStringInfo(out, "lockmode: %d (%s), pid: %d, pgprocno: %d, txid: %d\n",
			   		 info->lockmode,
			   		 GetLockmodeName(info->locktag.locktag_lockmethodid, info->lockmode),
			   		 info->pid,
			   		 info->pgprocno,
			   		 info->txid);
}
#endif /* GDD_DEBUG */


/*
 * Serialize/Deserialize global/local wait-for-graph
 */

static char *
getAtoInt32(char *buf, int32 *value)
{
	int64 value64;

	buf = getAtoInt64(buf, &value64);
	*value = (int32)value64;
	return buf;
}

static char *
getAtoInt64(char *buf, int64 *value)
{
	long	v;

	if (!buf)
	{
		*value = 0;
		return NULL;
	}
	while (*buf < '0' || *buf > '9')
	{
		if (*buf == '-')
		{
			buf = getAtoInt64(buf + 1, value);
			*value *= -1;
			return buf;
		}
		buf++;
	}
	if (*buf == 0)
	{
		*value = 0;
		return NULL;
	}
	v = 0;
	while (*buf)
	{
		int ii;

		if (*buf >= '0' && *buf <= '9')
			ii = *buf - '0';
		else
		{
			*value = v;
			return buf;
		}
		v *= 10;
		v += ii;
		buf++;
	}
	*value = v;
	return NULL;
}

static char *
getAtoUint32(char *buf, uint32 *value)
{
	uint64 value64;

	buf = getAtoUint64(buf, &value64);
	*value = (uint32)value64;
	return buf;
}

static char *
getAtoUint16(char *buf, uint16 *value)
{
	uint64 value64;

	buf = getAtoUint64(buf, &value64);
	*value = (uint16)value64;
	return buf;
}

static char *
getAtoUint8(char *buf, uint8 *value)
{
	uint64 value64;

	buf = getAtoUint64(buf, &value64);
	*value = (uint8)value64;
	return buf;
}

static char *
getAtoUint64(char *buf, uint64 *value)
{
	uint64	v;

	if (!buf)
	{
		*value = 0;
		return NULL;
	}
	while (*buf < '0' || *buf > '9')
		buf++;
	if (*buf == 0)
	{
		*value = 0;
		return NULL;
	}
	v = 0;
	while (*buf)
	{
		int ii;

		if (*buf >= '0' && *buf <= '9')
			ii = *buf - '0';
		else
		{
			*value = v;
			return buf;
		}
		v *= 10;
		v += ii;
		buf++;
	}
	*value = v;
	return NULL;
}

static char *
getHexaToInt(char *buf, int *value)
{
	int	v;

	if (!buf)
	{
		*value = 0;
		return NULL;
	}
	while ((*buf < '0' || (*buf > '9' && *buf < 'A') || (*buf > 'F' && *buf < 'a') || (*buf > 'f')))
		buf++;
	if (*buf == 0)
	{
		*value = 0;
		return NULL;
	}
	v = 0;
	while (*buf)
	{
		int ii;

		if (*buf >= '0' && *buf <= '9')
			ii = *buf - '0';
		else if (*buf >= 'A' && *buf <= 'F')
			ii = *buf - 'A' + 10;
		else if (*buf >= 'a' && *buf <= 'f')
			ii = *buf - 'a' + 10;
		else
		{
			*value = v;
			return buf;
		}
		v = v << 4;
		v += ii;
		buf++;
	}
	*value = v;
	return NULL;
}

static char *
getHexaToLong(char *buf, long *value)
{
	long	v;

	if (!buf)
	{
		*value = 0;
		return NULL;
	}
	while ((*buf < '0' || (*buf > '9' && *buf < 'A') || (*buf > 'F' && *buf < 'a') || (*buf > 'f')))
		buf++;
	if (*buf == 0)
	{
		*value = 0;
		return NULL;
	}
	v = 0;
	while (*buf)
	{
		int ii;

		if (*buf >= '0' && *buf <= '9')
			ii = *buf - '0';
		else if (*buf >= 'A' && *buf <= 'F')
			ii = *buf - 'A' + 10;
		else if (*buf >= 'a' && *buf <= 'f')
			ii = *buf - 'a' + 10;
		else
		{
			*value = v;
			return buf;
		}
		v = v << 4;
		v += ii;
		buf++;
	}
	*value = v;
	return NULL;
}

static char *
getString(char *buf, char **value)
{
	StringInfoData v;

	initStringInfo(&v);
	while (*buf && *buf != '\'')
		buf++;
	if (!*buf)
	{
		*value = NULL;
		return NULL;
	}
	buf++;
	while(*buf)
	{
		if (*buf == '\'')
		{
			if (*(buf + 1) == '\'')
			{
				appendStringInfoChar(&v, *buf);
				buf += 2;
			}
			else
			{
				*value = v.data;
				return buf + 1;
			}
		}
		else
		{
			appendStringInfoChar(&v, *buf);
			buf++;
		}
	}
	/* No trainig quote found! Invalid format. */
	pfree(v.data);
	*value = NULL;
	return NULL;
}

static char *
findChar(char *buf, char c)
{
	while(*buf && *buf != c)
		buf++;
	if (!*buf)
		return NULL;
	return buf + 1;
}

static bool
CloseScan(char *buf)
{
	while(*buf == ' ' || *buf == '\t' || *buf == '\n')
		buf++;
	return *buf ? false : true;
}

static char *
normalizeString(char *buf)
{
	StringInfoData v;

	initStringInfo(&v);
	if (buf == NULL)
	{
		appendStringInfoString(&v, "''");
		return v.data;
	}
	appendStringInfoChar(&v, '\'');
	while(*buf)
	{
		if (*buf == '\'')
			appendStringInfoString(&v, "''");
		else
			appendStringInfoChar(&v, *buf);
		buf++;
	}
	appendStringInfoChar(&v, '\'');
	return v.data;
}

static char *
SerializeLocalWfG(LOCAL_WFG *local_wfg)
{
	StringInfoData	s;
	int				ii;

	initStringInfo(&s);
	/* Start */
	appendStringInfoString(&s, "( ");
	/* Magic */
	appendStringInfo(&s, "%08x %016lx %08x ",
			WfG_LOCAL_MAGIC,						/* Magic */
			local_wfg->database_system_identifier,	/* Daabase system id */
			local_wfg->local_wfg_flag);				/* Flag */

	/* Visited Proc */
	if (local_wfg->local_wfg_flag & WfG_HAS_VISITED_PROC)
		appendStringInfo(&s, "( %d %d %d ) ",
				local_wfg->visitedProcPid,
				local_wfg->visitedProcPgprocno,
				local_wfg->visitedProcLxid);

	/* Deadlock Info */
	/* 		Opening Paren  and nDedalockInfo */
	appendStringInfo(&s, "( %d ", local_wfg->nDeadlockInfo);

	/* Each deadlock info */
	for (ii = 0; ii < local_wfg->nDeadlockInfo; ii++)
	{
		DEADLOCK_INFO	*info = &(local_wfg->deadlock_info[ii]);
		appendStringInfo(&s, "( ( %d %d %d %d %d %d ) %d %d %d %d ) ",
				info->locktag.locktag_field1,		/* Locktag */
				info->locktag.locktag_field2,
				info->locktag.locktag_field3,
				info->locktag.locktag_field4,
				info->locktag.locktag_type,
				info->locktag.locktag_lockmethodid,
				info->lockmode,						/* lockmode, pid, pgprocno, txid */
				info->pid,
				info->pgprocno,
				info->txid);
	}
	/* Closing */
	appendStringInfoString(&s, ") ");

	/* Backend activities */
	/* Opening */
	appendStringInfoString(&s, "( ");
	for (ii = 0; ii < local_wfg->nDeadlockInfo; ii++)
		appendStringInfo(&s, " %s",
				normalizeString(local_wfg->backend_activity[ii]));
	/* Closing */
	appendStringInfoString(&s, " ) ");

	/* External Lock Info */
	if (local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
	{
		appendStringInfo(&s, "( %d %d %d %d %s %d %d %d )",
				local_wfg->external_lock->pid,
				local_wfg->external_lock->pgprocno,
				local_wfg->external_lock->txnid,
				local_wfg->external_lock->serno,
				normalizeString(local_wfg->external_lock->dsn),
				local_wfg->external_lock->target_pid,
				local_wfg->external_lock->target_pgprocno,
				local_wfg->external_lock->target_txn);
	}
	/* Closing */
	appendStringInfoString(&s, " )");
	return s.data;
}


static char *
DeserializeLocalWfG(char *buf, LOCAL_WFG **local_wfg)
{
	LOCAL_WFG	*l_wfg = (LOCAL_WFG *)palloc0(sizeof(LOCAL_WFG));
	int			 ii;
	int32		 magic;

	*local_wfg = NULL;
	FindChar(buf, '(');
	GetHexaToInt(buf, &magic);				/* Magic */
	if (magic != WfG_LOCAL_MAGIC)
		elog(ERROR, "Invalid Magic Number in serialized local wait-for-graph.");
	GetHexaToLong(buf, &l_wfg->database_system_identifier);	/* System ID */
	GetHexaToInt(buf, &l_wfg->local_wfg_flag);				/* Flag */
	if (l_wfg->local_wfg_flag & WfG_HAS_VISITED_PROC)
	{
		/* Visited Proc Info */
		FindChar(buf, '(');
		GetAtoInt32(buf, &l_wfg->visitedProcPid);
		GetAtoInt32(buf, &l_wfg->visitedProcPgprocno);
		GetAtoInt32(buf, &l_wfg->visitedProcLxid);
		FindChar(buf, ')');
	}
	/* Deadlock Info */
	FindChar(buf, '(');
	GetAtoInt32(buf, &l_wfg->nDeadlockInfo);
	if (l_wfg->nDeadlockInfo <= 0)
		elog(ERROR, "Invalid number of deadlock info in serialized wait-for-graph.");
	l_wfg->deadlock_info = (DEADLOCK_INFO *)palloc(sizeof(DEADLOCK_INFO) * l_wfg->nDeadlockInfo);
	for (ii = 0; ii < l_wfg->nDeadlockInfo; ii++)
	{
		DEADLOCK_INFO	*info = &(l_wfg->deadlock_info[ii]);
		FindChar(buf, '(');
		/* LOCKTAG */
		FindChar(buf, '(');
		GetAtoUint32(buf, &(info->locktag.locktag_field1));
		GetAtoUint32(buf, &(info->locktag.locktag_field2));
		GetAtoUint32(buf, &(info->locktag.locktag_field3));
		GetAtoUint16(buf, &(info->locktag.locktag_field4));
		GetAtoUint8(buf, &(info->locktag.locktag_type));
		GetAtoUint8(buf, &(info->locktag.locktag_lockmethodid));
		FindChar(buf, ')');
		/* Others */
		GetAtoInt32(buf, &(info->lockmode));
		GetAtoInt32(buf, &(info->pid));
		GetAtoInt32(buf, &(info->pgprocno));
		GetAtoUint32(buf, &(info->txid));
		FindChar(buf, ')');
	}
	FindChar(buf, ')');

	/* Backend Activities */
	FindChar(buf, '(');
	l_wfg->backend_activity = (char **)palloc(sizeof(char *) * l_wfg->nDeadlockInfo);
	for (ii = 0; ii < l_wfg->nDeadlockInfo; ii++)
		GetString(buf, &l_wfg->backend_activity[ii]);
	FindChar(buf, ')');

	/* ExternalLock */
	if (l_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
	{
		ExternalLockInfo *e_lock;

		e_lock = l_wfg->external_lock = (ExternalLockInfo *)palloc(sizeof(ExternalLockInfo));
		FindChar(buf, '(');
		GetAtoInt32(buf, &e_lock->pid);
		GetAtoInt32(buf, &e_lock->pgprocno);
		GetAtoUint32(buf, &e_lock->txnid);
		GetAtoInt32(buf, &e_lock->serno);
		GetString(buf, &e_lock->dsn);
		GetAtoInt32(buf, &e_lock->target_pid);
		GetAtoInt32(buf, &e_lock->target_pgprocno);
		GetAtoUint32(buf, &e_lock->target_txn);
		FindChar(buf, ')');
	}
	FindChar(buf, ')');
	*local_wfg = l_wfg;
	return buf;
}

static char *
SerializeGlobalWfG(GLOBAL_WFG *g_wfg)
{
	StringInfoData	s;
	int				ii;

	initStringInfo(&s);

	/* Start, open paren */
	appendStringInfoString(&s, "( ");
	appendStringInfo(&s, "%08x ", WfG_GLOBAL_MAGIC);

	/* Local WfG */
	appendStringInfo(&s, "( %d ", g_wfg->nLocalWfg);

	for (ii = 0; ii < g_wfg->nLocalWfg; ii++)
	{
		char	*l_wfg;

		/* Local Wfg in LOCAL_WFG */
		l_wfg = SerializeLocalWfG((LOCAL_WFG *)(g_wfg->local_wfg[ii]));
		appendStringInfoString(&s, l_wfg);
		pfree(l_wfg);
	}

	/* Closing Local WfG */
	appendStringInfoString(&s, " )");

	/* Closing Global WfG */

	appendStringInfoString(&s, " )");

	return s.data;
}

static GLOBAL_WFG *
DeserializeGlobalWfG(char *buf)
{
	GLOBAL_WFG	*g_wfg;
	int			 ii;
	int32		 magic;

	g_wfg = (GLOBAL_WFG *)palloc(sizeof(GLOBAL_WFG));
	FindChar(buf, '(');
	GetHexaToInt(buf, &magic);
	if (magic != WfG_GLOBAL_MAGIC)
		elog(ERROR, "Invalid MAGIC in global wait-for-graph.");
	FindChar(buf, '(');
	GetAtoInt32(buf, &g_wfg->nLocalWfg);
	if (g_wfg->nLocalWfg <= 0)
		elog(ERROR, "Negative local wait-for-graph in the global wait-for-graph.");
	g_wfg->local_wfg = (LOCAL_WFG **)palloc(sizeof(LOCAL_WFG *) * g_wfg->nLocalWfg);
	
	for (ii = 0; ii < g_wfg->nLocalWfg; ii++)
	{
		LOCAL_WFG	*l_wfg;
		buf = DeserializeLocalWfG(buf, &l_wfg);
		if (buf == NULL)
			elog(ERROR, "Inconsistent serialized global wait-for-graph.");
		g_wfg->local_wfg[ii] = l_wfg;
	}
	FindChar(buf, ')');
	FindChar(buf, ')');
	if (!CloseScan(buf))
		return NULL;
	return g_wfg;
}

static void
GlobalDeadlockReport_int(void)
{
	StringInfoData	 clientbuf;	/* errdetail for client */
	StringInfoData	 logbuf;	/* errdetail for server log */
	StringInfoData	 locktagbuf;
	StringInfoData	 local_clientbuf;	/* Local buffer common to clientbuf and localbuf */

	DEADLOCK_INFO	*info;
	DEADLOCK_INFO	*next_info;
	LOCAL_WFG		*local_wfg;
	int64			 my_db_id;
	int64			 next_db_id;
	int				 ii;

	if (global_deadlock_info == NULL)
		elog(ERROR, "No global deadlock info was found.\n");

	initStringInfo(&clientbuf);
	initStringInfo(&logbuf);
	initStringInfo(&locktagbuf);
	initStringInfo(&local_clientbuf);

	for (ii = 0; ii < global_deadlock_info->nLocalWfg; ii++)
	{
		int				 jj;

		/* Each local WFG */

		my_db_id = global_deadlock_info->local_wfg[ii]->database_system_identifier;

		if (ii != (global_deadlock_info->nLocalWfg - 1))
			next_db_id = global_deadlock_info->local_wfg[ii + 1]->database_system_identifier;
		else
			next_db_id = global_deadlock_info->local_wfg[0]->database_system_identifier;
		local_wfg = global_deadlock_info->local_wfg[ii];

		resetStringInfo(&local_clientbuf);
		appendStringInfo(&local_clientbuf,
						_("\nDeadlock info for Database system id: %016lx,  "),
						my_db_id);

		for (jj = 0; jj < local_wfg->nDeadlockInfo; jj++)
		{
			resetStringInfo(&locktagbuf);
			info = &local_wfg->deadlock_info[jj];

			DescribeLockTag(&locktagbuf, &info->locktag);
			if (local_wfg->deadlock_info[jj].locktag.locktag_type == LOCKTAG_EXTERNAL)
			{
				/*
				 * Please note that last cyle information is included in global deadlock info.
				 * We don't have to visit the first menber of local wait-for-graph if no
				 * external lock is involved.
				 */
				ExternalLockInfo	*ext_lock = global_deadlock_info->local_wfg[ii]->external_lock;
				info = &local_wfg->deadlock_info[jj];

				appendStringInfo(&local_clientbuf,
								_("Process %d waits for remote database %016lx, process %d.  "),
								info->pid, next_db_id, ext_lock->target_pid);
			}
			else
			{
				Assert(jj < (local_wfg->nDeadlockInfo - 1));

				next_info = &local_wfg->deadlock_info[jj + 1];
				appendStringInfo(&local_clientbuf,
							_("Process %d waits for %s on %s; blocked by process %d.  "),
							info->pid,
							GetLockmodeName(info->locktag.locktag_lockmethodid,
											info->lockmode),
							locktagbuf.data,
							next_info->pid);
			}
		}
		appendStringInfoString(&clientbuf, local_clientbuf.data);
		appendStringInfoString(&logbuf, local_clientbuf.data);
		for (jj = 0; jj < local_wfg->nDeadlockInfo; jj++)
		{
			appendStringInfo(&logbuf,
						    _("Process %d query: \"%s\" "),
						    local_wfg->deadlock_info[jj].pid,
						    local_wfg->backend_activity[jj]);
		}
	}

	/*
	 * K.Suzuki: At present, we are reporting same deadlock to pgstat.
	 */
	pgstat_report_deadlock();

	/* Until query datails are available, client message is the same as server log.  TDB */
	ereport(ERROR,
			(errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
			 errmsg("global deadlock detected"),
			 errdetail_internal("%s", clientbuf.data),
			 errdetail_log("%s", logbuf.data),
			 errhint("See server log for query details.")));
}

static void *
gdd_repalloc(void *pointer, Size size)
{
	if (pointer == NULL)
		return palloc(size);
	else
		return repalloc(pointer, size);
}
