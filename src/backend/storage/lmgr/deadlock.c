/*
 * K.Suzuki visitedProcs をバックアップして WfG に入れる必要がある。これは、
 * Global WfG の起点のノードだけでいいと思う。ここしか使わないから。
 *
 * 実際使うのはこの起点の最初の pgproc のみ。global cycle のチェックはこれしか
 * 使わない。
 * FLAG に WfG_HAS_VISITED_PROC 0x00000002 を追加して serializable/deserializable のコードを変更する。
 */

/*-------------------------------------------------------------------------
 *
 * deadlock.c
 *	  POSTGRES deadlock detection code
 *
 * See src/backend/storage/lmgr/README for a description of the deadlock
 * detection and resolution algorithms.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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

static LOCAL_WFG *BuildLocalWfG(PGPROC *origin);
static void  free_local_wfg(LOCAL_WFG *local_wfg);
static void hold_all_lockline(void);
static void release_all_lockline(void);
static void release_all_lockline(void);
#ifdef DEBUG_DEADLOCK
static void PrintLockQueue(LOCK *lock, const char *info);
#endif
static bool external_lock_is_same(ExternalLockInfo *one, ExternalLockInfo *two);
static StringInfo SerializeLocalWfG(LOCAL_WFG *local_wfg);
static LOCAL_WFG *DeserializeLocalWfG(char *buf);
static GLOBAL_WFG *AddToGlobalWfG(GLOBAL_WFG *g_wfg, LOCAL_WFG *local_wfg);
static StringInfo SerializeGlobalWfG(GLOBAL_WFG *g_wfg);
static GLOBAL_WFG *DeserializeGlobalWfG(char *buf);
static PGPROC *find_pgproc(int pid);

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

/*
 * Working space for the deadlock detector
 */

/* Workspace for FindLockCycle */
static PGPROC **visitedProcs;	/* Array of visited procs */
static int	nVisitedProcs;

/*
 * K.Suzuki additional visited proc list from global WfG
 *
 * In checking global cycle, we need process information of the origin of WfG.
 * This is taken from the first member of visited Procs array.
 */
static bool				isGlobalCheck = false;	/* True if the following three entires are valid */
static int	 			wfgProc = 0;			/* pid */
static int	 			wfgPgprocno = 0;		/* pgprocno */
static TransactionId	wfgTxid = 0;			/* transaction id */

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
 */
DeadLockState
DeadLockCheck(PGPROC *proc)
{
	int			i,
				j;
	DeadLockState	status;

	/* Initialize to "no constraints" */
	nCurConstraints = 0;
	nPossibleConstraints = 0;
	nWaitOrders = 0;

	/* Initialize to not blocked by an autovacuum worker */
	blocking_autovacuum_proc = NULL;

	/* Search for deadlocks and possible fixes */
	status = DeadLockCheckRecurse(proc);
	/*
	 * K.Suzuki: external lock 検出時の処理を追加
	 */
	if (status == DS_HARD_DEADLOCK || status == DS_EXTERNAL_LOCK)
	{
		/*
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
		return DS_EXTERNAL_LOCK;
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
		if (status != DS_HARD_DEADLOCK && status != DS_EXTERNAL_LOCK)
			return status;		/* found a valid solution! */
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
 *     -2: configuration に external lock が含まれている。
 *			the configuration has an external lock at the edge of the graph.
 *		>0: the configuration has one or more soft deadlocks
 *
 * In the soft-deadlock case, one of the soft cycles is chosen arbitrarily
 * and a list of its soft edges is returned beginning at
 * possibleConstraints+nPossibleConstraints.  The return value is the
 * number of soft edges.
 *--------------------
 */
/*
 * K.Suzuki: WIP まだここのコード及びここから呼び出される関数のコードは見ていない。
 *			 external lock の検出及びこれに伴う返却値はこれから。
 */
static int
TestConfiguration(PGPROC *startProc)
{
	int			softFound = 0;
	EDGE	   *softEdges = possibleConstraints + nPossibleConstraints;
	int			nSoftEdges;
	int			i;

	/*
	 * Make sure we have room for FindLockCycle's output.
	 */
	if (nPossibleConstraints + MaxBackends > maxPossibleConstraints)
		return -1;

	/*
	 * K.Suzuki: ExpandConstraints() も、見て見る必要がある。
	 *
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
	/*
	 * K.Suzuki: 以下でも、external lock の検出を追加する必要がある。
	 *			 FindLockCycle() も見ておく必要がある。この戻り値も external lock
	 *			 を反映しておく必要がある。
	 */
	for (i = 0; i < nCurConstraints; i++)
	{
		if (FindLockCycle(curConstraints[i].waiter, softEdges, &nSoftEdges))
		{
			if (nSoftEdges == 0)
				return -1;		/* hard deadlock detected */
			softFound = nSoftEdges;
		}
		if (FindLockCycle(curConstraints[i].blocker, softEdges, &nSoftEdges))
		{
			if (nSoftEdges == 0)
				return -1;		/* hard deadlock detected */
			softFound = nSoftEdges;
		}
	}
	if (FindLockCycle(startProc, softEdges, &nSoftEdges))
	{
		if (nSoftEdges == 0)
			return -1;			/* hard deadlock detected */
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
	int			i;
	dlist_iter	iter;

	/*
	 * If this process is a lock group member, check the leader instead. (Note
	 * that we might be the leader, in which case this is a no-op.)
	 */
	if (checkProc->lockGroupLeader != NULL)
		checkProc = checkProc->lockGroupLeader;

	/*
	 * Have we already seen this proc?
	 */
	if (isGlobalCheck)
	{
		if (wfgProc == checkProc->pid &&
			wfgPgprocno == checkProc->pgprocno &&
			wfgTxid == checkProc->lxid)
			return DS_HARD_DEADLOCK;
	}
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

				return isGlobalCheck ? DS_NO_DEADLOCK : DS_HARD_DEADLOCK;
			}

			/*
			 * Otherwise, we have a cycle but it does not include the start
			 * point, so say "no deadlock".
			 */
			return DS_NO_DEADLOCK;
		}
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
			/*
			 * K.Suzuki: 以下、戻る前に DS_HARD_DEADLOCK 時のように後処理がいるかも。
			 *			 後で WfG を作らないといけないので。
			 */
			return DS_EXTERNAL_LOCK;
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
		if (state == DS_HARD_DEADLOCK || state == DS_EXTERNAL_LOCK)
			return state;
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
			if (state == DS_HARD_DEADLOCK || state == DS_EXTERNAL_LOCK)
				return state;
		}
	}

	return DS_NO_DEADLOCK;
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

					/* This proc hard-blocks checkProc */
					state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
					if (state == DS_HARD_DEADLOCK || DS_EXTERNAL_LOCK)
					{
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

						return state;
					}

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

				/* This proc soft-blocks checkProc */
				state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
				if (state == DS_HARD_DEADLOCK || DS_EXTERNAL_LOCK)
				{
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

				/* This proc soft-blocks checkProc */
				state = FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges);
				if (state == DS_HARD_DEADLOCK || state == DS_EXTERNAL_LOCK)
				{
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
			}

			proc = (PGPROC *) proc->links.next;
		}
	}

	/*
	 * No conflict detected here.
	 */
	return false;
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

static LOCAL_WFG *
BuildLocalWfG(PGPROC *origin)
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
	local_wfg->nDeadlockInfo = nDeadlockDetails;
	local_wfg->deadlock_info = (DEADLOCK_INFO *)palloc(sizeof(DEADLOCK_INFO) * nDeadlockDetails);
	for (ii = 0; ii < nDeadlockDetails; ii++)
	{
		memcpy(&local_wfg->deadlock_info[ii], &deadlockDetails[ii], sizeof(DEADLOCK_INFO));
	}
	if (local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
		local_wfg->external_lock = GetExternalLockProperties(&deadlockDetails[nDeadlockDetails -1].locktag);
	else
		local_wfg->external_lock = NULL;
	local_wfg->local_wfg_trailor = WfG_LOCAL_MAGIC;

	return local_wfg;
}

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
		appendBinaryStringInfoInt16(str, eLockInfo->serno);
		appendBinaryStringInfoInt32(str, eLockInfo->target_pid);
		appendBinaryStringInfoInt32(str, eLockInfo->target_pgprocno);
		appendBinaryStringInfoInt32(str, eLockInfo->target_txn);
		dsnlen = ((strlen(eLockInfo->dsn) + 4)/4) * 4;
		dsn = (char *)palloc(dsnlen);
		memset(dsn, 0, dsnlen);
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
	Extract32(buf, &wfg->nDeadlockInfo);

	/* visitedProc */
	if (wfg->local_wfg_flag & WfG_HAS_VISITED_PROC)
	{
		Extract32(buf, &wfg->visitedProcPid);
		Extract32(buf, &wfg->visitedProcPgprocno);
		Extract32(buf, &wfg->visitedProcLxid);
	}

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

	buf[0] = (value & 0xff00000000000000) >> 56;
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

	buf[0] = (value & 0xff000000) >> 24;
	buf[1] = (value & 0x00ff0000) >> 16;
	buf[2] = (value & 0x0000ff00) >> 8;
	buf[3] =  value & 0x000000ff;

	appendBinaryStringInfo(str, buf, 4);
}

static void
appendBinaryStringInfoInt16(StringInfo str, int16 value)
{
	char buf[2];

	buf[0] = (value >> 8);
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
	s[0] = (value >> 24);
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

	*value  = (int64)(*buf++) << 56;
	*value |= (int64)(*buf++) << 48;
	*value |= (int64)(*buf++) << 40;
	*value |= (int64)(*buf++) << 32;
	*value |= (int64)(*buf++) << 24;
	*value |= (int64)(*buf++) << 16;
	*value |= (int64)(*buf++) << 8;
	*value |= (int64)(*buf++);
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

	*value = (int32)(*buf++) << 24;
	*value |= (int32)(*buf++) << 16;
	*value |= (int32)(*buf++) << 8;
	*value |= (int32)(*buf++);
	return buf;
}

static char *
extractUint32(char *buf, uint32 *value)
{

	*value = (uint32)(*buf++) << 24;
	*value |= (uint32)(*buf++) << 16;
	*value |= (uint32)(*buf++) << 8;
	*value |= (uint32)(*buf++);
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

	*value = (uint16)(*buf++) << 8;
	*value |= (uint16)(*buf++);
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
	int		 	 ii;

	output[size*2] = '\0';

	for (ii = 0; ii < size; ii++, input++)
	{
		int	cc;

		cc = *input;
		*output++ = hexadata[ (cc & 0x000000f0) >> 4 ];
		*output++ = hexadata[  cc & 0x0000000f ];
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
			cc = *input - '9';
		else if (*input >= 'A' && *input <= 'F')
			cc = *input - 'A' + 10;
		else if (*input >= 'a' && *input <= 'f')
			cc = *input - 'a' + 10;
		else
			goto err;
		cc = cc << 4;
		input++;
		if (*input >= '0' && *input <= '9')
			cc |= *input - '9';
		else if (*input >= 'A' && *input <= 'F')
			cc |= *input - 'A' + 10;
		else if (*input >= 'a' && *input <= 'f')
			cc |= *input - 'a' + 10;
		else
			goto err;
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
DeadLockState
GlobalDeadlockCheck(PGPROC *proc)
{
	DeadLockState	 state;
	LOCAL_WFG		*local_wfg;

	local_wfg = BuildLocalWfG(proc);
	if (local_wfg == NULL)
		return DS_NO_DEADLOCK;
	state = GlobalDeadlockCheckRemote(local_wfg, NULL, NULL);
	free_local_wfg(local_wfg);
	return state;
}

/*
 ********************************************************************************************
 *
 * K.Suzuki: Global deadlock detection part
 *
 * ここの内容ちょっとまだおかしい。rechec は WfG の起点の場合のみに必要だし、もし DEADLOCK
 * が見つかったらこれまでの WfG を返さないといけない。これは呼び出し元の責任にすればいいのか
 * な、、、
 *
 * この部分は来週以降に書くことにしよう。
 *
 * パラメータを追加し、text ** で形式の WfG を返すことにする。内容は hexa 変換した global wfg。
 * もし、ここが起点の場合はこれに NULL を指定しても構わない。どのみちここは使わないので。
 * これは palloc() で確保するので呼び出し元で pfree() すること。
 *
 ********************************************************************************************
 */

/*
 * If This is the star point to detect global deadlock, specify NULL to global_wfg.
 * Otherwise, specify received global wfg as global_wfg.
 */
DeadLockState
GlobalDeadlockCheckRemote(LOCAL_WFG *local_wfg, GLOBAL_WFG *global_wfg, char **returning_wfg)
{
	PGconn			*conn;
	PGresult		*res;
	StringInfo		 global_wfg_bin;
	char			*global_wfg_text = NULL;
	StringInfoData	 query;
	DeadLockState	 state;
	char			*state_s;
	char		    *returned_wfg;
	bool			 isStarting;

	isStarting = global_wfg ? false : true;
	if (!(local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
		return DS_NO_DEADLOCK;

	initStringInfo(&query);
	*returning_wfg = NULL;

	/* Build global deadlock */
	global_wfg = AddToGlobalWfG(global_wfg, (void *)local_wfg);
	global_wfg_bin = SerializeGlobalWfG(global_wfg);
	global_wfg_text = binary2text(global_wfg_bin->data, global_wfg_bin->len);

	/* Build remote query */
	appendStringInfo(&query, "SELECT * FROM pg_global_deadlock_check_from_remote('%s');", global_wfg_text);
	pfree(global_wfg_text);
	pfree(global_wfg_bin);

	/* Connect to remote */
	conn = PQconnectdb(local_wfg->external_lock->dsn);
	if (conn == NULL || PQstatus(conn) == CONNECTION_BAD)
	{
		elog(WARNING,
				"Could not connect to remote database '%s' not reacheable.",
				local_wfg->external_lock->dsn);
		state = DS_GLOBAL_ERROR;
		goto nodeadlock;
	}
	res = PQexec(conn, query.data);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(WARNING,
				"Cannot run remote query to check global deadlock.");
		state = DS_GLOBAL_ERROR;
		goto nodeadlock;
	}
	
	state_s = PQgetvalue(res, 0, 0);
	state = atoi(state_s);
	if (state == DS_NO_DEADLOCK || state == DS_GLOBAL_ERROR)
	{
		PQclear(res);
		PQfinish(conn);
		goto nodeadlock;
	}
	if (state != DS_HARD_DEADLOCK)
	{
		elog(WARNING,
				"Invalid return value from remote deadlock check, '%s'",
				local_wfg->external_lock->dsn);
		state = DS_GLOBAL_ERROR;
		goto nodeadlock;
	}

	/*
	 * Now DS_HARD_DEADLOCK was reported from the remote
	 */
	returned_wfg = PQgetvalue(res, 0, 1);
	PQclear(res);
	if (!isStarting)
	{
		GLOBAL_WFG		*returned_global_wfg;
		LOCAL_WFG		*returned_local_wfg;
		LOCAL_WFG		*local_wfg_here;
		DeadLockState	 state_here;
		PGPROC			*target_proc;
		/*
		 * K.Suzuki:
		 *	*この部分のコードは不完全。まず、ローカルノードで local_wfg が
		 * 	 stable であることを確認し、その後リモートノードに行かないと
		 *	 いけない。
		 *	*あと、Global wfg には自ノードの wfg も含まれている。現状ではOKだが、
		 *	 pg_global_deadlock_recheck() では自ノードチェックしたらその前の
		 *	 local_wfg を削除した上で次に回すようにしないといけない。
		 */
		/*
		 * This is the origin node.   Need to check if the global WfG cycle is stable.
		 */
		returned_global_wfg = DeserializeGlobalWfG(returned_wfg);
		if (returned_global_wfg == NULL)
			elog(ERROR, "Failed to reconstruct global wait-for-graph");
		returned_local_wfg = returned_global_wfg->local_wfg[0];

		hold_all_lockline();
		target_proc = &ProcGlobal->allProcs[returned_local_wfg->visitedProcPgprocno];

		if (!(returned_local_wfg->local_wfg_flag & WfG_HAS_VISITED_PROC) ||
			target_proc->pid != returned_local_wfg->visitedProcPid ||
			target_proc->lxid != returned_local_wfg->visitedProcLxid ||
			(!(returned_local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)))

			/* The initial process information changed (somehow) */
			elog(ERROR, "Inconsistent local_wfg");

		state_here = DeadLockCheck(target_proc);
		release_all_lockline();

		if (state_here != DS_EXTERNAL_LOCK)
			goto nodeadlock;

		local_wfg_here = BuildLocalWfG(target_proc);
		if (!(local_wfg_here->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
			goto nodeadlock;
		if (memcmp(&(returned_local_wfg->deadlock_info[returned_local_wfg->nDeadlockInfo - 1].locktag),
				   &(local_wfg_here->deadlock_info[local_wfg_here->nDeadlockInfo -1].locktag),
				   sizeof(LOCKTAG)) != 0)
			goto nodeadlock;
		/*
		 * Deadlock status looks consistent so far.
		 */
		/*
		 * Check if the current EXTERNAL LOCK is consistent with WfG
		 */
		resetStringInfo(&query);
		appendStringInfo(&query, "SELECT * FROM pg_global_deadock_recheck_from_remote(1, '%s');", returned_wfg);
		res = PQexec(conn, query.data);
		if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(WARNING,
					"Cannot run remote query to check global deadlock.");
			state = DS_GLOBAL_ERROR;
			goto nodeadlock;
		}
		state_s = PQgetvalue(res, 0, 0);
		state = atoi(state_s);
		*returning_wfg = NULL;
	}
	else
	{
		/*
		 * This is not the origine.  Need to set returned WfG before returning.
		 */
		*returning_wfg = returned_wfg;
	}
	PQclear(res);
	PQfinish(conn);
	if (query.data)
		pfree(query.data);
	if (global_wfg)
		pfree(global_wfg);
	if (global_wfg_text)
		pfree(global_wfg_text);
	if (returned_wfg)
		pfree(returned_wfg);
	return state;

nodeadlock:
	if (query.data)
		pfree(query.data);
	if (global_wfg)
		pfree(global_wfg);
	if (global_wfg_bin->data)
		pfree(global_wfg_bin->data);
	if (global_wfg_text)
		pfree(global_wfg_text);
	*returning_wfg = NULL;
	return state;
}

static DeadLockState
GlobalDeadlockRecheckRemote(int myPos, char *global_wfg_text, ExternalLockInfo *external_lock)
{
	PGconn			*conn = NULL;
	PGresult		*res = NULL;
	StringInfoData	 query;
	char			*state_s;
	DeadLockState	 state;

	initStringInfo(&query);
	appendStringInfo(&query, "SELECT pg_global_deadlock_recheck_from_remote(%d, '%s');", myPos, global_wfg_text);

	conn = PQconnectdb(external_lock->dsn);
	if (conn == NULL || PQstatus(conn) == CONNECTION_BAD)
	{
		elog(WARNING,
				"Could not connect to remote database '%s' not reacheable.",
				external_lock->dsn);
		state = DS_GLOBAL_ERROR;
		goto returning;
	}
	res = PQexec(conn, query.data);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(WARNING,
				"Cannot run remote query to check global deadlock.");
		state = DS_GLOBAL_ERROR;
		goto returning;
	}
	state_s = PQgetvalue(res, 0, 0);
	state = atoi(state_s);

returning:
	if (res)
		PQclear(res);
	if (conn)
		PQfinish(conn);
	return state;
}


/*
 * Function name
 *
 *	SQL function name: pg_global_deadlock_check_from_remote(IN test, OUT record)
 *
 *  Return value is one tuple: (dead_lock_state int, wfg text)
 */
PG_FUNCTION_INFO_V1(pg_global_deadlock_check_from_remote);

Datum
pg_global_deadlock_check_from_remote(PG_FUNCTION_ARGS)
{
	int64			 database_system_id;
	char 			*global_wfg_text;
	char		    *global_wfg_stream;
	GLOBAL_WFG 		*global_wfg;
	LOCAL_WFG		*local_wfg_origin;		/* local WfG of the origin node of the global WfG */
	LOCAL_WFG		*local_wfg_here;		/* local WfG of this node */
	PGPROC			*pgproc_in_passed_external_lock;
	ExternalLockInfo	*passed_external_lock;		/* External lock passed to this function */
	char		 	*returning_global_wfg = NULL;
	int				 size;

	/* Used to return the result */
	TupleDesc		 tupd;
	HeapTupleData	 tupleData;
	HeapTuple		 tuple = &tupleData;
	char			*values[2];

	Datum			 result;
	DeadLockState	 state = DS_NO_DEADLOCK;

	/* Initialize DeadLockCheck() global option */
	isGlobalCheck = false;
	/* Initialize the return value */
	values[0] = (char *)palloc(32);
	/* ID of this database */
	database_system_id = get_database_system_id();

	/*
	 * Deserialize received Global WFG
	 */
	global_wfg_text = PG_GETARG_CSTRING(0);
	global_wfg_stream = hexa2bin(global_wfg_text, strlen(global_wfg_text), &size);
	global_wfg = DeserializeGlobalWfG(global_wfg_stream);
	pfree(global_wfg_text);
	pfree(global_wfg_stream);
	global_wfg_stream = NULL;
	if (global_wfg == NULL)
	{
		state = DS_GLOBAL_ERROR;
		elog(WARNING, "The input parameter does not contain valid wait-for-graph data.");
		goto returning;
	}
	/*
	 * To continue, we need the external lock information of the last node, which lead to invoke this
	 * function.
	 */
	if (global_wfg->nLocalWfg <= 0 ||			/* No local WfG in the global WfG */
		global_wfg->is_text[0] == true ||		/* Could not deserialize the origin WfG */
		global_wfg->is_text[global_wfg->nLocalWfg - 1] == true) 	/* Could not deserialize the last local WfG */
	{
		/* Error, no deadlock */
		state = DS_GLOBAL_ERROR;
		elog(WARNING, "The last local wait-for-graph was not compatible and could not deserialize.");
		goto returning;
	}
	local_wfg_origin = (LOCAL_WFG *)(&global_wfg->local_wfg[0]);
	if (!(((LOCAL_WFG *)(&global_wfg->local_wfg[global_wfg->nLocalWfg - 1]))->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
	{
		/*
		 * The last local WfG must have external lock linked to here.  Id nor, it is system level error.
		 * Here, does not do the error but return as no deadlock so that the origin node can handle the error as
		 * timeout.
		 */
		state = DS_GLOBAL_ERROR;
		elog(WARNING, "The last local wait-for-graph does not contain valid external lock information.");
		goto returning;
	}
	passed_external_lock = ((LOCAL_WFG *)(&global_wfg->local_wfg[global_wfg->nLocalWfg - 1]))->external_lock;
	/* K.Suzuki: この後でやるべきこと
	 *
	 * (1) DeadLockCheck() の前処理として、isGlobalCheck と wfgProc 等の設定をする。
	 *		* このノードが origin なら、isGlobalCheck をtrueにして、wfgProc などを external lock から設定
	 *		* そうでないなら isGlobalCheck をfalseに、wfgProc などはゼロに
	 * (2) ターゲットの PGPROC を設定。
	 *		* external lock から得られる PGPROC の内容が実際と異なる場合は、この WfG が変わっているので、
	 *		  DS_NO_DEADLOCK を返す。
	 * (3) ターゲットの PGPROC で DeadLockCheck() を呼び出す
	 *		* DS_NO_DEADLOCK が帰ってきたら、これを返却する
	 *		* DS_HARD_DEADLOCK が帰ってきたら
	 *			* このノードが Origin なら、後の recheck のためにこのノードの local wfg を global wfg に含めて
	 *			  呼び出し元に返却する
	 *			* origin でないなら、これは単なるローカル deadlock であって、他の契機にチェックされるべきものなので、
	 *			  DS_NO_DEADLOCK を返却する。
	 *		* DS_EXTERNAL_LOCK が帰ってきたら
	 *			* GlobalDeadlockCheckRemote() を使って更に先のデッドロックを調べる
	 *				* DS_NO_DEADLOCK が返ってきたら、そのまま呼び出し元に返す。
	 *				* DS_HARD_DEADLOCK が返ってきたら、返ってきた WfG を含めて呼び出し元に返す。
	 *				* それ以外はエラー。WARNING 出して DS_NO_DEADLOCK を返す。
	 *		* それ以外の返却値の場合は、DS_NO_DEADLOCK を返す。
	 */

	/*
	 * Setup global check option for DeadLockCheck()
	 */
	if ((local_wfg_origin->database_system_identifier == database_system_id) &&
		(local_wfg_origin->local_wfg_flag & WfG_HAS_VISITED_PROC))
	{
		/*
		 * The node here is the origin node of this global WfG.
		 *
		 * Check the local deadlock with the origin PGPROC information.
		 */
		isGlobalCheck = true;
		wfgProc = local_wfg_origin->visitedProcPid;
		wfgPgprocno = local_wfg_origin->visitedProcPgprocno;
		wfgTxid = local_wfg_origin->visitedProcLxid;
	}
	else
	{
		isGlobalCheck = false;
		wfgProc = 0;
		wfgPgprocno = -1;
		wfgTxid = 0;
	}
	/*
	 * Setup external lock and the target PGPROC and check if it is stable.
	 */
	hold_all_lockline();
	pgproc_in_passed_external_lock = &ProcGlobal->allProcs[passed_external_lock->target_pgprocno];

	if (pgproc_in_passed_external_lock->pid != wfgProc ||
		pgproc_in_passed_external_lock->lxid != wfgTxid)
	{
		/*
		 * Process or transaction status changed from EXTERNAL LOCK.   This WfG is not stable and is not
		 * a part of a global deadlock.
		 */
		release_all_lockline();
		state = DS_NO_DEADLOCK;
		goto returning;
	}

	/*
	 * Now the status is stable so far and this WfG may be a part of a global deadlock.
	 */
	/*
	 * Check local deadlock.
	 */
	state = DeadLockCheck(pgproc_in_passed_external_lock);
	release_all_lockline();
	if (state != DS_HARD_DEADLOCK && state != DS_EXTERNAL_LOCK)
	{
		/* No deadlock detected */
		release_all_lockline();
		state = DS_NO_DEADLOCK;
		goto returning;
	}
	else if (state == DS_HARD_DEADLOCK)
	{
		if (!isGlobalCheck)
		{
			/* This is just a local deadlock.   This should be handled in different occasion. */
			release_all_lockline();
			state = DS_NO_DEADLOCK;
			goto returning;
		}
		else
		{
			StringInfo	 global_wfg_stream;
			char		*global_wfg_text;

			/* Found global deadlock here.  For recheck, we need to return all the global WfG */

			/* Build the global WfG to return */
			local_wfg_here = BuildLocalWfG(pgproc_in_passed_external_lock);
			global_wfg = AddToGlobalWfG(global_wfg, local_wfg_here);
			global_wfg_stream = SerializeGlobalWfG(global_wfg);
			global_wfg_text = binary2text(global_wfg_stream->data, global_wfg_stream->len);
			pfree(global_wfg_stream->data);
			pfree(global_wfg_stream);

			/* Build return value */
			tupd = CreateTemplateTupleDesc(2);
			TupleDescInitEntry(tupd, 1, "deadlock_state", INT4OID, -1, 0);
			TupleDescInitEntry(tupd, 2, "wfg_out", TEXTOID, -1, 0);
			snprintf(values[0], 32, "%d", DS_NO_DEADLOCK);
			values[1]=global_wfg_text;
			tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), values);
			result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
			pfree(values[0]);
			/* Return */
			PG_RETURN_DATUM(result);
		}
	}
	else
	{
		DeadLockState state_here;

		/* DS_EXTERNAL_LOCK */
		local_wfg_here = BuildLocalWfG(pgproc_in_passed_external_lock);
		state_here = GlobalDeadlockCheckRemote(local_wfg_here, global_wfg, &returning_global_wfg);
		if (state_here != DS_HARD_DEADLOCK)
		{
			state = DS_NO_DEADLOCK;
			goto returning;
		}
		else
		{
			Assert(returning_global_wfg);

			/* Build return value */
			tupd = CreateTemplateTupleDesc(2);
			TupleDescInitEntry(tupd, 1, "deadlock_state", INT4OID, -1, 0);
			TupleDescInitEntry(tupd, 2, "wfg_out", TEXTOID, -1, 0);
			snprintf(values[0], 32, "%d", DS_NO_DEADLOCK);
			values[1]=returning_global_wfg;
			tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), values);
			result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
			pfree(values[0]);
			/* Return */
			PG_RETURN_DATUM(result);
		}
	}

returning:
	tupd = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(tupd, 1, "deadlock_state", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 2, "wfg_out", TEXTOID, -1, 0);
	snprintf(values[0], 32, "%d", state);
	values[1]="";
	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), values);
	result = TupleGetDatum(TupleDescGetSlot(tupd), tuple);
	pfree(values[0]);
	PG_RETURN_DATUM(result);
}

/*
 * Function
 *
 * SQL function name: pg_global_deadlock_recheck(IN text, OUT record)
 *
 * Return value is one integer: DeadlockState
 */

PG_FUNCTION_INFO_V1(pg_global_deadlock_recheck_from_remote);

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
	LOCAL_WFG	*local_wfg_origin;
	LOCAL_WFG	*local_wfg_here;
	PGPROC		*pgproc_in_caller_external_lock;
	PGPROC		*target_proc;
	ExternalLockInfo	*caller_external_lock;
	bool		 isFinal;
	DeadLockState	state;

	
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
	 * Setup local_wfg for origin, caller and here.
	 * Also setup if this is the final node to recheck.
	 *
	 * Please note that nLocalWfg == 2 happens only when two databases are involved in
	 * candidate global WfG.   In this case, caller is the origin.
	 */
	local_wfg_origin = global_wfg->local_wfg[0];

	Assert(global_wfg->nLocalWfg >= 2);
	Assert(my_pos <= global_wfg->nLocalWfg);

	local_wfg_caller = global_wfg->local_wfg[my_pos - 1];
	local_wfg_here = global_wfg->local_wfg[my_pos];
	if (global_wfg->nLocalWfg <= my_pos + 1)
		isFinal = true;
	else
		isFinal = false;

	/* Check if myself is consistent with caller's external lock information */
	if (!(local_wfg_caller->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK))
	{
		state = DS_GLOBAL_ERROR;
		goto returning;
	}
	caller_external_lock = local_wfg_caller->external_lock;
	pgproc_in_caller_external_lock = &ProcGlobal->allProcs[caller_external_lock->target_pgprocno];
	hold_all_lockline();
	if ((caller_external_lock->target_pid != pgproc_in_caller_external_lock->pid) ||
		(caller_external_lock->target_txn != pgproc_in_caller_external_lock->lxid))
	{
		/* Target PGPROC's status changed. */
		release_all_lockline();
		state = DS_NO_DEADLOCK;
		goto returning;
	}
	target_proc = pgproc_in_caller_external_lock;
	/*
	 * Prepare for local DeadLockCheck()
	 */
	if (isFinal)
	{
		if ((local_wfg_origin->database_system_identifier == database_system_id) &&
			(local_wfg_origin->local_wfg_flag & WfG_HAS_VISITED_PROC))
		{
			release_all_lockline();
			state = DS_GLOBAL_ERROR;
			goto returning;
		}
		isGlobalCheck = true;
		wfgProc = local_wfg_origin->visitedProcPid;
		wfgPgprocno = local_wfg_origin->visitedProcPgprocno;
		wfgTxid = local_wfg_origin->visitedProcLxid;
	}
	else
	{
		isGlobalCheck = false;
		wfgProc = 0;
		wfgPgprocno = -1;
		wfgTxid = 0;
	}
	state = DeadLockCheck(target_proc);
	release_all_lockline();
	if (state == DS_NO_DEADLOCK)
		goto returning;
	else if (state == DS_HARD_DEADLOCK)
	{
		if (isFinal)
			/* Global deadlock confirmed */
			goto returning;
		else
		{
			/* Status changed.   This is just a local deadlock. */
			state = DS_NO_DEADLOCK;
			goto returning;
		}
	}
	else if (state == DS_EXTERNAL_LOCK)
	{
		if (isFinal)
		{
			/* Status changed. */
			state = DS_NO_DEADLOCK;
			goto returning;
		}
		else
		{
			/* Check further */
			LOCAL_WFG	*local_wfg_local;

			/* 次、NULL でいいのか？このノードが originである可能性はないのか？ */
			local_wfg_local = BuildLocalWfG(NULL);
			/*
			 *  次のノードへの external lock を調べる:
			 *		global wfg から導出したのと DeadLockCheck() から得られたものが
			 *		同じであるかどうかを確かめる。
			 */
			if (!(local_wfg_local->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK) ||
				!(local_wfg_here->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK) ||
				!external_lock_is_same(local_wfg_local->external_lock, local_wfg_here->external_lock))
			{
				elog(WARNING, "Invalid internal data on external lock.");
				state = DS_GLOBAL_ERROR;
				goto returning;
			}

			state = GlobalDeadlockRecheckRemote(my_pos + 1, global_wfg_text, local_wfg_local->external_lock);
			goto returning;
		}
	}
	else
	{
		/* Status changed */
		state = DS_NO_DEADLOCK;
		goto returning;
	}

returning:
	if (global_wfg_text)
		pfree(global_wfg_text);
	PG_RETURN_INT32(state);
}

/*
 * Function
 *
 * SQL function name: pg_global_deadlock_check_describe_backend(IN int, OUT record)
 *
 * Return value is one integer: DeadlockState
 */

PG_FUNCTION_INFO_V1(pg_global_deadlock_check_describe_backend);

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

static void
free_local_wfg(LOCAL_WFG *local_wfg)
{
	if (local_wfg->local_wfg_flag & WfG_HAS_EXTERNAL_LOCK)
		FreeExternalLockProperties(local_wfg->external_lock);
	if (local_wfg->nDeadlockInfo > 0)
		pfree(local_wfg->deadlock_info);
	pfree(local_wfg);
}

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
