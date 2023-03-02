/*-------------------------------------------------------------------------
 *
 * parallel_replay.c
 *      PostgreSQL write-ahead log manager
 *
 *
 * Portions Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/parallel_replay.c
 *
 * Infrastructure for parallel replay in recovery and streaming replication
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#ifdef PR_SKIP_REPLAY
#include <stdlib.h>
#endif
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "access/parallel_replay.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "pg_config.h"
#include "postmaster/postmaster.h"
#include "replication/origin.h"
#include "replication/walreceiver.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "utils/elog.h"


/*
 ************************************************************************************************
 * Reference to external object: mainly local to xlog.c
 ************************************************************************************************
 */
extern bool doRequestWalReceiverReply;	/* Need to see from parallel_replay.c */
/*
 ************************************************************************************************
 * Parallel replay GUC parameter
 ************************************************************************************************
 */

bool    parallel_replay = false;	/* If parallel replay is enabled								*/
int     num_preplay_workers;		/* Number of parallel replay worker								*/
									/* This included READER WORKER, which is actually				*/
									/* startup process.												*/
									/* We have four kinds of workers as follows:					*/
									/* READER WORKER: Actually startup process, reads WAL records	*/
									/*		and hands them to DISPATCHER WORKER						*/
									/* DISPATCHER WORKER: Receives all the WAL records from READER	*/
									/*		WORKER, analyze them and dispatches to TXN WORKER or	*/
									/*		BLOCK WORKER.											*/
									/* INVALID PAGE WORKER: Records history of invalid pages.		*/
									/* TXN WORKER: Replays WAL records without associated block		*/
									/*		data.													*/
									/* BLOCK WORKER: Replays WAL records with block data.  We can	*/
									/*		run more than one BLOCK WORKER.							*/
int     num_preplay_worker_queue;	/* Number of total queue for parallel replay.					*/
									/* Must be equal to or larger than num_replay_workers.			*/
int     PR_buf_size_mb;     		/* Number of blocks for preplay_xlogbuf_size */
bool	PR_test = false;			/* Flag to sync to GDB */
int		num_preplay_max_txn;		/* Max transaction at any given time in recovery. */
									/* Should be larger than max_connections of upstream database */
									/* If smaller than max_transactions, max_transaction value */
									/* will be used. */
int		num_preplay_max_txn;		/* If less than max_connections, max_connections will be taken */

/*
 * Flag to ignore error during parallel replay.
 *
 * Please note:
 *	To make this, we need to change the behavior of elog() series of functions (in fact, pg_unreachable()),
 *	This is done in "c.h" header files.
 */
#ifdef PR_IGNORE_REPLAY_ERROR
bool	pr_during_redo;
jmp_buf	pr_jmpbuf;

static void set_sigsegv_handler(void);
#endif

/*
 ************************************************************************************************
 * Parallel replay shared memory
 ************************************************************************************************
 */

static dsm_segment	*pr_shm_seg = NULL;

/* The following variables are initialized by PR_initShm() */
PR_shm   	*pr_shm = NULL;							/* Whole shared memory area */
static txn_wal_info_PR	*pr_txn_wal_info = NULL;	/* Chain of WAL for a transaction */
static txn_hash_el_PR	*pr_txn_hash = NULL;		/* Hash for TXN hash pool */
static txn_cell_PR		*pr_txn_cell_pool = NULL;	/* Pool of transaction track information */
static PR_invalidPages 	*pr_invalidPages = NULL;	/* Shared information of invalic page registration */
static PR_XLogHistory	*pr_history = NULL;			/* List of outstanding XLogRecord */
static PR_worker	*pr_worker = NULL;				/* Paralle replay workers */
static PR_queue		*pr_queue = NULL;				/* Dispatch queue element to workers */
static PR_buffer	*pr_buffer = NULL;				/* Buffer are for other usage */

#ifdef WAL_DEBUG
#define BufferDumpAreaLen	(4096 * 16)
static char	*buffer_dump_data;
#endif

/* My worker process info */
static int           my_worker_idx = 0;         /* Index of the worker.  Set when the worker starts. */
static PR_worker    *my_worker = NULL;      	/* My worker structure */

#ifdef WAL_DEBUG
/* For test code */
/*
 * Mainly usage:
 * 1) Test logs to dedicated directory ($PGDATA/$PR_SYNCSOCKDIR)
 * 2) Provide means to connect workers to gdb at run.
 *    Instruction to start GDB, attach workers to gdb, defining break point and
 *    continue to run the worker will be written to test logs.   Copy and paste
 *    these lines to your bash.
 */ 
static FILE *pr_debug_log = NULL;
static char	*pr_debug_signal_file = NULL;
static char	*debug_log_dir = NULL;
static char	*debug_log_file_path = NULL;
static char	*debug_log_file_link = NULL;
static int	 my_worker_idx;

#endif

/* Module-global definition */
#define PR_MAXPATHLEN	512

/*
 * Size boundary, address calculation and boundary adjustment
 */

#define MemBoundary         	(sizeof(void *))
#define addr_forward(p, s)  	((void *)((uint64)(p) + (Size)(s)))
#define addr_backward(p, s) 	((void *)((uint64)(p) - (Size)(s)))
#define addr_after(a, b)    	((uint64)(a) > (uint64)(b))
#define addr_after_eq(a, b) 	((uint64)(a) >= (uint64)(b))
#define addr_before(a, b)   	((uint64)(a) < (uint64)(b))
#define addr_before_eq(a, b)    ((uint64)(a) <= (uint64)(b))

/* Addresses are casted to unsigned INT64 to calculate the distance */
static Size
addr_difference(void *start, void * end)
{
	uint64 ustart = (uint64)start;
	uint64 uend = (uint64)end;

	if (ustart > uend)
		return (Size)(ustart - uend);
	else
		return (Size)(uend - ustart);
}

/* When O0 is ised, it conflicts with inline function. */
#if 1
#define INLINE static
#else
#define INLINE inline
#endif


/*
 ***********************************************************************************************
 * Directories and related definition for synchronization among workers.
 ***********************************************************************************************
 */

/* Macros */
#define PR_SOCKNAME_LEN 127
#define PR_SYNCSOCKDIR  "pr_syncsock"
#define PR_SYNCSOCKFMT  "%s/%s/pr_%06d"
#define PR_SYNC_MSG_SZ 15
#define PR_MB	(2 << 19)
/* Worker process synchronization */
static char *sync_sock_dir = NULL;
static char	 my_worker_msg[PR_SYNC_MSG_SZ + 1];
static Size	 my_worker_msg_sz = 0;

/* Shared Memory Functions */

/*
 ***********************************************************************************************
 * Internal Functions/macros
 ***********************************************************************************************
 */

/* Invalid Page info */
static void initInvalidPages(void);

/* Synch functions */
static bool set_syncFlag(int worker_id, uint32 flag_to_set);
static void PR_syncBeforeDispatch(void);

/* History info functions */
INLINE Size xlogHistorySize(void);
static void initXLogHistory(void);

/* Worker Functions */
INLINE Size worker_size(void);
static void initWorker(void);

/* Queue Functions */
INLINE Size queue_size(void);
static void initQueue(void);
static void initQueueElement(void);
#define queueData(el) ((el)->data)

/* Buffer Function */
INLINE Size			 buffer_size(void);
static void			 initBuffer(void);
static void			*PR_allocBuffer_int(Size sz, bool need_lock, bool retry_opt);
static PR_BufChunk	*prev_chunk(PR_BufChunk *chunk);
static void			 free_chunk(PR_BufChunk *chunk);
static PR_BufChunk	*alloc_chunk(Size sz);
static PR_BufChunk	*concat_next_chunk(PR_BufChunk *chunk);
static PR_BufChunk	*concat_prev_chunk(PR_BufChunk *chunk);
#define concat_surrounding_chunks(c)	concat_prev_chunk(concat_next_chunk(c))
#if 0
static PR_BufChunk  *search_chunk(void *addr);
#endif
static void			*retry_allocBuffer(Size sz, bool need_lock);
INLINE PR_BufChunk	*next_chunk(PR_BufChunk *chunk);
#if 0
static bool			 isOtherWorkersRunning(void);
#endif
#define CHUNK_OVERHEAD	(sizeof(PR_BufChunk) + sizeof(Size))

/*
 * Dispatch Data function
 */
static void	freeDispatchData(XLogDispatchData_PR *dispatch_data);
static bool checkRmgrTxnSync(RmgrId rmgrid, uint8 info);
static bool checkSyncBeforeDispatch(RmgrId rmgrid, uint8 info);
static bool isSyncBeforeDispatchNeeded(XLogReaderState *reader);
static bool checkSyncAfterDispatch(RmgrId rmgrid, uint8 info);
static bool isSyncAfterDispatchNeeded(XLogReaderState *reader);

/*
 * Transaction WAL info function
 */
static Size			 pr_txn_hash_size(void);
static void			 init_txn_hash(void);
static txn_cell_PR	*get_txn_cell(bool need_lock);
static void			 free_txn_cell(txn_cell_PR *txn_cell, bool need_lock);
static txn_cell_PR	*find_txn_cell(TransactionId xid, bool create, bool need_lock);
static void			 addDispatchDataToTxn(XLogDispatchData_PR *dispatch_data, bool need_lock);
static bool			 removeDispatchDataFromTxn(XLogDispatchData_PR *dispatch_data, bool need_lock);
static txn_cell_PR	*isTxnSyncNeeded(XLogDispatchData_PR *dispatch_data, XLogRecord *record, TransactionId *xid, bool remove_myself);
static void			 syncTxn(txn_cell_PR *txn_cell);
static void			 dispatchDataToXLogHistory(XLogDispatchData_PR *dispatch_data);
#define get_txn_hash(x) &pr_txn_hash[txn_hash_value(x)]

/* XLogReaderState/XLogRecord functions */
static int	blockHash(int spc, int db, int rel, int blk, int n_max);
static void getXLogRecordRmgrInfo(XLogReaderState *reader, RmgrId *rmgrid, uint8 *info);


/* Workerr Loop */
static int loop_exit_code = 0;			/* Use this value as exit code of each worker */

static int dispatcherWorkerLoop(void);
static int txnWorkerLoop(void);
static int invalidPageWorkerLoop(void);
static int blockWorkerLoop(void);

/* REDO function */

static void PR_redo(XLogDispatchData_PR	*data);


/* Test code */
#ifdef WAL_DEBUG
static void PRDebug_out(StringInfo s);
static bool dump_buffer(const char *funcname, StringInfo s, bool need_lock);
static void dump_chunk(PR_BufChunk *chunk, const char *funcname, StringInfo s, bool need_lock);
static char *now_timeofday(void);
static void dump_invalidPageData(XLogInvalidPageData_PR *page);
static char *forkNumName(ForkNumber forknum);
static char *invalidPageCmdName(PR_invalidPageCheckCmd cmd);
extern int PR_loop_num;
extern int PR_loop_count;
#endif

#ifdef PR_SKIP_REPLAY
static unsigned int	pr_rand_seed;
static void init_wait_time(void)
{
	pr_rand_seed = (unsigned int)my_worker_idx;
}
#define MAXWAIT 1000L
static void wait_a_bit(void)
{
	pg_usleep(((long)rand_r(&pr_rand_seed)) % MAXWAIT);
}
#endif

/*
 ************************************************************************************************
 * Synchronization Functions: synchronizing among worker processes
 ************************************************************************************************
 */

/*
 * Initialize socket directory - build socket directory if necessary.
 *
 * Synchronization is implemented using UDP socket.
 *
 * This hould be called before worker process is folked.
 */
void
PR_syncInitSockDir(void)
{
	struct stat statbuf;
	int			my_errno;
	int			rv;

	sync_sock_dir = (char *)palloc(PR_MAXPATHLEN);
	snprintf(sync_sock_dir, PR_MAXPATHLEN, "%s/%s", Unix_socket_directories, PR_SYNCSOCKDIR);
	rv = stat(sync_sock_dir, &statbuf);
	my_errno = errno;
	if (rv)
	{
		int	local_errno;

		/* sync sock directory sync error */
		if (my_errno != ENOENT)
		{
			PR_breakpoint();
			PR_failing();
			elog(PANIC, "Failed to stat PR debug directory, %s", strerror(my_errno));
		}
        rv = mkdir(sync_sock_dir, S_IRUSR|S_IWUSR|S_IXUSR);
        local_errno = errno;
        if (rv != 0)
		{
			PR_breakpoint();
			PR_failing();
            elog(PANIC, "Failed to create PR debug directory, %s", strerror(local_errno));
		}
    }
    else
	{
		/* Debug directory stat successfful */
		if ((statbuf.st_mode & S_IFDIR) == 0)
		{
			PR_breakpoint();
			/* It was not a directory */
			PR_failing();
			elog(PANIC, "%s must be a directory but not.", sync_sock_dir);
		}
	}
	/* Remove all the sockets */
	rmtree(sync_sock_dir, false);
}

/*
 * Clean all the subtree under sync socket dir.
 */
void
PR_syncFinishSockDir(void)
{
	rmtree(sync_sock_dir, true);
}

/*
 * Create socket and bind to the sync socket for the local worker process.
 * This should be called inside each worker process.
 */

/*
 * Structure for synchronization sockets and addresses.
 */
typedef struct syncSockInfo syncSockInfo;

struct syncSockInfo
{
	int					syncsock;
	struct sockaddr_un	sockaddr;
};

static syncSockInfo	*sync_sock_info = NULL;

void
PR_syncInit(void)
{
    int     rc;
    int     ii;

    Assert(pr_shm && sync_sock_dir);

    sync_sock_info = (syncSockInfo *)palloc(sizeof(syncSockInfo) * num_preplay_workers);

    for (ii = 0; ii < num_preplay_workers; ii++)
    {
        sprintf(sync_sock_info[ii].sockaddr.sun_path,
				PR_SYNCSOCKFMT, Unix_socket_directories, PR_SYNCSOCKDIR, ii);
        sync_sock_info[ii].sockaddr.sun_family = AF_UNIX;

		sync_sock_info[ii].syncsock = socket(AF_UNIX, SOCK_DGRAM, 0);
		if (sync_sock_info[ii].syncsock < 0)
		{
			PR_breakpoint();
			ereport(FATAL,
					(errcode_for_socket_access(),
					 errmsg("Could not create the socket: %m")));
		}

		if (ii == my_worker_idx)
		{
			rc = bind(sync_sock_info[ii].syncsock, &sync_sock_info[ii].sockaddr, sizeof(struct sockaddr_un));
			if (rc < 0)
			{
				PR_breakpoint();
				ereport(FATAL,
						(errcode_for_socket_access(),
						 errmsg("Could not bind the socket to \"%s\": %m",
							 sync_sock_info[ii].sockaddr.sun_path)));
			}
		}
    }
    snprintf(my_worker_msg, PR_SYNC_MSG_SZ, "%04d\n", my_worker_idx);
    my_worker_msg_sz = strlen(my_worker_msg);
}

/*
 * Close all the sync sock info
 */
void
PR_syncFinish(void)
{
	int	ii;

	Assert (pr_shm && sync_sock_dir);

	for (ii = 0; ii < num_preplay_workers; ii++)
		close(sync_sock_info[ii].syncsock);
	pfree(sync_sock_info);
	sync_sock_info = NULL;
}

/*
 * Send synchronization message to the target worker
 */
void
PR_sendSync(int worker_idx)
{
    Size    ll;

    Assert(pr_shm && worker_idx != my_worker_idx);

    ll = sendto(sync_sock_info[worker_idx].syncsock, my_worker_msg, my_worker_msg_sz, 0,
			&sync_sock_info[worker_idx].sockaddr, sizeof(struct sockaddr_un));
    if (ll != my_worker_msg_sz)
	{
		PR_breakpoint();
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Can not send sync message from worker %d to %d: %m",
					 my_worker_idx, worker_idx)));
	}
}

/*
 * Koichi: 修正箇所
 * 1) worker struct に "waiting" フラグを追加する
 * 2) Wait する前に、"waiting" フラグを立てる。
 * 3) PR_sendSync が "waiting" フラグを下ろす
 * 4) Wait する前に、全 worker がwait しているかどうかを調べる。全 worker が wait していたら、
 *    一種のdeadlock なので、エラーにする。
 * Walt flag は PR_worker の外のほうがいいように思われる。別な lock で一発で lock したほうが、
 * deadlock の可能性は低くなるように思われる。
 * この lock の取得／解放は PR_recvSync() だけで行えばいい。
 * Receive synchronization message from a worker.
 *
 * Return value indicates the worker sending synchronization message.
 *
 * This functions watches the timeout.   When timeout reaches, it checks if at least
 * one worker is running.   If not, reports error.
 */
int
PR_recvSync(void)
{
#define SYNC_RECV_BUF_SZ    64
    char    recv_buf[SYNC_RECV_BUF_SZ];
	int		ii;
	bool	is_some_running;
    Size    sz;

    Assert (pr_shm);

	SpinLockAcquire(&pr_shm->wait_flag_lock);

	pr_worker[my_worker_idx].worker_waiting = true;

	for (ii = 0, is_some_running = false; ii < num_preplay_workers; ii++)
	{
		if (pr_worker[ii].worker_waiting == false)
		{
			is_some_running = true;
			break;
		}
	}

	SpinLockRelease(&pr_shm->wait_flag_lock);

	if (!is_some_running)
	{
#ifdef WAL_DEBUG
		PRDebug_log("**** All the parallel replay workers are waiting for something.  Cannot move forward.***\n");
		PR_breakpoint();
#else
		PR_failing();
		elog(PANIC, "**** All the parallel replay workers are waiting for something.  Cannot move forward.***");
#endif
	}

    sz = recv(sync_sock_info[my_worker_idx].syncsock, recv_buf, SYNC_RECV_BUF_SZ, 0);

	SpinLockAcquire(&pr_shm->wait_flag_lock);
	pr_worker[my_worker_idx].worker_waiting = false;
	SpinLockRelease(&pr_shm->wait_flag_lock);

    if (sz < 0)
	{
		PR_breakpoint();
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Could not receive message.  worker %d: %m", my_worker_idx)));
	}
    return(atoi(recv_buf));
#undef SYNC_RECV_BUF_SZ
}

static bool
set_syncFlag(int worker_id, uint32 flag_to_set)
{
	Assert (worker_id != my_worker_idx);

	SpinLockAcquire(&pr_worker[worker_id].slock);
	if (pr_worker[worker_id].wait_dispatch)
	{
		SpinLockRelease(&pr_worker[worker_id].slock);
		return false;		/* Sync not needed */
	}
	else
	{
		pr_worker[worker_id].flags |= flag_to_set;
		SpinLockRelease(&pr_worker[worker_id].slock);
		return true;		/* Sync needed */
	}
}
/*
 * This is called only by the READER worker
 *
 * Workers should continue to handle dispatched XLogRecords.   When no queue element is found,
 * workers check if PR_WK_SYNC_READER flag is on.  Then, worker will sync to READER worker.
 */
void
PR_syncAll(void)
{
	/*
	 * ここ、まだ変。この事象の後、dispatcerr が残りのデータを
	 * 当該worker にディスパッチすることがあるが、その場合が
	 * 処理できていない。
	 *
	 * Sync_reader を dispatcherのみに課し、dispatcher が
	 * READER から割り振られた全データを他の worker との間で
	 * 完了同期取るのが正しいように思われる。
	 *
	 * 正しいやり方は、DISPATCHER に PR_WK_SYNC_READER をセット、
	 * DISPATCHER が全キューを処理完了後、このフラグを見つけたら
	 * PR_syncBeforeDispatcher() を行って、その後 READER に
	 * 完了同期を取るもの。
	 */
	/*
	 * これらは各 worker の loop での実装に変更されているので注意
	 */
	int	ii;
	int	num_sync = 0;

	Assert(my_worker_idx == PR_READER_WORKER_IDX);

	for (ii = 1; ii < num_preplay_workers; ii++)
	{
		if (ii == PR_INVALID_PAGE_WORKER_IDX)
			continue;
		if (set_syncFlag(ii, PR_WK_SYNC_READER))
			num_sync++;
	}
	for (ii = 0; ii < num_sync; ii++)
		PR_recvSync();
	/*
	 * Finaly Invalid Block Worker
	 */
	if (set_syncFlag(PR_INVALID_PAGE_WORKER_IDX, PR_WK_SYNC_READER))
		PR_recvSync();
}


/*
 * Koichi: ここでは、Invalid Page Worker の同期は取っていない。これで大丈夫か、
 * 見直しておくこと。
 */
static void
PR_syncBeforeDispatch(void)
{
	int ii;
	int	num_sync = 0;

	Assert(my_worker_idx == PR_DISPATCHER_WORKER_IDX);

	for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers; ii++)
	{
		if (ii == PR_INVALID_PAGE_WORKER_IDX)
			continue;
		if (set_syncFlag(ii, PR_WK_SYNC_DISPATCHER))
			num_sync++;
	}
	for (ii = 0; ii < num_sync; ii++)
		PR_recvSync();

	if (set_syncFlag(PR_INVALID_PAGE_WORKER_IDX, PR_WK_SYNC_DISPATCHER))
		PR_recvSync();
}

/*
 * Check if all the XLogRecord for the transaction must have been replayed
 * before replaying this XLogRecord.
 */
static bool
checkRmgrTxnSync(RmgrId rmgrid, uint8 info)
{
	if (rmgrid != RM_XACT_ID)
		return false;
	switch(info)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_PREPARE:
		case XLOG_XACT_ABORT:
			return true;
		default:
			return false;
	}
	return false;
}

/*
 * Check if all the outstanding XLogRecord must have been replayed before
 * dispatching this XLogRecord.
 */
static bool
checkSyncBeforeDispatch(RmgrId rmgrid, uint8 info)
{
	switch(rmgrid)
	{
		case RM_XLOG_ID:
			switch(info)
			{
				case XLOG_CHECKPOINT_ONLINE:
					return true;
				case XLOG_NOOP:
				case XLOG_NEXTOID:
					return false;
				case XLOG_SWITCH:
				case XLOG_BACKUP_END:
				case XLOG_PARAMETER_CHANGE:
				case XLOG_RESTORE_POINT:
					return true;
				case XLOG_FPW_CHANGE:
					return false;
				case XLOG_END_OF_RECOVERY:
					return true;
				case XLOG_FPI_FOR_HINT:
				case XLOG_FPI:
				case XLOG_OVERWRITE_CONTRECORD:
					return false;
				default:
					return false;
			}
		case RM_XACT_ID:
			switch (info)
			{
				case XLOG_XACT_COMMIT:
				case XLOG_XACT_PREPARE:
				case XLOG_XACT_ABORT:
				case XLOG_XACT_COMMIT_PREPARED:
				case XLOG_XACT_ABORT_PREPARED:
					return false;
				case XLOG_XACT_ASSIGNMENT:
					return true;
				case XLOG_XACT_INVALIDATIONS	:
					return false;
				default:
					return false;
			}
		case RM_SMGR_ID:
			switch(info)
			{
				case XLOG_SMGR_CREATE:
					return false;
				case XLOG_SMGR_TRUNCATE:
					return true;
				default:
					return false;
			}
		case RM_CLOG_ID:
		case RM_DBASE_ID:
		case RM_TBLSPC_ID:
		case RM_MULTIXACT_ID:
		case RM_RELMAP_ID:
		case RM_STANDBY_ID:
			return true;
		case RM_HEAP2_ID:
		case RM_HEAP_ID:
		case RM_BTREE_ID:
		case RM_HASH_ID:
		case RM_GIN_ID:
		case RM_GIST_ID:
		case RM_SEQ_ID:
		case RM_SPGIST_ID:
		case RM_BRIN_ID:
		case RM_COMMIT_TS_ID:
			return false;
		case RM_REPLORIGIN_ID:
			return true;
		case RM_GENERIC_ID:
		case RM_LOGICALMSG_ID:
			return false;
		default:
			return false;
	}
	return false;
}

static bool
isSyncBeforeDispatchNeeded(XLogReaderState *reader)
{
	RmgrId	rmgr_id;
	uint8	info;

	getXLogRecordRmgrInfo(reader, &rmgr_id, &info);
	return checkSyncBeforeDispatch(rmgr_id, info);
}

/*
 * Check if this XLogRecord must have been replayed before
 * dispatching subsequent XLogRecords.
 */
static bool
checkSyncAfterDispatch(RmgrId rmgrid, uint8 info)
{
	switch(rmgrid)
	{
		case RM_SMGR_ID:
			switch(info)
			{
				case XLOG_SMGR_CREATE:
					return true;
				default:
					return false;
			}
		default:
			return false;
	}
}

static bool
isSyncAfterDispatchNeeded(XLogReaderState *reader)
{
	RmgrId	rmgr_id;
	uint8	info;

	getXLogRecordRmgrInfo(reader, &rmgr_id, &info);
	return checkSyncAfterDispatch(rmgr_id, info);
}

/*
 ****************************************************************************
 *
 * Shared memory functions
 *
 * These function should be called from the READER WORKER.
 *
 ****************************************************************************
 */
/*
 * Koichi: 今の所、PR_buf_size_mg (preplay_buffers) は、バッファ領域のサイズに使用し、
 * 他の構造体はそれぞれのパラメータからサイズを持ってきている。従って、結果的に確保される
 * 共有メモリのサイズは preplay_buffers で指定したサイズよりも大きくなる。
 * これでいいかどうか、後で決める必要がある。
 */
void
PR_initShm(void)
{
	Size	my_shm_size;
	Size	my_txn_hash_sz;
	Size	my_invalidP_sz;
	Size	my_history_sz;
	Size	my_worker_sz;
	Size	my_queue_sz;

	Assert(my_worker_idx == PR_READER_WORKER_IDX);

	my_shm_size = pr_sizeof(PR_shm)
#ifdef WAL_DEBUG
		+ BufferDumpAreaLen
#endif
		+ (my_txn_hash_sz = pr_txn_hash_size())
		+ (my_invalidP_sz = pr_sizeof(PR_invalidPages))
		+ (my_history_sz = xlogHistorySize())
		+ (my_worker_sz = worker_size())
	   	+ (my_queue_sz = queue_size())
		+ buffer_size();

	pr_shm_seg = dsm_create(my_shm_size, 0);
	
	pr_shm = dsm_segment_address(pr_shm_seg);
#ifdef WAL_DEBUG
	buffer_dump_data = (char *)pr_shm;
	pr_shm = addr_forward(pr_shm, BufferDumpAreaLen);
#endif
	pr_shm->txn_wal_info = pr_txn_wal_info = addr_forward(pr_shm, pr_sizeof(PR_shm));
	pr_shm->invalidPages = pr_invalidPages = addr_forward(pr_txn_wal_info, my_txn_hash_sz);
	pr_shm->history = pr_history = addr_forward(pr_invalidPages, pr_sizeof(PR_invalidPages));
	pr_shm->workers = pr_worker = addr_forward(pr_history, my_history_sz);
	pr_shm->queue = pr_queue = addr_forward(pr_worker, my_worker_sz);
	pr_shm->buffer = pr_buffer = addr_forward(pr_queue, my_queue_sz);
	pr_shm->some_failed = false;
	pr_shm->EndRecPtr = InvalidXLogRecPtr;
	pr_shm->MinTimeLineID = 0;

	SpinLockInit(&pr_shm->slock);

	init_txn_hash();
	initXLogHistory();
	initInvalidPages();
	initWorker();
	initQueue();
	initBuffer();
}


void
PR_finishShm(void)
{
	if (pr_shm_seg)
	{
		dsm_detach(pr_shm_seg);
		pr_shm_seg = NULL;
	}
	pr_shm = NULL;
	pr_txn_wal_info = NULL;
	pr_txn_hash = NULL;
	pr_txn_cell_pool = NULL;
	pr_invalidPages = NULL;
	pr_history = NULL;
	pr_worker = NULL;
	pr_queue = NULL;
	pr_buffer = NULL;
}

/*
 ****************************************************************************
 *
 * LSN for each transaction: transaction worker need to synchronize block workers
 * before applying commit/abort WAL record 
 *
 ****************************************************************************
 */

static int txn_hash_size = 0;

static Size
pr_txn_hash_size(void)
{
	/*
	 * Koichi: 実は、Standby で動作している場合、この MaxConnections は
	 * Primary のものでなければならない。この部分も後で見直す必要がある。
	 */
	if (num_preplay_max_txn < MaxConnections)
		num_preplay_max_txn = MaxConnections;
	txn_hash_size = num_preplay_max_txn/2;
	return (pr_sizeof(txn_wal_info_PR) + (pr_sizeof(txn_hash_el_PR) * txn_hash_size)
			+ (pr_sizeof(txn_cell_PR) * num_preplay_max_txn));
}

static void
init_txn_hash(void)
{
	int	ii;
	txn_cell_PR		*cell_pool;
	txn_hash_el_PR	*txn_hash;


	pr_txn_wal_info = pr_shm->txn_wal_info;
	pr_txn_wal_info->txn_hash = pr_txn_hash = (txn_hash_el_PR *)addr_forward(pr_txn_wal_info, pr_sizeof(txn_wal_info_PR));
	pr_txn_wal_info->cell_pool = pr_txn_cell_pool = (txn_cell_PR *)addr_forward(pr_txn_wal_info->txn_hash, pr_sizeof(txn_hash_el_PR) * txn_hash_size);
	for (ii = 0, txn_hash = pr_txn_wal_info->txn_hash; ii < txn_hash_size; ii++)
	{
		SpinLockInit(&txn_hash[ii].slock);
		txn_hash[ii].head = txn_hash[ii].tail = NULL;
	}
	for (ii = 0, cell_pool = pr_txn_wal_info->cell_pool; ii < num_preplay_max_txn; ii++)
	{
		cell_pool[ii].next = &cell_pool[ii+1];
		cell_pool[ii].xid = InvalidTransactionId;
		cell_pool[ii].head = cell_pool[ii].tail = NULL;
	}
	cell_pool[ii].next = NULL;
}

#define txn_hash_value(x)	((x) % txn_hash_size)

/*
 * It is very very rare but as a logic, we have a chance
 * to return NULL value.
 */
static txn_cell_PR *
get_txn_cell(bool need_lock)
{
	txn_cell_PR	*rv;

	if (need_lock)
		SpinLockAcquire(&pr_txn_wal_info->cell_slock);
	rv = pr_txn_cell_pool->next;
	if (rv)
	{
		pr_txn_cell_pool->next = rv->next;
		rv->next = NULL;
	}
	if (need_lock)
		SpinLockRelease(&pr_txn_wal_info->cell_slock);
	return rv;
}

static void
free_txn_cell(txn_cell_PR *txn_cell, bool need_lock)
{
	txn_cell->xid = InvalidTransactionId;
	txn_cell->head = txn_cell->tail = NULL;
	if (need_lock)
		SpinLockAcquire(&pr_txn_wal_info->cell_slock);
	txn_cell->next = pr_txn_cell_pool->next;
	pr_txn_cell_pool->next = txn_cell;
	if (need_lock)
		SpinLockRelease(&pr_txn_wal_info->cell_slock);
}

static txn_cell_PR *
find_txn_cell(TransactionId xid, bool create, bool need_lock)
{
	unsigned int hash_v = txn_hash_value(xid);
	txn_hash_el_PR	*txn_hash;
	txn_cell_PR		*txn_cell;

	txn_hash = &pr_txn_hash[hash_v];
	if (need_lock)
		SpinLockAcquire(&txn_hash->slock);
	for (txn_cell = txn_hash->head; txn_cell; txn_cell = txn_cell->next)
	{
		if (txn_cell->xid == xid)
		{
			if (need_lock)
				SpinLockRelease(&txn_hash->slock);
			return txn_cell;
		}
	}
	if (!create)
	{
		if (need_lock)
			SpinLockRelease(&txn_hash->slock);
		return NULL;
	}
	txn_cell = get_txn_cell(need_lock);
	txn_cell->xid = xid;
	txn_cell->txn_worker_waiting = false;
	if (txn_hash->tail == NULL)
		txn_hash->head = txn_hash->tail = txn_cell;
	else
	{
		txn_hash->tail->next = txn_cell;
		txn_cell->next = NULL;
		txn_hash->tail = txn_cell;
	}
	if (need_lock)
		SpinLockRelease(&txn_hash->slock);
	return txn_cell;
}

static void
freeDispatchData(XLogDispatchData_PR *dispatch_data)
{
	Assert(dispatch_data);

	/*
	 * Now we have up to four shm area to free. 
	 * Let's acquire the lock here to avoid the chance of lock conflict.
	 */
	SpinLockAcquire(&pr_buffer->slock);
#ifdef WAL_DEBUG
	if (dispatch_data->reader->xlog_string)
		PR_freeBuffer(dispatch_data->reader->xlog_string, false);
#endif
	if (dispatch_data->reader)
	{
		int ii;

		Assert(dispatch_data->reader->for_parallel_replay == true);

		if (dispatch_data->reader->main_data)
			PR_freeBuffer(dispatch_data->reader->main_data, false);
		if (dispatch_data->reader->record)
			PR_freeBuffer(dispatch_data->reader->record, false);
		/*
		 * We must be extremely careful about the use of reader->max_block_id.
		 *
		 * This indicates the last index of reader->blocks which is in use,
		 * not number of arrays in use.
		 *
		 * If no reader->block is used max_block_id is set to -1, not zero.
		 * This is why we need '>=' operator, not '>'.
		 */
		for (ii = 0; ii <= dispatch_data->reader->max_block_id; ii++)
		{
			if (dispatch_data->reader->blocks[ii].has_data)
				PR_freeBuffer(dispatch_data->reader->blocks[ii].data, false);
		}
		PR_freeBuffer(dispatch_data->reader, false);
	}
	PR_freeBuffer(dispatch_data, false);
	SpinLockRelease(&pr_buffer->slock);
}

/*
 * This is called by DISPATCHER WORKER before assigned to any other workers.
 * No lock is needed here.
 *
 * This function does not add RM_XACT_ID XLogRecord to transaction history data because
 * each of them are supposed to terminate transaction or their XLogRecord is not needed
 * to synchronize with other WAL record replay.
 *
 * need_lock argument is used only for finding txn_cell.
 */
static void
addDispatchDataToTxn(XLogDispatchData_PR *dispatch_data, bool need_lock)
{
	unsigned int hash_v;
	txn_cell_PR		*txn_cell;
	txn_hash_el_PR	*hash_el;

	if (dispatch_data->xid == InvalidTransactionId)
		/* No transaction associated with this WAL record */
		return;

	if (dispatch_data->n_involved == 0)
		/* No block assosiated with this WAL record */
		return;

	hash_v = txn_hash_value(dispatch_data->xid);
	txn_cell = find_txn_cell(dispatch_data->xid, true, need_lock);
	if (txn_cell == NULL)
		elog(ERROR, "Could not allocate transaction cell.");
	hash_el = &pr_txn_hash[hash_v];

	Assert(txn_cell);

	SpinLockAcquire(&hash_el->slock);
	if (txn_cell->head == NULL)
	{
		txn_cell->head = txn_cell->tail = dispatch_data;
		dispatch_data->next = dispatch_data->prev = NULL;
	}
	else
	{
		dispatch_data->next = NULL;
		dispatch_data->prev = txn_cell->tail;
		txn_cell->tail->prev = dispatch_data;
		txn_cell->tail = dispatch_data;
	}
	SpinLockRelease(&hash_el->slock);
}

/*
 * Assumes the chain is correct
 */
static bool
removeDispatchDataFromTxn(XLogDispatchData_PR *dispatch_data, bool need_lock)
{
	txn_cell_PR		*txn_cell;
	txn_hash_el_PR	*hash_el;

	if (dispatch_data->xid == InvalidTransactionId)
		return true;	/* No dispatch data in this txn */

	hash_el = get_txn_hash(dispatch_data->xid);

	if (need_lock)
		SpinLockAcquire(&hash_el->slock);

	txn_cell = find_txn_cell(dispatch_data->xid, false, false);

	if (txn_cell == NULL)
	{
		elog(LOG, "Cannot find transaction cell.");
		if (need_lock)
			SpinLockRelease(&hash_el->slock);
		return false;	/* No dispatch data in this txn */
	}
	if (dispatch_data->next && dispatch_data->prev)
	{
		dispatch_data->next->prev = dispatch_data->prev;
		dispatch_data->prev->next = dispatch_data->next->prev;
	}
	else if (dispatch_data->next)
	{
		/* Head */
		Assert(dispatch_data == txn_cell->head);
		dispatch_data->next->prev = NULL;
		txn_cell->head = dispatch_data->next;
	}
	else if (dispatch_data->prev)
	{
		/* Tail */
		Assert(dispatch_data == txn_cell->tail);
		dispatch_data->prev->next = NULL;
		txn_cell->tail = dispatch_data->prev;
	}
	else
	{
		/* This is the sole dispatch data for the transaction */
		txn_cell->head = txn_cell->tail = NULL;
		if (need_lock)
			SpinLockRelease(&hash_el->slock);
		return true;	/* No dispatch data in this txn */
	}

	if (need_lock)
		SpinLockRelease(&hash_el->slock);

	return false;	/* Still another dispatch data in this txn */
}

#if 0
/*
 * Koichi:
 *		ここ、なんかおかしい。Dispatcher で、当該 TxnCell を設定していないように
 *		見える
 */
static bool
removeTxnCell(txn_cell_PR *txn_cell)
{
	txn_hash_el_PR	*hash_el;
	txn_cell_PR		*cell, *prev;

	Assert(txn_cell->head != NULL && txn_cell->tail != NULL);

	hash_el = get_txn_hash(txn_cell->xid);
	SpinLockAcquire(&hash_el->slock);
	if (hash_el->head == txn_cell && hash_el->tail == txn_cell)
	{
		hash_el->head = NULL;
		hash_el->tail = NULL;
		SpinLockRelease(&hash_el->slock);
		return true;
	}
	else if (hash_el->head == txn_cell)
	{
		hash_el->head = txn_cell->next;
		SpinLockRelease(&hash_el->slock);
		return true;
	}
	else
	{
		for(cell = hash_el->head, prev = NULL; cell; prev = cell, cell = cell->next)
		{
			if (cell != txn_cell)
				continue;
			else
			{
				if (cell == hash_el->tail)
				{
					hash_el->tail = prev;
					prev->next = NULL;
				}
				else
				{
					prev->next = cell->next;
				}
				SpinLockRelease(&hash_el->slock);
				return true;
			}
		}
		SpinLockRelease(&hash_el->slock);
		elog(ERROR, "Inconsistent internal state.");
		return false;
	}
	return false;		/* Never comems here */
}
#endif


/*
 ****************************************************************************
 *
 * Invalid Page info functions
 *
 ****************************************************************************
 */
static void
initInvalidPages(void)
{
	pr_invalidPages->invalidPageFound = false;
	SpinLockInit(&pr_invalidPages->slock);
}

void
PR_log_invalid_page(RelFileNode node, ForkNumber forkno, BlockNumber blkno, bool present)
{
	XLogInvalidPageData_PR	*invalidData;

	Assert(PR_isInParallelRecovery());

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(XLogInvalidPageData_PR), true);
	invalidData->cmd = PR_LOG;
	invalidData->node = node;
	invalidData->forkno = forkno;
	invalidData->blkno = blkno;
	invalidData->present = present;
	invalidData->dboid = InvalidOid;

	PR_enqueue(invalidData, InvalidPageData, PR_INVALID_PAGE_WORKER_IDX);
}

void
PR_forget_invalid_pages(RelFileNode node, ForkNumber forkno, BlockNumber minblkno)
{
	XLogInvalidPageData_PR	*invalidData;

	Assert(PR_isInParallelRecovery());

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(XLogInvalidPageData_PR), true);
	invalidData->cmd = PR_FORGET_PAGES;
	invalidData->node = node;
	invalidData->forkno = forkno;
	invalidData->blkno = minblkno;
	invalidData->present = false;
	invalidData->dboid = InvalidOid;

	PR_enqueue(invalidData, InvalidPageData, PR_INVALID_PAGE_WORKER_IDX);
}

void
PR_forget_invalid_pages_db(Oid dbid)
{
	XLogInvalidPageData_PR	*invalidData;

	Assert(PR_isInParallelRecovery());

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(XLogInvalidPageData_PR), true);
	invalidData->cmd = PR_FORGET_DB;
	invalidData->forkno = 0;
	invalidData->blkno = 0;
	invalidData->present = false;
	invalidData->dboid = dbid;

	PR_enqueue(invalidData, InvalidPageData, PR_INVALID_PAGE_WORKER_IDX);
}

bool
PR_XLogHaveInvalidPages(void)
{
	bool	rv;

	SpinLockAcquire(&pr_invalidPages->slock);
	rv = pr_invalidPages->invalidPageFound;
	SpinLockRelease(&pr_invalidPages->slock);
	return rv;
}

void
PR_XLogCheckInvalidPages(void)
{
	XLogInvalidPageData_PR	*invalidData;

	Assert(PR_isInParallelRecovery());

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(XLogInvalidPageData_PR), true);
	invalidData->cmd = PR_CHECK_INVALID_PAGES;
	invalidData->forkno = 0;
	invalidData->blkno = 0;
	invalidData->present = false;
	invalidData->dboid = InvalidOid;

	PR_enqueue(invalidData, InvalidPageData, PR_INVALID_PAGE_WORKER_IDX);
}

/*
 ****************************************************************************
 *
 * History info functions
 *
 ****************************************************************************
 */

INLINE Size
xlogHistorySize(void)
{
	return (pr_sizeof(PR_XLogHistory) + (pr_sizeof(PR_XLogHistory_el) * (num_preplay_worker_queue + 2)));
}

static void
initXLogHistory(void)
{
	PR_XLogHistory_el	*el;
	int	ii;

	Assert(pr_history);

	el = (PR_XLogHistory_el *)addr_forward(pr_history, pr_sizeof(PR_XLogHistory));

	pr_history->hist_element = &el[0];

	/* Make el.next circular list */
	for (ii = 0; ii < num_preplay_worker_queue + 2; ii++)
	{
		el[ii].next = &el[ii+1];
		el[ii].curr_ptr = InvalidXLogRecPtr;
		el[ii].end_ptr = InvalidXLogRecPtr;
		el[ii].my_timeline = 0;
		el[ii].replayed = false;
	}
	el[num_preplay_worker_queue + 1].next = NULL;

	SpinLockInit(&pr_history->slock);
}

PR_XLogHistory_el *
PR_addXLogHistory(XLogRecPtr currPtr, XLogRecPtr endPtr, TimeLineID myTimeline)
{
	PR_XLogHistory_el	*el;

	Assert(pr_history);

	SpinLockAcquire(&pr_history->slock);

	if (pr_history->num_elements > (num_preplay_worker_queue + 1))
	{
		SpinLockRelease(&pr_history->slock);
		elog(FATAL, "Could not allocate XLog replay history element.");
		return NULL;
	}
	el = &pr_history->hist_element[pr_history->num_elements];
	el->curr_ptr = currPtr;
	el->end_ptr = endPtr;
	el->my_timeline = myTimeline;
	el->replayed = false;
	pr_history->num_elements++;

#ifdef WAL_DEBUG
	PRDebug_log("\n========== Add XLogHistory ================\n"
			    "%s: returning %p, num_elements: %d, currPtr %016lx, endPtr: %016lx, Timeline: %d\n",
			    __func__, el, pr_history->num_elements, currPtr, endPtr, myTimeline);
#endif
	
	SpinLockRelease(&pr_history->slock);
	return el;
}

/*
 *  Koichi: ここ、直さないといけない。なんか怪しい。ここの目的は：
 *  XLogRecPtr の更新 -> 自分を含めてリスト中の全 XLogRecord の replay が終わったら、
 *  XLogRecPtr を更新し、このリストも初期化する。
 *  実は cyclic リストにする必要はなかった。-> これを修正する
 */
void
PR_setXLogReplayed(PR_XLogHistory_el *el)
{
	int	ii;
	PR_XLogHistory_el *last_el;

	Assert(pr_history);

	SpinLockAcquire(&pr_history->slock);

	el->replayed = true;

	for (ii = 0; ii < pr_history->num_elements; ii++)
	{
		if (pr_history->hist_element[ii].replayed == false)
		{
			SpinLockRelease(&pr_history->slock);
			return;
		}
	}

	/*
	 * Then all the elements were replayed
	 * We can update EndPtr and cleanup the list
	 */
	last_el = &pr_history->hist_element[pr_history->num_elements - 1];
	XLogCtlDataUpdatePtr(last_el->end_ptr, last_el->my_timeline, true);
	pr_history->num_elements = 0;
	SpinLockRelease(&pr_history->slock);
#ifdef WAL_DEBUG
	PRDebug_log("\n=========== Cleaning up XLogHistory =================\n");
#endif
}

/*
 ****************************************************************************
 *
 * Common Worker functions
 *
 ****************************************************************************
 */

int
PR_myWorkerIdx(void)
{
	return my_worker_idx;
}

/*
 * Indicates that I failed and exiting.
 *
 * Koichi: この関数はまだ使われていない。これから、エラー処理として使うようにする。
 */
void
PR_failing(void)
{
	SpinLockAcquire(&my_worker->slock);
	my_worker->worker_failed = true;
	SpinLockRelease(&my_worker->slock);
	SpinLockAcquire(&pr_shm->slock);
	pr_shm->some_failed = true;
	SpinLockRelease(&pr_shm->slock);
}

void
PR_setWorker(int worker_idx)
{
	Assert(pr_shm);

	if ((worker_idx <= 0) || (worker_idx >= num_preplay_workers))
	{
		PR_failing();
		elog(PANIC, "Worker idex %d out of range.", worker_idx);
	}
	my_worker_idx = worker_idx;
	my_worker = &pr_worker[worker_idx];
}

char *
PR_worker_name(int worker_idx, char *buff)
{
	if (worker_idx == PR_READER_WORKER_IDX)
		strcpy(buff, "READER");
	else if (worker_idx == PR_DISPATCHER_WORKER_IDX)
		strcpy(buff, "DISPATCHER");
	else if (worker_idx == PR_TXN_WORKER_IDX)
		strcpy(buff, "TXN WORKER");
	else if (worker_idx == PR_INVALID_PAGE_WORKER_IDX)
		strcpy(buff, "INVALID PAGE WORKER");
	else
		sprintf(buff, "BLOCK WORKER(%d)", worker_idx - PR_BLK_WORKER_MIN_IDX);
	return buff;
}

PR_worker *
PR_myWorker(void)
{
	return &pr_worker[my_worker_idx];
}

INLINE Size
worker_size(void)
{
	return (pr_sizeof(PR_worker) * num_preplay_workers);
}

static void
initWorker(void)
{
	int	ii;
	PR_worker *worker;

	Assert(pr_worker);

	for (ii = 0; ii < num_preplay_workers; ii++)
	{
		worker = &pr_worker[ii];
		worker->worker_idx = ii;
		worker->wait_dispatch = false;
		worker->worker_failed = false;
		worker->flags = 0;
		worker->head = NULL;
		worker->tail = NULL;
		SpinLockInit(&worker->slock);
	}
}

/*
 * Startup all the workers
 */

void
PR_atStartWorker(int idx)
{
	my_worker_idx = idx;
	my_worker = &pr_shm->workers[idx];

	PR_syncInit();	/* Initialize synchronization sockets */

	SpinLockAcquire(&my_worker->slock);
	/*
	 * wait_dispatch will be turned on only when PR_fetchqueue() is
	 * called and finds queue is empty.
	 *
	 * Once READER receives first response from other workers,
	 * worker queue is ready and READER or DISPATCHER can
	 * add dispatch to the queue.
	 */
	my_worker->wait_dispatch = false;
	if (my_worker_idx != PR_READER_WORKER_IDX)
	{
		/*
		 * Other workers need to disable this because
		 * the current callback depends on record in
		 * xlogreader and can conflict with workers
		 * other than READER.
		 */
		error_context_stack = NULL;
	}
	my_worker->flags = 0;
	my_worker->worker_pid = getpid();
	SpinLockRelease(&my_worker->slock);

	/* Tell the READER worker that I'm ready */

	if (my_worker_idx != PR_READER_WORKER_IDX)
		PR_sendSync(PR_READER_WORKER_IDX);
}

/*
 * Should be called by READER WORKERR
 */
void
PR_WorkerStartup(void)
{
	pid_t	pid;
	int		ii;
	int		rv;

	PR_atStartWorker(PR_READER_WORKER_IDX);

	for (ii = 1; ii < num_preplay_workers; ii++)
	{
		pid = StartChildProcess(ParallelRedoProcess, ii);
		if (pid > 0)
		{
			rv = PR_recvSync();
			if (rv != ii)
			{
				PR_failing();
				elog(PANIC, "Invalid initial redo sequence.");
			}
			else
				elog(LOG, "Parallel Redo Process (%d) start.", ii);
		}
		else if (pid <= 0)
		{
			/* Should not return here */
			PR_failing();
			elog(PANIC, "Internal error.  Should not come here.");
			exit(1);	/* Should not come here */
		}
	}
}

/* See xlog.c */
void
PR_WorkerFinish(void)
{
	int ii;
	int	wstatus;

	/*
	 * Terminate all the worker process and wait for exit().
	 */

	Assert(my_worker_idx == PR_READER_WORKER_IDX);

	/* First, need to terminate DISPATCHER worker */
	SpinLockAcquire(&pr_worker[PR_DISPATCHER_WORKER_IDX].slock);
	if (pr_worker[PR_DISPATCHER_WORKER_IDX].wait_dispatch == true)
	{
		pr_worker[PR_DISPATCHER_WORKER_IDX].wait_dispatch = false;
		SpinLockRelease(&pr_worker[PR_DISPATCHER_WORKER_IDX].slock);
		PR_sendSync(PR_DISPATCHER_WORKER_IDX);
		SpinLockAcquire(&pr_worker[PR_DISPATCHER_WORKER_IDX].slock);
	}
	pr_worker[PR_DISPATCHER_WORKER_IDX].flags |= PR_WK_TERMINATE;
	SpinLockRelease(&pr_worker[PR_DISPATCHER_WORKER_IDX].slock);

	waitpid(pr_worker[PR_DISPATCHER_WORKER_IDX].worker_pid, &wstatus, 0);

	/*
	 * Then wait for other worker to terminate, except for INVALID PAGE worker
	 */
	for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers; ii++)
	{
		if (ii != PR_INVALID_PAGE_WORKER_IDX)
			waitpid(pr_worker[ii].worker_pid, &wstatus, 0);
	}

	/*
	 * Finally, terminate INVALID PAGE worker
	 *
	 * This was needed because request to invalid page worker is done by
	 * individual block worker.  We need to terminate this worker when
	 * all the other workers terminates and all the invalid block request
	 * has been dispatched to the invalid block worker.
	 */
	SpinLockAcquire(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);
	if (pr_worker[PR_INVALID_PAGE_WORKER_IDX].wait_dispatch == true)
	{
		pr_worker[PR_INVALID_PAGE_WORKER_IDX].wait_dispatch = false;
		SpinLockRelease(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);
		PR_sendSync(PR_INVALID_PAGE_WORKER_IDX);
		SpinLockAcquire(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);
	}
	pr_worker[PR_INVALID_PAGE_WORKER_IDX].flags |= PR_WK_TERMINATE;
	SpinLockRelease(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);

	waitpid(pr_worker[PR_INVALID_PAGE_WORKER_IDX].worker_pid, &wstatus, 0);

}

/* See xlog.c */
/*
 * This function is called by READER worker and instructs DISPATCHER worker
 * to handle all the outstanding queues, and then wait for other workers
 * to finish all their queues and sync to READER worker.
 * This does not instruct to exit workers.   When all the workers handled
 * assigned queue, they continue to wait for subsequent dispatches.
 */
void
PR_WaitDispatcherQueueHandling(void)
{
	/*
	 * Syncchronize all the workers to handle all the dispatched queue
	 */
	Assert(my_worker_idx == PR_READER_WORKER_IDX);

	SpinLockAcquire(&pr_worker[PR_DISPATCHER_WORKER_IDX].slock);
	pr_worker[PR_DISPATCHER_WORKER_IDX].flags |= PR_WK_SYNC_READER;
	SpinLockRelease(&pr_worker[PR_DISPATCHER_WORKER_IDX].slock);

	PR_recvSync();
}

/*
 * Main entry to Parallel Replay worker process
 *
 * Call chain is as follows:
 * PR_WorkerStartup() (parallel_replay.c)
 * -> StartChildProcess() (postmaster.c)
 * -> AuxiliaryProcessMain() (bootstrap.c)
 * -> ParallelRedoProcessMaiin(idx) (parallel_replay.c)
 */
void ParallelRedoProcessMain(int idx)
{
	int exitcode;
	/*
	 * Note: READER worker is in fact Startup process and is handled by
	 * StartupProcessMain().
	 */
	Assert(idx > PR_READER_WORKER_IDX);


	if (idx == PR_DISPATCHER_WORKER_IDX)
	{
		PR_atStartWorker(PR_DISPATCHER_WORKER_IDX);
		exitcode = dispatcherWorkerLoop();
	}
	else if (idx == PR_TXN_WORKER_IDX)
	{
		PR_atStartWorker(PR_TXN_WORKER_IDX);
		exitcode = txnWorkerLoop();
	}
	else if (idx == PR_INVALID_PAGE_WORKER_IDX)
	{
		PR_atStartWorker(PR_INVALID_PAGE_WORKER_IDX);
		exitcode = invalidPageWorkerLoop();
	}
	else if (PR_IS_BLK_WORKER_IDX(idx))
	{
		PR_atStartWorker(idx);
		exitcode = blockWorkerLoop();
	}
	else
	{
		PR_failing();
		elog(PANIC, "Internal error. Invalid Parallel Redo worker index: %d", idx);
	}

	/* The worker does not return */

	/*
	 * Exit normally.  Exit code 0 tells that parallel redo process completed
	 * all the assigned recovery work.
	 */
	proc_exit(exitcode);
}

/*
 ****************************************************************************
 *
 * Queue functions
 *
 ****************************************************************************
 */
INLINE Size
queue_size(void)
{
	return (pr_sizeof(PR_queue)									/* Queue sructure */
			+ (pr_sizeof(int) * (num_preplay_workers + 1))			/* Worker array after Queue */
			+ (pr_sizeof(PR_queue_el) * num_preplay_worker_queue));/* Queue element */
}

static void
initQueue(void)
{
	Assert(pr_queue);

	pr_queue->num_queue_element = num_preplay_worker_queue;
	pr_queue->wait_worker_list = addr_forward(pr_queue, pr_sizeof(PR_queue));
	initQueueElement();
	SpinLockInit(&pr_queue->slock);
}

static void
initQueueElement(void)
{
	PR_queue_el	*el;
	int	ii;

	Assert(pr_queue);

	pr_queue->element = addr_forward(pr_queue->wait_worker_list, pr_sizeof(int) * (num_preplay_workers + 1));
	for (ii = 0, el = pr_queue->element; ii < num_preplay_worker_queue; ii++, el++)
	{
		el->next = el + 1;
		el->data_type = Init;
		el->data = NULL;
		if (ii == (num_preplay_worker_queue - 1))
			el->next = NULL;
	}
}

static PR_queue_el *
getQueueElement(void)
{
	PR_queue_el	*el;

	SpinLockAcquire(&pr_queue->slock);
	if (pr_queue->element == NULL)
	{
		pr_queue->wait_worker_list[my_worker_idx] = true;
		SpinLockRelease(&pr_queue->slock);
		/* Wait for another worker to free a queue element */
		PR_recvSync();
		return getQueueElement();
	}
	else
	{
		el = pr_queue->element;
		pr_queue->element = el->next;
		SpinLockRelease(&pr_queue->slock);
		return el;
	}
	return NULL;	/* Never comes here */
}

void
PR_freeQueueElement(PR_queue_el *el)
{
	int ii;

	memset(el, 0, sizeof(PR_queue_el));
	SpinLockAcquire(&pr_queue->slock);
	el->next = pr_queue->element;
	pr_queue->element = el;
	for (ii = num_preplay_workers - 1; ii >= 0; ii--)
	{
		if (pr_queue->wait_worker_list[ii])
		{
			pr_queue->wait_worker_list[ii] = false;
			SpinLockRelease(&pr_queue->slock);
			PR_sendSync(ii);
			return;
		}
	}
	SpinLockRelease(&pr_queue->slock);
	return;
}


/* Returns NULL if no queue element is assigned and terminate flag is set */
PR_queue_el *
PR_fetchQueue(void)
{
	PR_queue_el *rv;
	unsigned flags;
#ifdef WAL_DEBUG
	int	synched_worker;
	char workername[64];
#endif

	SpinLockAcquire(&my_worker->slock);
	flags = my_worker->flags;
	rv = my_worker->head;
	if (my_worker->head)
	{
		/*
		 * Available queue.
		 */
		my_worker->head = rv->next;
		if (my_worker->head == NULL)
			my_worker->tail = NULL;
		SpinLockRelease(&my_worker->slock);
		return rv;
	}
	/*
	 * Following code is for the case without any available dispatch information
	 * in the queue.
	 */
	if (!(flags & PR_WK_SYNC))
	{
		/*
		 * No sync flag is available.
		 *
		 * Now setup dispatch request flag and recall myself.
		 */
		my_worker->wait_dispatch = true;
		SpinLockRelease(&my_worker->slock);
#ifdef WAL_DEBUG
		PRDebug_log("Queue is empty.   Wanting for sync from provider.\n");
		synched_worker =
#endif
		PR_recvSync();
#ifdef WAL_DEBUG
		PRDebug_log("Synch received from %s.\n", PR_worker_name(synched_worker, workername));
#endif
		return PR_fetchQueue();
	}
	/*
	 * AKO sync flag is set.
	 */
	if (flags & PR_WK_TERMINATE)
	{
#ifdef WAL_DEBUG
		PRDebug_log("Detected temination flag.\n");
#endif
		/*
		 * TERMINATE request has been made (orignally from READER WORKER)
		 */
		my_worker->flags = 0;
		SpinLockRelease(&my_worker->slock);

		Assert(my_worker_idx != PR_READER_WORKER_IDX);

		if (my_worker_idx == PR_DISPATCHER_WORKER_IDX)
		{
			int ii;
			/*
			 * First, worker terminate request is sent from READER to DISPATCHER.
			 * So, if DISPATCHER receives this, after dispatching everything from
			 * READER worker, it asks all the other workers to terminate.
			 */
#ifdef WAL_DEBUG
			PRDebug_log("Terminating other workers.\n");
#endif
			for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers; ii++)
			{
				if (ii != PR_INVALID_PAGE_WORKER_IDX)
				{
					/*
					 * Bacause other workers may make a request to the invalid
					 * page worker and the reader worker may need invalid page
					 * information,  we do not terminate this worker here.
					 */
					SpinLockAcquire(&pr_worker[ii].slock);
					pr_worker[ii].flags |= PR_WK_TERMINATE;
					if (pr_worker[ii].wait_dispatch == true)
					{
						pr_worker[ii].wait_dispatch = false;
						SpinLockRelease(&pr_worker[ii].slock);
#ifdef WAL_DEBUG
						PRDebug_log("%s is waiting for another dispatch. Disable this and turn on termination flag.\n",
								PR_worker_name(ii, workername));
#endif
						PR_sendSync(ii);
#ifdef WAL_DEBUG
						PRDebug_log("Synch sent to %s.\n", PR_worker_name(ii, workername));
#endif
					}
					else
						SpinLockRelease(&pr_worker[ii].slock);
				}
			}
		}
		return NULL;
	}
	else if (flags & PR_WK_SYNC_READER)
	{

		/*
		 * PR_WK_SYNC_READER is usually made only by READER WORKER
		 * to DISPATCHER WORKER.
		 *
		 * DISPATCHER WORKER works as sync proxy for all the other workers.
		 */
		Assert(my_worker_idx != PR_READER_WORKER_IDX);

		flags &= ~PR_WK_SYNC_READER;

		SpinLockRelease(&my_worker->slock);

#ifdef WAL_DEBUG
		PRDebug_log("Detected synch flag to READER worker.\n");
#endif
		if (my_worker_idx == PR_DISPATCHER_WORKER_IDX)
		{
#ifdef WAL_DEBUG
			PRDebug_log("Synch all the other workers.\n");
#endif
			PR_syncBeforeDispatch();
		}
#ifdef WAL_DEBUG
		PRDebug_log("Synching to READER worker.\n");
#endif
		PR_sendSync(PR_READER_WORKER_IDX);
#ifdef WAL_DEBUG
		PRDebug_log("Synch done.\n");
#endif
	}
	if (flags & PR_WK_SYNC_DISPATCHER)
	{
		Assert(my_worker_idx != PR_DISPATCHER_WORKER_IDX);

#ifdef WAL_DEBUG
		PRDebug_log("Detected synch flag to DISPATCHER worker.\n");
#endif
		my_worker->flags &= ~PR_WK_SYNC_DISPATCHER;
		SpinLockRelease(&my_worker->slock);
#ifdef WAL_DEBUG
		PRDebug_log("Synching to DISPATCHER worker.\n");
#endif
		PR_sendSync(PR_DISPATCHER_WORKER_IDX);
#ifdef WAL_DEBUG
		PRDebug_log("Synch done.\n");
#endif
	}
	if (flags & PR_WK_SYNC_TXN)
	{
		Assert(my_worker_idx != PR_TXN_WORKER_IDX);

		my_worker->flags &= ~PR_WK_SYNC_TXN;
		SpinLockRelease(&my_worker->slock);
		PR_sendSync(PR_TXN_WORKER_IDX);
	}
	return PR_fetchQueue();
}

/*
 * Enqueue
 */
void
PR_enqueue(void *data, PR_QueueDataType type, int	worker_idx)
{
	PR_queue_el	*el;
	PR_worker	*target_worker;
	XLogRecPtr	 currRecPtr;
#ifdef WAL_DEBUG
	char	workername[64];
#endif


	target_worker = &pr_worker[worker_idx];
	el = getQueueElement();
	el->next = NULL;
	el->data_type = type;
	el->data = data;
	switch(type)
	{
		case ReaderState:
			currRecPtr = ((XLogReaderState *)data)->ReadRecPtr;
			break;
		case XLogDispatchData:
			currRecPtr = ((XLogDispatchData_PR *)data)->reader->ReadRecPtr;
			break;
		default:
			currRecPtr = 0;
	}
	SpinLockAcquire(&target_worker->slock);
	target_worker->assignedRecPtr = currRecPtr;
	if (target_worker->head == NULL)
		target_worker->head = target_worker->tail = el;
	else
	{
		target_worker->tail->next = el;
		target_worker->tail = el;
	}
	if (target_worker->wait_dispatch == true)
	{
		target_worker->wait_dispatch = false;
		SpinLockRelease(&target_worker->slock);
#ifdef WAL_DEBUG
		PRDebug_log("%s queue is empty and it is waiting. Sending sync.\n", PR_worker_name(worker_idx, workername));
#endif
		PR_sendSync(worker_idx);
#ifdef WAL_DEBUG
		PRDebug_log("Sync sent.\n");
#endif
	}
	else
		SpinLockRelease(&target_worker->slock);
	return;
}

/*
 ****************************************************************************
 * 
 * XLogReaderState function
 *
 ****************************************************************************
 */
/* Assumes that ReadRecord has been done and it is read into shared buffer */
void
PR_enqueueXLogReaderState(XLogReaderState *state, XLogRecord *record, int worker_idx)
{
	XLogReaderState *state_shm;

	state_shm = PR_allocBuffer(pr_sizeof(XLogReaderState), true);
	memcpy(state_shm, state, sizeof(XLogReaderState));
	state_shm->record = record;
	PR_enqueue(state_shm, ReaderState, worker_idx);
}

XLogDispatchData_PR *
PR_allocXLogDispatchData(void)
{
	XLogDispatchData_PR *dispatch_data;
	Size	sz;
   
	sz = pr_sizeof(XLogDispatchData_PR)
		+ size_boundary((sizeof(bool) * num_preplay_workers))
		+ size_boundary((sizeof(int) * (num_preplay_workers + 1)));

	dispatch_data = (XLogDispatchData_PR *)PR_allocBuffer(sz, true);
	dispatch_data->worker_array = addr_forward(dispatch_data, pr_sizeof(XLogDispatchData_PR));
	dispatch_data->worker_list = addr_forward(dispatch_data->worker_array, size_boundary((sizeof(bool) * num_preplay_workers)));
	return dispatch_data;
}


XLogDispatchData_PR *
PR_analyzeXLogReaderState(XLogReaderState *reader, XLogRecord *record)
{
	int	ii;
	XLogDispatchData_PR *dispatch_data;
	RelFileNode	rnode;
	int			block_id;
	BlockNumber blk;
	ForkNumber  forknum;
	int			block_hash;

	dispatch_data = PR_allocXLogDispatchData();
	dispatch_data->reader = reader;
	dispatch_data->reader->record = record;

	dispatch_data->rmid = XLogRecGetRmid(reader);
	dispatch_data->info = XLogRecGetInfo(reader);
	dispatch_data->rminfo = dispatch_data->info & ~XLR_INFO_MASK;

	for (ii = 0; ii < num_preplay_workers; ii++)
	{
		dispatch_data->worker_array[ii] = false;
		dispatch_data->worker_list[ii] = 0;
	}
	dispatch_data->reader = reader;

	SpinLockInit(&dispatch_data->slock);
	dispatch_data->xid = XLogRecGetXid(reader);
	dispatch_data->n_remaining = 0;
	dispatch_data->n_involved = 0;

	for (block_id = 0; block_id <= reader->max_block_id; block_id++)
	{
		bool rstate;
		if (!XLogRecHasBlockRef(reader, block_id))
			continue;
		rstate = XLogRecGetBlockTag((XLogReaderState *)reader, block_id, &rnode, &forknum, &blk);
		if (rstate == false)
			continue;
		block_hash = blockHash(rnode.spcNode, rnode.dbNode, rnode.relNode,
				blk, num_preplay_workers - PR_BLK_WORKER_MIN_IDX);
		block_hash += PR_BLK_WORKER_MIN_IDX;
		if (dispatch_data->worker_array[block_hash] == false)
		{
			dispatch_data->worker_array[block_hash] = true;
			dispatch_data->worker_list[dispatch_data->n_involved] = block_hash;
			dispatch_data->worker_list[dispatch_data->n_involved + 1] = 0;
			dispatch_data->n_involved++;
			dispatch_data->n_remaining++;
		}
	}
	if (dispatch_data->n_involved == 0)
	{
		/* Not assigned to block worker.   Just assign this to TXN worker */
		dispatch_data->n_involved = 1;
		dispatch_data->worker_list[0] = PR_TXN_WORKER_IDX;
		dispatch_data->worker_list[1] = 0;
		dispatch_data->worker_array[PR_TXN_WORKER_IDX] = true;
	}
	return dispatch_data;
}

/*
 * Setup blocks member for shared XLogReaderState
 */
void
PR_setBlocks(XLogReaderState *shared, XLogReaderState *orig)
{
	int ii;
	DecodedBkpBlock	*shared_block;
	DecodedBkpBlock *orig_block;

	for (ii = 0; ii < orig->max_block_id; ii++)
	{
		shared_block = &shared->blocks[ii];
		orig_block = &orig->blocks[ii];
		if (orig_block->has_image)
		{
			/* set bkp_image ptr */
			shared_block->bkp_image
				= (char *)addr_forward(shared->record, addr_difference(orig->record, orig_block->bkp_image));
		}
		else
			shared_block->bkp_image = NULL;
		if (orig_block->has_data)
		{
			/* aloocate data */
			shared_block->data = (char *)PR_allocBuffer(orig_block->data_len, true);
			memcpy(shared_block->data, orig_block->data, orig_block->data_len);
		}
		else
			shared_block->data = NULL;
	}
}

static int
blockHash(int spc, int db, int rel, int blk, int n_max)
{
	unsigned int wk_all;

	Assert(n_max > 0);

	/* Following code does not finish if (n_max == 1) */
	if (n_max <= 1)
		return 0;

	wk_all = hash_bytes_uint32(spc + db + rel + blk);
	return(wk_all % n_max);
#if 0
	wk_all = fold_int2int8(spc) + fold_int2int8(db) + fold_int2int8(rel) + fold_int2int8(blk);
	wk_all = fold_int2int8(wk_all);

	while(wk_all >= n_max)
		wk_all = wk_all/n_max + wk_all%n_max;
	return wk_all;
#endif
}

/*
 ****************************************************************************
 *
 * Buffer functions
 *
 *
 ****************************************************************************
 */

#define SizeAtTail(chunk)   (Size *)(addr_backward(addr_forward((chunk), (chunk)->size), pr_sizeof(Size)))
#define Chunk(buffer)		(PR_BufChunk *)(addr_backward((buffer), pr_sizeof(PR_BufChunk)))
#define Tail(buffer)		SizeAtTail(Chunk(buffer))
#define Buffer(chunk)		(void *)addr_forward(chunk, pr_sizeof(PR_BufChunk))

/*
 * The function assumes chunk is valid.   If chunk is at the last of the pr_buffer area,
 * this will return pr_buffer->tail, not NULL.
 */
INLINE PR_BufChunk *
next_chunk(PR_BufChunk *chunk)
{
	return (PR_BufChunk *)addr_forward(chunk, chunk->size);
}

#ifdef WAL_DEBUG
/*
 * If s is not NULL, then all the output data will be appended to s.
 * In this case, the caller must have initialized s.
 * If s is NULL, then all the output data will be written to the debuf file.
 */
static bool
dump_buffer(const char *funcname, StringInfo outs, bool need_lock)
{
	static StringInfo	s;
	static StringInfo	ss = NULL;
	int	ii;
	PR_BufChunk	*curr_chunk;
	bool	rv = true;

	if (!pr_buffer->dump_opt)
		return true;
	if (outs == NULL)
	{
		if (ss ==  NULL)
			s = ss = makeStringInfo();
		else
		{
			resetStringInfo(ss);
			s = ss;
		}
	}
	else
		s = outs;

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	appendStringInfo(s, "\n\n===<< Buffer area dump: func: %s, worker: %d >>=================\n", funcname, my_worker_idx);
	appendStringInfo(s, "Pr_buffer: 0x%p, updated: %ld,\n", pr_buffer, pr_buffer->update_sno);
	appendStringInfo(s, "head: 0x%p (%ld), tail: 0x%p (%ld)\n",
								pr_buffer->head, addr_difference(pr_buffer, pr_buffer->head),
								pr_buffer->tail, addr_difference(pr_buffer, pr_buffer->tail));
	appendStringInfo(s, "alloc_start: 0x%p (%ld), alloc_end: 0x%p (%ld)\n",
								pr_buffer->alloc_start, addr_difference(pr_buffer, pr_buffer->alloc_start),
								pr_buffer->alloc_end, addr_difference(pr_buffer, pr_buffer->alloc_end));
	appendStringInfo(s, "needed_by_worker: 0x%p (%ld) (n_worker: %d)\n    ", 
								(pr_buffer->needed_by_worker),
								addr_difference(pr_buffer, pr_buffer->needed_by_worker),
								num_preplay_workers);
	for (ii = 0; ii < num_preplay_workers; ii++)
	{
		if (ii == 0)
			appendStringInfo(s, "(idx: %d, value: %ld)", ii, pr_buffer->needed_by_worker[ii]);
		else
			appendStringInfo(s, ", (idx: %d, value: %ld)", ii, pr_buffer->needed_by_worker[ii]);
	}

	appendStringInfoString(s, "\n------<< Chunk List >>---------------\n");
	/* Dump each chunk from the top */
	for(curr_chunk = (PR_BufChunk *)(pr_buffer->head), ii = 0;
			addr_before(curr_chunk, pr_buffer->tail);
			curr_chunk = next_chunk(curr_chunk), ii++)
	{
		Size *size_at_tail;

		size_at_tail = SizeAtTail(curr_chunk);
		if (curr_chunk->magic == PR_BufChunk_Allocated)
		{
			appendStringInfo(s, "Addr: 0x%016lx (%ld), SNO: %ld, Size: %ld, Magic: Allocated, Size_at_tail: %ld\n",
									(uint64)curr_chunk,
									addr_difference(pr_buffer, curr_chunk),
									curr_chunk->sno,
									curr_chunk->size,
									*size_at_tail);
		}
		else if (curr_chunk->magic == PR_BufChunk_Free)
		{
			appendStringInfo(s, "Addr: 0x%016lx (%ld), Size: %ld, Magic: Free, Size_at_tail: %ld\n",
									(uint64)curr_chunk,
									addr_difference(pr_buffer, curr_chunk),
									curr_chunk->size,
									*size_at_tail);
		}
		else
		{
			appendStringInfo(s, "Addr: 0x%016lx (%ld), Size: %ld, **** ERRROR CHUNK **** Size_at_tail: %ld\n",
									(uint64)curr_chunk,
									addr_difference(pr_buffer, curr_chunk),
									curr_chunk->size,
									*size_at_tail);
			rv = false;
			PR_breakpoint();
			break;
		}
	}
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	appendStringInfoString(s, "------<< Chunk List End >>---------------\n");
	appendStringInfoString(s, "===<< Buffer dump end >>=================\n\n");
	if (outs == NULL)
		PRDebug_out(s);
	return rv;
}

static void
dump_chunk(PR_BufChunk *chunk, const char *funcname, StringInfo outs, bool need_lock)
{
	StringInfo	s;
	static StringInfo	ss = NULL;
	Size	*size_at_tail;

	if (!pr_buffer->dump_opt)
		return;

	if (outs == NULL)
	{
		if (ss == NULL)
			ss = s = makeStringInfo();
		else
		{
			s = ss;
			resetStringInfo(s);
		}
	}
	else
		s = outs;

	appendStringInfo(s, "\n===<< Chunk dump: func: %s, Addr: 0x%p >>=================\n", funcname, chunk);

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);

	size_at_tail = SizeAtTail(chunk);
	appendStringInfo(s, "Buffer: 0x%p, head: 0x%p (%ld), tail: 0x%p (%ld), alloc_start: 0x%p (%ld), alloc_end: 0x%p (%ld).\n",
			pr_buffer,
			pr_buffer->head, addr_difference(pr_buffer->head, pr_buffer),
			pr_buffer->tail, addr_difference(pr_buffer->tail, pr_buffer),
			pr_buffer->alloc_start, addr_difference(pr_buffer->alloc_start, pr_buffer),
			pr_buffer->alloc_end, addr_difference(pr_buffer->alloc_end,pr_buffer));

	if (chunk->magic == PR_BufChunk_Allocated)
		appendStringInfo(s, "Adddr: 0x%p (%ld), SNO: %ld, size: %ld, magic: Allocated, size_at_tail: %ld\n",
				chunk, addr_difference(pr_buffer, chunk), chunk->sno, chunk->size, *size_at_tail);
	else if (chunk->magic == PR_BufChunk_Free)
		appendStringInfo(s, "Adddr: 0x%p (%ld), size: %ld, magic: Free, size_at_tail: %ld\n",
				chunk, addr_difference(pr_buffer, chunk), chunk->size, *size_at_tail);
	else
		appendStringInfo(s, "Addr: 0x%p (%ld), Size: %ld, **** ERRROR CHUNK **** Size_at_tail: %ld\n",
			chunk, addr_difference(pr_buffer, chunk), chunk->size, *size_at_tail);
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	appendStringInfoString(s, "===<< Chunk dump end >>=================\n\n");
	if (outs == NULL)
		PRDebug_out(s);
}

#endif


/*
 * Allocate memory in pr_buffer area.
 *
 * All the memory area in this buffer is for each XLogRecord, this is allocated and
 * eventually consumed by workers.
 *
 * For this, when buffer is full, we can wait ultin such area is consumed and freed.
 * Globaay, the allocation is done from the top to tail.   Even if intermediate
 * area is freed, it is not allocated until it is freed and becomes a part of
 * allocation area between alloc_start to alloc_end, or from alloc_start to tail.
 *
 * Allocation is done in loose cyclic way.
 */
void *
PR_allocBuffer(Size sz, bool need_lock)
{
	return PR_allocBuffer_int(sz, need_lock, true);
}

static void *
PR_allocBuffer_int(Size sz, bool need_lock, bool retry_opt)
{
	PR_BufChunk	*new_chunk;
	Size		 chunk_sz;
	void		*rv;
#ifdef WAL_DEBUG
	static StringInfo	 s = NULL;
#endif


#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);

	appendStringInfo(s, "--- %s: allocation size: %lu ---\n", __func__, sz);
	dump_buffer(__func__, s, need_lock);
	PRDebug_out(s);
	resetStringInfo(s);
#endif

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);

#ifdef WAL_DEBUG
	pr_buffer->update_sno++;
#endif
	chunk_sz = sz + CHUNK_OVERHEAD;
	new_chunk = alloc_chunk(chunk_sz);
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	if (new_chunk)
	{
		/* Successful to allocate buffer */
#ifdef WAL_DEBUG
		dump_chunk(new_chunk, __func__, s, need_lock);
#endif
		return &new_chunk->data[0];
	}

	/* Now no free chunk was available */

	if (!retry_opt)
		return NULL;

	/* New we need to retry */

	rv = retry_allocBuffer(sz, need_lock);

#ifdef WAL_DEBUG
	new_chunk = Chunk(rv);
	dump_chunk(new_chunk, __func__, s, need_lock);
#endif

	return rv;
}


/*
 * When requested size of buffer is not available, the worker writes requested size
 * to pr_buffer and wait until requested size is available by PR_freeBuffer().
 */
static void *
retry_allocBuffer(Size sz, bool need_lock)
{
	void	*rv;
#ifdef WAL_DEBUG
	static StringInfo	s = NULL;
	bool	buf_ok;
#endif

	Assert(sz > 0);

#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
	buf_ok = dump_buffer(__func__, s, need_lock);
	if (buf_ok != true)
		PR_breakpoint();
	PRDebug_out(s);
	resetStringInfo(s);
#endif
	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	pr_buffer->needed_by_worker[my_worker_idx] = sz;
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);

	PR_recvSync();

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	rv = pr_buffer->allocated_buffer[my_worker_idx];
	pr_buffer->allocated_buffer[my_worker_idx] = NULL;
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	return rv;
}

/*
 * Allocate chunk in the free area.  pr_buffer->slock has to be acquired by the caller.
 */
static PR_BufChunk *
alloc_chunk(Size chunk_sz)
{
	PR_BufChunk	*base_chunk;	/* Candidate free chunk to allocate new chunk */
	PR_BufChunk	*new_free_chunk;
	Size		 new_free_chunk_size;
	Size		*size_at_tail;


	if (pr_buffer->alloc_start == pr_buffer->alloc_end)
		/* No space left */
		return NULL;

	base_chunk = (PR_BufChunk *)pr_buffer->alloc_start;
	if (base_chunk->size >= chunk_sz)
	{
		/*
		 * There's sufficient free area to assing the chunk.
		 */
		if (base_chunk->size > chunk_sz + CHUNK_OVERHEAD)
		{
			/* Current free chunk can be divided into alloc chunk and free chunk */
			/*
			 * Divide free chunk into allocated chunk and remainig free chunk.
			 */
			new_free_chunk_size = base_chunk->size - chunk_sz;
			new_free_chunk = addr_forward(pr_buffer->alloc_start, chunk_sz);
			base_chunk->size = chunk_sz;
			size_at_tail = SizeAtTail(base_chunk);
			*size_at_tail = chunk_sz;
			new_free_chunk->size = new_free_chunk_size;
			size_at_tail = SizeAtTail(new_free_chunk);
			*size_at_tail = new_free_chunk_size;
			base_chunk->magic = PR_BufChunk_Allocated;
			new_free_chunk->magic = PR_BufChunk_Free;
			pr_buffer->alloc_start = (void *)new_free_chunk;
		}
		else
		{
			/*
			 * Current free chunk is sufficient to allocate new chunk but remaining
			 * area is not large enough to allocate free chunk.
			 * In this case, all the free are is returned and alloc_start becomes
			 * same as alloc_end eventually.
			 */
			base_chunk->magic = PR_BufChunk_Allocated;
			pr_buffer->alloc_start = next_chunk(base_chunk);
		}
		base_chunk->sno = pr_buffer->curr_sno++;
		return base_chunk;
	}
	else if (addr_before_eq(pr_buffer->alloc_start, pr_buffer->alloc_end))
	{
		/* No free area before alloc_start.  Can allocate no chunk */
		return NULL;
	}
	else
	{
		/*
		 * Try to allocate the buffer in another free buffer area.
		 */
		if (addr_before(pr_buffer->alloc_end, pr_buffer->alloc_start))
		{
			PR_BufChunk *new_chunk;
			void		*old_alloc_start;

			old_alloc_start = pr_buffer->alloc_start;
			pr_buffer->alloc_start = pr_buffer->head;
			new_chunk =  alloc_chunk(chunk_sz);
			if (new_chunk == NULL)
			{
				/*
				 * If no space is left, restore alloc_start for subsequent request
				 * if the requested size is smaller.
				 */
				pr_buffer->alloc_start = old_alloc_start;
			}
			new_chunk->sno = pr_buffer->curr_sno++;
			return new_chunk;
		}
	}
	return NULL;	/* Does not reach here */
}


/*
 * When freeing the buffer, freed buffer will be concatinated with surrounding free 
 * buffer area.
 *
 * Also, if pending buffer request can be fullfilled by such release, buffer will be
 * allocated for such worker here.
 */
void
PR_freeBuffer(void *buffer, bool need_lock)
{
	PR_BufChunk	*chunk;
	int			 ii;
	static bool *sync_alloc_target = NULL;
#ifdef WAL_DEBUG
	static StringInfo	s = NULL;
	bool	buf_ok;
#endif


	/*
	 * Initialize sync target structure if needed.
	 */
	if (sync_alloc_target == NULL)
		sync_alloc_target = (bool *)palloc(sizeof(bool) * num_preplay_workers);

	/* Do not forget to ping reader or dispatcher if there's free area available */
	/* Ping the reader worker only when dispatcher does not require the buffer */

#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
	appendStringInfo(s, "--- %s: freeing: %p ---\n", __func__, buffer);
	dump_chunk(Chunk(buffer), __func__, s, need_lock);
	buf_ok = dump_buffer(__func__, s, need_lock);
	PRDebug_out(s);
	resetStringInfo(s);

	if (buf_ok != true)
		PR_breakpoint();
#endif
	if (addr_before(buffer, pr_buffer->head) || addr_after(buffer, pr_buffer->tail))
		return;

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
#ifdef WAL_DEBUG
	pr_buffer->update_sno++;
#endif
	chunk = Chunk(buffer);
	if (chunk->magic != PR_BufChunk_Allocated)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		PRDebug_log("Attempt to free wrong buffer. Addr: 0x%p.\n", buffer);
		elog(LOG, "Attempt to free wrong buffer.");
		return;
	}
	resetStringInfo(s);

	free_chunk(chunk);

#ifdef WAL_DEBUG
	dump_buffer(__func__, s, false);
	PRDebug_out(s);
#endif

	/*
	 * Now successfully freed buffer.
	 *
	 * Because other workers may be requesting free buffer, we need to do allocate buffer for such workers.
	 *
	 * Important thing is to allocate the memory from lower worker (BLK --> READER) so that they will be freed anyway,
	 * and such workers will end up in waiting for next queue to handle.
	 */

	for (ii = 0; ii < num_preplay_workers; ii++)
		sync_alloc_target[ii] = false;

	for (ii = num_preplay_workers - 1; ii >= PR_READER_WORKER_IDX; ii--)
	{
		void *allocated;

		if (pr_buffer->needed_by_worker[ii] == false)
			/* The worker is not waiting for memory */
			continue;
		/*
		 * Please note that the call immediately returns even if no buffer is available.
		 */
		allocated = PR_allocBuffer_int(pr_buffer->needed_by_worker[ii], false, false);
		if (allocated == NULL)
			/*
			 * Failed to allocate the buffer.  Should wait further and we avoid to allocate buffer
			 * for higher workers so that lower workers can handle buffers and release them to be
			 * available for higher workers.
			 * Until then, higher workers just wait for lower workers to comsume buffers, finish
			 * handling and wait for further data to hande, dispatched by highter workers.
			 */
			break;
		pr_buffer->allocated_buffer[ii] = allocated;
		pr_buffer->needed_by_worker[ii] = false;
		sync_alloc_target[ii] = true;
	}

	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	/*
	 * Now all the relevant spin locks were released.  Safe to send sync.
	 */
	for (ii = num_preplay_workers - 1; ii >= PR_READER_WORKER_IDX; ii--)
	{
		if (sync_alloc_target[ii] == true)
			PR_sendSync(ii);
	}

#ifdef WAL_DEBUG
	buf_ok = dump_buffer(__func__, s, need_lock);
	if (buf_ok != true)
		PR_breakpoint();
	PRDebug_out(s);
#endif
	return;

}

/* The caller should acquire the lock pr_buffer->slock */
static void
free_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk	*new_chunk;

	if (chunk->magic != PR_BufChunk_Allocated)
		return;
	chunk->magic = PR_BufChunk_Free;
	if (chunk == pr_buffer->head)
	{
		new_chunk = concat_next_chunk(chunk);
		if (pr_buffer->alloc_end == pr_buffer->tail)
		{
			pr_buffer->alloc_end = next_chunk(new_chunk);
			if (pr_buffer->alloc_start == pr_buffer->alloc_end)
			{
				pr_buffer->alloc_start = pr_buffer->head;
				pr_buffer->alloc_end = pr_buffer->tail;
			}
		}
		return;
	}
	if (next_chunk(chunk) == pr_buffer->tail)
	{
		if (next_chunk(chunk) == pr_buffer->tail)
		{
			new_chunk = concat_prev_chunk(chunk);
			pr_buffer->alloc_end = pr_buffer->tail;
		}
		else
			concat_prev_chunk(chunk);
		return;

	}
	if (addr_before(pr_buffer->alloc_start, pr_buffer->alloc_end))
	{
		if (((void *)chunk == pr_buffer->head) && ((void *)next_chunk(chunk) == pr_buffer->alloc_start))
		{
			/* (1) Chunk is the whole area from head to alloc_start */
			new_chunk = concat_next_chunk(chunk);
			pr_buffer->alloc_start = pr_buffer->head;
		}
		else if (((void *)chunk == pr_buffer->alloc_end) && (next_chunk(chunk) == pr_buffer->tail))
		{
			/* (2) Chunk is whole from alloc_end to tail */
			new_chunk = concat_prev_chunk(chunk);
			pr_buffer->alloc_end = pr_buffer->tail;
		}
		else if ((void *)next_chunk(chunk) == pr_buffer->alloc_start)
		{
			/* (3) The chunk is just before allocation start point */
			new_chunk = concat_surrounding_chunks(chunk);
			pr_buffer->alloc_start = (void *)new_chunk;
		}
		else if (addr_before(next_chunk(chunk), pr_buffer->alloc_start))
			/*
			 * (4) Chunk is in the begging part and other allocated chunks exists
			 *     between this chunk and pr_buffer->alloc_start
			 */
			concat_surrounding_chunks(chunk);
		else if ((void *)chunk == pr_buffer->alloc_end)
		{
			/* (5) Chunk is just after pr_buffer->allloc_end. */
			new_chunk = concat_surrounding_chunks(chunk);
			pr_buffer->alloc_end = next_chunk(new_chunk);
		}
		else if ((void *)chunk == pr_buffer->head && pr_buffer->alloc_end == pr_buffer->tail)
		{
			/*
			 * (6) Chunk is at the beginning of the buffer pool and
			 *     alloc_end == tail.   In this case, this (concatenated) chunk additional
			 *     free area.
			 */
			new_chunk = concat_next_chunk(chunk);
			pr_buffer->alloc_end = next_chunk(chunk);
		}
		else
			/*
			 * (7) Chunk is in the middle between head and alloc_start
			 *     or alloc_end and tail
			 */
			concat_surrounding_chunks(chunk);
	}
	else if (addr_after(pr_buffer->alloc_start, pr_buffer->alloc_end))
	{
		if (((void *)chunk == pr_buffer->alloc_end) && ((void *)next_chunk(chunk) == pr_buffer->alloc_start))
		{
			/* (8) Chunk is the whole allocated area */
			concat_surrounding_chunks(chunk);
			pr_buffer->alloc_start = pr_buffer->head;
			pr_buffer->alloc_end = pr_buffer->tail;
		}
		else if ((void *)chunk == pr_buffer->alloc_end)
		{
			/* (9) Chunk is just after pr_buffer->alloc_end */
			new_chunk = concat_surrounding_chunks(chunk);
			pr_buffer->alloc_end = next_chunk(new_chunk);
		}
		else if ((void *)next_chunk(chunk) == pr_buffer->alloc_start)
		{
			/* (10) Chunk is just before pr_buffer->alloc_start */
			new_chunk = concat_surrounding_chunks(chunk);
			pr_buffer->alloc_start = new_chunk;
		}
		else
			/* (11) Chunk is in the midddle of alloc_end and alloc_start */
			concat_surrounding_chunks(chunk);
	}
	else
	{
		Assert(pr_buffer->alloc_start == pr_buffer->alloc_end);

		/* Now no space left in the buffer pool.   alloc_start == alloc_end */
		/*
		 * We need to use current alloc_start and alloc_end so that 
		 * buffer near current_start will be freed first.
		 */
		if ((void *)chunk == pr_buffer->alloc_end)
		{
			/* (12) Releasing chunk just before alloc_start/alloc_end */
			chunk = concat_next_chunk(chunk);	/* Concat just in case */
			pr_buffer->alloc_end = next_chunk(chunk);
		}
		else if ((void *)next_chunk(chunk) == pr_buffer->alloc_start)
		{
			/* (13) Releasing chunk just after alloc_start/alloc_end */
			chunk = concat_prev_chunk(chunk);
			pr_buffer->alloc_start = chunk;
		}
		else
			/* (14) Releasing chunk in intermediate place */
			concat_surrounding_chunks(chunk);
	}
	return;
}


#if 0
/* The caller must acquire the lock at pr_buffer->slock */
/*
 * Search chunk for given address
 */
static PR_BufChunk *
search_chunk(void *addr)
{
	PR_BufChunk *curr;
	PR_BufChunk *next;

	/*
	 * The check so far is not exact.  Some line allows addr in the OVERHEAD area
	 * of the chunk and some does not.
	 *
	 * Hope this is simple and good enough for most of the case.
	 */
	if (addr_before_eq(addr, pr_buffer->head) || addr_after_eq(addr, pr_buffer->tail))
		return NULL;
	for (curr = (PR_BufChunk *)pr_buffer->head, next = next_chunk(curr);
			addr_before(curr, pr_buffer->tail);
			curr = next, next = next_chunk(next))
	{
		if (addr_after_eq(addr, curr) && addr_before(addr, next))
			return curr;
	}
	return NULL;
}
#endif


/*
 * Check if the address is in allocated shared buffer
 */
bool
PR_isInBuffer(void *addr)
{
	/*
	 * The check so far is not exact.  Some condition allows addr in the OVERHEAD area
	 * of the chunk and some does not.
	 *
	 * Hope this is simple and good enough for most of the case.
	 */
	return (addr_after(addr, pr_buffer->head) && addr_before(addr, pr_buffer->tail)) ? true : false;
}

INLINE Size
buffer_size(void)
{
	return(PR_buf_size_mb);
}

static void
initBuffer(void)
{
	PR_BufChunk	*chunk;
	Size	*size_at_tail;
	Size	*needed_by_worker;
	int		 ii;

	Assert(pr_buffer);

	pr_buffer->area_size = buffer_size() - (pr_sizeof(Size) * num_preplay_workers) - pr_sizeof(PR_buffer);
	pr_buffer->needed_by_worker = (Size *)addr_forward(pr_buffer, pr_sizeof(PR_buffer));
	pr_buffer->curr_sno = 0;
	needed_by_worker = &pr_buffer->needed_by_worker[0];
	for(ii = 0; ii < num_preplay_workers; ii++)
		needed_by_worker[ii] = 0;
	pr_buffer->head = addr_forward(needed_by_worker, (pr_sizeof(Size) * num_preplay_workers));
	pr_buffer->tail = addr_forward(pr_buffer, buffer_size());
	pr_buffer->alloc_start = pr_buffer->head;
	pr_buffer->alloc_end = pr_buffer->tail;
	chunk = (PR_BufChunk *)pr_buffer->alloc_start;
	chunk->size = pr_buffer->area_size;
	chunk->magic = PR_BufChunk_Free;
	size_at_tail = SizeAtTail(chunk);
	*size_at_tail = chunk->size;
#ifdef WAL_DEBUG
	pr_buffer->dump_opt = true;
	pr_buffer->update_sno = 0;
	dump_buffer(__func__, NULL, false);
#endif
	SpinLockInit(&pr_buffer->slock);
}


/* The caller should acquire the lock pr_buffer->slock */
/*
 * The function assumes that the given chunk is valid.   This function may return NULL
 * if chunk is at the beginning of pr_buffer data area.
 */
static PR_BufChunk *
prev_chunk(PR_BufChunk *chunk)
{
	Size		*size_at_tail;

	if ((void *)chunk == pr_buffer->head)
		return NULL;
	size_at_tail = addr_backward(chunk, pr_sizeof(Size));
	return (PR_BufChunk *)addr_backward(chunk, *size_at_tail);
}


/*
 * Concatinate this free chunk with the next one.  The function
 * assumes that the given chunk is valid.
 */
/* Caller must acquire the lock pr_block->slock */
static PR_BufChunk *
concat_next_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk *next;
	Size	*size_at_tail;

	Assert(pr_buffer && (chunk->magic == PR_BufChunk_Free));

	next = next_chunk(chunk);
	if (addr_after_eq(next, pr_buffer->tail))
		return chunk;
	if (next->magic != PR_BufChunk_Free)
		return chunk;;
	chunk->size += next->size;
	size_at_tail = SizeAtTail(chunk);
	*size_at_tail = chunk->size;
	return chunk;
}

/*
 * Concatinate this free chunk with the previous one.  The function
 * assumes that the given chunk is valid.
 */
/* Caller must acquire the lock pr_block->slock */
static PR_BufChunk *
concat_prev_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk *prev;
	Size	*size_at_tail;

	Assert(pr_buffer && (chunk->magic == PR_BufChunk_Free));
	Assert(addr_after_eq(chunk, pr_buffer->head) && addr_before_eq(chunk, pr_buffer->tail));

	if (addr_before_eq(chunk, pr_buffer->head))
		return chunk;
	size_at_tail = SizeAtTail(chunk);
	prev = prev_chunk(chunk);
	if (!prev || prev->magic != PR_BufChunk_Free)
		return chunk;
	prev->size += chunk->size;
	*size_at_tail = prev->size;
	return prev;
}

#ifdef WAL_DEBUG
/*
 ******************************************************************************************************
 *
 * Test code
 *
 * Because parallel recovery consists of many worker process folked by startup process, it is not
 * simple to test using debugge.  For debug, we need to attach each worker process to gdb when it is
 * about to start.
 *
 * For this purpose, we use the following mechanism so that you can attach worker process to gdb.
 *
 * 0.  Start the database with following GUC parameters:
 *
 *       parallel_reply = on
 *       num_preplay_workers = xxx
 *		 num_preplay_queues = yyy
 *	     num_preplay_max_txn = zzz	# must be larger or equal to master's max_connections
 *		 preplay_buffers = uuu		# conventional size, such as xxMB, xxGB
 *		 parallel_replay_test = on
 *
 * 1.  With -D WEB_DEBUG, text format WAL record will be stored in XLogReaderState->xlog_string for reference
 *     from gdb.
 *
 * 2.  Debug message will be written to:
 *           $PGDATA/pr_debug/pr_debug.log
 *     to show what worker is about to start.
 *
 * 3.  In this message, pid of the worker will be presented alog with signal file name and break point for gdb.
 *
 * 4.  You can attach the process to gdb, set the break point as suggested, and keep gdb run.
 *
 * 5.  The worker is waitng for the signal file.  Touch the signal file and then the worker will continue to
 *     to run.   Then you can trace this worker with gdb.
 *     Signal file path is:
 *			$PGDATA/pr_debug/%d.signal
 *     where %d is pid of the worker.
 *
 ******************************************************************************************************
 */


#define	PRDEBUG_BUFSZ (4096 * 16)
#define	PRDEBUG_HDRSZ 512

#define	true 1
#define false 0

#define	DEBUG_LOG_FILENAME_LEN	512
#define DEBUG_LOG_FILENAME "pr_debug.log"
#define DEBUG_LOG_DIRNAME  "pr_debug"

static char *my_timeofday(void);
static void build_PRDebug_log_hdr(void);

/*
 * Should be called by READER WORKER before other workers start
 */
void
PRDebug_init(bool force_init)
{
	struct stat statbuf;
	int	   rv;
	int		my_errno;

	if (force_init)
		PRDebug_finish();

	elog(DEBUG3, "%s:%s called.", __func__, __FILE__);
	/* Create and initialize debug dir */
	debug_log_dir = (char *)malloc(DEBUG_LOG_FILENAME_LEN);
	sprintf(debug_log_dir, "%s/%s", DataDir, DEBUG_LOG_DIRNAME);
	rv = stat(debug_log_dir, &statbuf);
	my_errno = errno;
	if (rv)
	{
		/* Debug directory stat error */
		int	local_errno;

		if (my_errno != ENOENT)
			elog(PANIC, "Failed to stat PR debug directory, %s", strerror(my_errno));
		rv = mkdir(debug_log_dir, S_IRUSR|S_IWUSR|S_IXUSR);
		local_errno = errno;
		if (rv != 0)
		{
			PR_failing();
			elog(PANIC, "Failed to create PR debug directory, %s", strerror(local_errno));
		}
	}
	else
	{
		/* Debug directory stat successfful */
		if ((statbuf.st_mode & S_IFDIR) == 0)
		{
			/* Not directory */
			elog(PANIC, "%s must be a directory but not.", debug_log_dir);

		}
	}
	debug_log_file_path = (char *)malloc(DEBUG_LOG_FILENAME_LEN);
	debug_log_file_link = (char *)malloc(DEBUG_LOG_FILENAME_LEN);
	sprintf(debug_log_file_path, "%s/%s_%s", debug_log_dir, my_timeofday(), DEBUG_LOG_FILENAME);
	sprintf(debug_log_file_link, "%s/%s", debug_log_dir, DEBUG_LOG_FILENAME);
	pr_debug_log = fopen(debug_log_file_path, "w");
	unlink(debug_log_file_link);
	rv = symlink(debug_log_file_path, debug_log_file_link);
	my_errno = errno;
	if (rv)
	{
		elog(PANIC, "Failed to make symlink from %s to %s, %s",
				debug_log_file_path, debug_log_file_link,
				strerror(my_errno));
	}
}

#define PR_TIEOFDAY_LEN 64
static char	my_timeofday_val[PR_TIEOFDAY_LEN];

static char *
my_timeofday(void)
{
	struct tm *timeofday;
	time_t	current_time;

	current_time = time(NULL);
	timeofday = localtime(&current_time);
	sprintf(my_timeofday_val, "%04d%02d%02d_%02d%02d%02d",
			    timeofday->tm_year + 1900, timeofday->tm_mon, timeofday->tm_mday,
				timeofday->tm_hour, timeofday->tm_min, timeofday->tm_sec);
	return my_timeofday_val;
}
#undef PR_TIEOFDAY_LEN

static char *break_point_file[] =
{
 	"reader_break.gdb",
	"dispatcher_worker_break.gdb",
	"txn_worker_break.gdb",
	"invalid_page_worker_break.gdb",
	"block_worker_break.gdb"
};

/*
 * Should be called by each worker, including READER WORKER
 *
 * Sock name: $PGDATA/pr_debug/sock_%d where %d is the worker index.
 *
 * When starting debug parallel redo processes with gdb, do the following:
 *
 * 1) Turn on GUC PR_test,
 * 2) Start PG with pg_ctl as usual,
 * 3) Watch the debug log file $PGDATA/pr_debug/pr_debug.log, i.e. tail -f pr_debug.log
 * 4) When message appeas to the debug log file, run gdb, attach the PID in the debug log file and
 *    set the break point to PRDebug_sync.
 * 5) Touch the signal file as found in the debug log file output.
 * 6) gdb begins to run with this process attached.  Stops at the break point specified.
 */
void
PRDebug_start(int worker_idx)
{
	struct stat statbuf;
	int		rv;
	int		my_errno;
	char	found_debug_file = false;
	static const int start_timeout = 60 * 10;	/* start timeout = 10min */
	int		time_waiting = 0;
	char	*break_point_fname;

	Assert(worker_idx >= 0);

	my_worker_idx = worker_idx;
	build_PRDebug_log_hdr();
	pr_debug_signal_file = (char *)malloc(DEBUG_LOG_FILENAME_LEN);

	/* Setup breakpoint file for each worker */
	if (worker_idx < PR_BLK_WORKER_MIN_IDX)
		break_point_fname = break_point_file[worker_idx];
	else
		break_point_fname = break_point_file[PR_BLK_WORKER_MIN_IDX];
	
	sprintf(pr_debug_signal_file, "%s/%s/%d.signal", DataDir, DEBUG_LOG_DIRNAME, my_worker_idx);
	unlink(pr_debug_signal_file);
	PRDebug_log("\n-------------------------------------------\n"
				"Now ready to attach the debugger to pid %d.  "
				"Set the break point to PRDebug_sync()\n"
			    "My worker idx is %d.\nPlease touch %s to begin.  "
				"I'm waiting for it.\n\n"
				"Do following from another shell:\n"
				"sudo gdb \\\n"
				"-ex 'attach %d' \\\n"
				"-ex 'tb PRDebug_sync' \\\n"
				"-ex 'source %s' \\\n"
				"-ex 'shell touch  %s' \\\n"
				"-ex 'continue'\n",
			getpid(), worker_idx, pr_debug_signal_file,
			getpid(), break_point_fname, pr_debug_signal_file);
	while((found_debug_file == false) && (time_waiting < start_timeout))
	{
		rv = stat(pr_debug_signal_file, &statbuf);
		my_errno = errno;
		if (rv)
		{
			if (my_errno == ENOENT)
			{
				sleep(1);
				time_waiting++;
				continue;
			}
			else
			{
				PR_failing();
				elog(PANIC, "%s stat error, %s", pr_debug_signal_file, strerror(my_errno));
				printf("%s stat error, %s", pr_debug_signal_file, strerror(my_errno));
				exit(1);
			}
		}
		else
		{
			found_debug_file = true;
			break;
		}
	}
	if (found_debug_file == true)
	{
		unlink(pr_debug_signal_file);
		PRDebug_log("Detected %s.  Can begin debug\n", pr_debug_signal_file);
	}
	else
		PRDebug_log("Could not find the file \"%s\". You may not be able to debug this.\n", pr_debug_signal_file);
	PRDebug_sync();
}

/*
 * Initial breakpoint for gdb to begin each worker debug
 */
void
PRDebug_sync(void)
{
	return;
}

/*
 * Breakpoint function to detect error with the debugger.
 */
void
PR_breakpoint_func(void)
{
	return;
}

static char	*pr_debug_log_hdr = NULL;

void
PRDebug_log(char *fmt, ...)
{
	char buf[PRDEBUG_BUFSZ];
	va_list	arg_ptr;
	ErrorContextCallback *error_context_stack_backup;

	/*
	 * xlog.c sets up error_context_stack for writing
	 * XLogRecord info to the log using error_context_stack.
	 * This conflicts here so we disable this and the restore
	 */
	error_context_stack_backup = error_context_stack;
	error_context_stack = NULL;

	va_start(arg_ptr, fmt);
	vsprintf(buf, fmt, arg_ptr);

	/*
	 * It is dangerous to call this here because elog plugin may call DecodeXlogRecord, where
	 * decoded_record is still NULL, ending up with SIGSEGV.

	elog(LOG, "%s%s", pr_debug_log_hdr, buf);

	*/
	fprintf(pr_debug_log, "%s %s ===  %s",
			pr_debug_log_hdr, now_timeofday(), buf);
	fflush(pr_debug_log);

	error_context_stack = error_context_stack_backup;
}

static char *
now_timeofday(void)
{
	static char	timebuf[16];
	struct timeval	tv;
	struct tm	    now;

	gettimeofday(&tv, NULL);
	if (gmtime_r(&tv.tv_sec, &now) != &now)
		return "";
	sprintf(timebuf,
			"%02d:%02d:%02d.%05ld",
			now.tm_hour, now.tm_min, now.tm_sec, tv.tv_usec);
	return timebuf;
}

static void
PRDebug_out(StringInfo s)
{
	if ((s->data[0] == '\0') || (s->len == 0))
		return;
	/*
	 * It is dangerous to call this here because elog plugin may call DecodeXlogRecord, where
	 * decoded_record is still NULL, ending up with SIGSEGV.

	elog(LOG, "%s%s", pr_debug_log_hdr, s->data);

	 */
	fprintf(pr_debug_log, "%s %s === %s",
			pr_debug_log_hdr, now_timeofday(), s->data);
	fflush(pr_debug_log);
}

static void
build_PRDebug_log_hdr(void)
{
	char workername[64];
	pr_debug_log_hdr = (char *)malloc(PRDEBUG_HDRSZ);
	if (my_worker_idx < PR_BLK_WORKER_MIN_IDX)
		sprintf(pr_debug_log_hdr, "%s(%d): ",
				PR_worker_name(my_worker_idx, workername), my_worker_idx);
	else
		sprintf(pr_debug_log_hdr, "%s_%02d(%d): ",
				PR_worker_name(my_worker_idx, workername), (my_worker_idx - PR_BLK_WORKER_MIN_IDX), my_worker_idx);
}

/*
 * Should be called from READER worker after all the WAL replay finished.
 */
void
PRDebug_finish(void)
{
	if (pr_debug_log)
	{
		fclose(pr_debug_log);
		pr_debug_log = NULL;
	}
	if (pr_debug_signal_file)
	{
		free(pr_debug_signal_file);
		pr_debug_signal_file = NULL;
	}
	if (debug_log_dir)
	{
		free(debug_log_dir);
		debug_log_dir = NULL;
	}
	if (debug_log_file_path)
	{
		free(debug_log_file_path);
		debug_log_file_path = NULL;
	}
	if (debug_log_file_link)
	{
		free(debug_log_file_link);
		debug_log_file_link = NULL;
	}
}

/*
 * Intended to ba called from XtartupXLOG() for the test of buffer allocation/free.
 * Intended to be monitored by gdb.
 */
typedef struct Buff_test Buff_test;

struct Buff_test
{
	char		*buff;
	Size		 size;
	PR_BufChunk	*chunk;
};

Buff_test	*buff_test;
int			 buff_test_size;

/*
 * Test PR_allocBuffer() and PR_freeBuffer().
 *
 * Alloc: 1024, 512, 256, 128, 64, 64, 128, 256, 512, 1024
 * Free:   x    N/A   X   N/A  X   N/A  X   N/A   X    N/A
 * Alloc   512  N/A  128  N/A  64  N/A 256  N/A  1024  N/A
 */
void
PR_debug_buffer(void)
{
	static Size testsize[] = {1024, 512, 256, 128, 64, 64, 128, 256, 512, 1024, 0};
	int ii;
	Buff_test	*curr;


	for (buff_test_size = 0; testsize[buff_test_size] > 0; buff_test_size++);

	buff_test = (Buff_test *)palloc(sizeof(Buff_test) * buff_test_size);
	for (ii = 0; ii < buff_test_size; ii++)
	{
		curr = &buff_test[ii];
		curr->buff = NULL;
		curr->chunk = NULL;
		curr->size = testsize[ii];
	}

	for (ii = 0; ii < buff_test_size; ii++)
	{
		curr = &buff_test[ii];
		curr->buff = PR_allocBuffer(curr->size, true);
		curr->chunk = Chunk(curr->buff);
	}
	for (ii = 0; ii < buff_test_size - 1; ii+=2)
	{
		curr =&buff_test[ii];
		PR_freeBuffer(curr->buff, true);
		curr->buff = NULL;
		curr->chunk = NULL;
	}
	for (ii = 0; ii < buff_test_size - 1; ii+=2)
	{
		curr =&buff_test[ii];
		curr->size = buff_test[ii+1].size;
		curr->buff = PR_allocBuffer(curr->size, true);
		curr->chunk = Chunk(curr->buff);
	}
	for (ii = 0; ii < buff_test_size; ii++)
	{
		curr = &buff_test[ii];
		if (curr->buff)
			PR_freeBuffer(curr->buff, true);
	}
}

/*
 * Test buffer management including wraparound
 * Assumes the shared buffer size is around 4MG.
 * 1. Leave three buffers allocated
 * 2. Free the latest buffer
 * 3. Allocate the same size of the buffer.
 */
void
PR_debug_buffer2(void)
{
	static Size testsize[] = {384 * 1024, 384 * 1024, 384 * 1024, 384 * 1024,  0};
	int ii, jj;
	Buff_test	*curr;

	for (buff_test_size = 0; testsize[buff_test_size] > 0; buff_test_size++);

	buff_test = (Buff_test *)palloc(sizeof(Buff_test) * buff_test_size);
	for (ii = 0; ii < buff_test_size; ii++)
	{
		curr = &buff_test[ii];
		curr->buff = NULL;
		curr->chunk = NULL;
		curr->size = testsize[ii];
	}

	/*
	 * Allocate initial buffer
	 */
	for (ii = 0; ii < buff_test_size; ii++)
	{
		curr = &buff_test[ii];
		curr->buff = PR_allocBuffer(curr->size, true);
		curr->chunk = Chunk(curr->buff);
	}
	/*
	 * Itellate free and allocate
	 */
	for (jj = 0; jj < 10; jj++)
	{
		for (ii = 0; ii < buff_test_size; ii++)
		{
			curr =&buff_test[ii];
			PR_freeBuffer(curr->buff, true);
			curr->buff = NULL;
			curr->chunk = NULL;
			curr->buff = PR_allocBuffer(curr->size, true);
			curr->chunk = Chunk(curr->buff);
		}
	}
	for (ii = 0; ii < buff_test_size; ii++)
	{
		curr = &buff_test[ii];
		if (curr->buff)
			PR_freeBuffer(curr->buff, true);
	}
}

XLogDispatchData_PR *test_dispatch_data = NULL;

void
PR_debug_analyzeState(XLogReaderState *state, XLogRecord *record)
{
	test_dispatch_data = PR_analyzeXLogReaderState(state, record);
	PR_freeBuffer(test_dispatch_data, true);
	test_dispatch_data = NULL;
}
#endif


/*
 ******************************************************************************************************************
 *
 * Parallel worker functions
 *
 ******************************************************************************************************************
 */

/*
 ******************************************************************************************************************
 * Invalid Page Worker
 ******************************************************************************************************************
 */

#ifdef WAL_DEBUG
static void
dump_invalidPageData(XLogInvalidPageData_PR *page)
{
	StringInfo	s;

	s = makeStringInfo();
	appendStringInfo(s, "XLogInvalidPageData_PR dump: cmd: %s, ", invalidPageCmdName(page->cmd));
	appendStringInfo(s, "node.spc:%d, node.db:%d, node.rel:%d, ",
			page->node.spcNode, page->node.dbNode, page->node.relNode);
	appendStringInfo(s, "forkNum: %s, BlockNum: %d, present: %s.\n",
			forkNumName(page->forkno), page->blkno, page->present ? "true" : "false");
	PRDebug_out(s);
	pfree(s->data);
	pfree(s);
}

static char *
forkNumName(ForkNumber forknum)
{
	switch(forknum)
	{
		case InvalidForkNumber:
			return "InvalidForkNumber";
		case MAIN_FORKNUM:
			return "MAIN_FORKNUM";
		case FSM_FORKNUM:
			return "FSM_FORKNUM";
		case VISIBILITYMAP_FORKNUM:
			return "VISIBILITYMAP_FORKNUM";
		case INIT_FORKNUM:
			return "INIT_FORKNUM";
		default:
			return "unknown";
	}
}

static char *
invalidPageCmdName(PR_invalidPageCheckCmd cmd)
{
	switch(cmd)
	{
		case PR_LOG:
			return "PR_LOG";
		case PR_FORGET_PAGES:
			return "PR_FORGET_PAGES";
		case PR_FORGET_DB:
			return "PR_FORGET_DB";
		case PR_CHECK_INVALID_PAGES:
			return "PR_CHECK_INVALID_PAGES";
		default:
			return "unknown";
	}
}
#endif

/*
 * When this starts, we assume that worker data have already been set up.
 *
 * So this is just a main loop.
 */
static int
invalidPageWorkerLoop(void)
{
	PR_queue_el	*el;
	XLogInvalidPageData_PR	*page;
	bool invalidPageFound;

#ifdef WAL_DEBUG
	PRDebug_log("Started %s()\n", __func__);
#endif /* WAL_DEBUG */

	SpinLockAcquire(&my_worker->slock);
	my_worker->handledRecPtr = 0;
	SpinLockRelease(&my_worker->slock);
	
	for (;;)
	{
#ifdef WAL_DEBUG
		PRDebug_log("Fetching queue\n");
#endif /* WAL_DEBUG */
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != InvalidPageData)
		{
			PR_failing();
			elog(PANIC, "Invalid internal status for invalid page worker.");
		}
		page = (XLogInvalidPageData_PR *)(el->data);
		PR_freeQueueElement(el);
#ifdef WAL_DEBUG
		PRDebug_log("Fetched\n");
		dump_invalidPageData(page);
#endif	/* WAL_DEBUG */
#ifdef PR_IGNORE_REPLAY_ERROR
		PR_freeBuffer(page, true);
		continue;
#endif /* PR_IGNORE_REPLAY_ERROR */
		switch(page->cmd)
		{
			case PR_LOG:
				PR_log_invalid_page_int(page->node, page->forkno, page->blkno, page->present);
				break;
			case PR_FORGET_PAGES:
				PR_forget_invalid_pages_int(page->node, page->forkno, page->blkno);
				break;
			case PR_FORGET_DB:
				PR_forget_invalid_pages_db_int(page->dboid);
				break;
			case PR_CHECK_INVALID_PAGES:
				PR_XLogCheckInvalidPages_int();
				break;
			default:
				PR_failing();
				elog(PANIC, "Inconsistent internal status.");
				break;
		}
		if (page->cmd != PR_LOG)
		{
			invalidPageFound = PR_XLogHaveInvalidPages_int();
			SpinLockAcquire(&pr_invalidPages->slock);
			pr_invalidPages->invalidPageFound = invalidPageFound;
			SpinLockRelease(&pr_invalidPages->slock);
		}
		PR_freeBuffer(page, true);
	}
#ifdef WAL_DEBUG
	PRDebug_log("Terminating %s()\n", __func__);
#endif /* WAL_DEBUG */
	return loop_exit_code;
}


/*
 *****************************************************************************************************************
 * Block Worker
 *****************************************************************************************************************
 */

static int
blockWorkerLoop(void)
{
	PR_queue_el	*el;
	XLogDispatchData_PR	*data;
	int		*worker_list;
#ifdef WAL_DEBUG
	int		 sync_worker;
	char	 workername[64];
#endif
	
#ifdef WAL_DEBUG
	PRDebug_log("Started %s()\n", __func__);
#endif /* WAL_DEBUG */
#ifdef PR_IGNORE_REPLAY_ERROR
	pr_during_redo = false;
	set_sigsegv_handler();
#endif	/* PR_IGNORE_REPLAY_ERROR */
#ifdef PR_SKIP_REPLAY
	init_wait_time();
#endif	/* PR_SKIP_REPLAY */
	for (;;)
	{
		XLogRecPtr	currRecPtr;

#ifdef WAL_DEBUG
		PRDebug_log("%s: fetching queue\n", __func__);
		if (PR_loop_count >= PR_loop_num)
		{
			PR_loop_count = 0;
			PR_breakpoint();
		}
		else
			PR_loop_count++;
#endif	/* WAL_DEBUG */
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != XLogDispatchData)
		{
			PR_breakpoint();		/* GDB breakpoint */
			PR_failing();
			elog(PANIC, "Invalid internal status for block worker.");
		}
		data = (XLogDispatchData_PR *)(el->data);
		PR_freeQueueElement(el);

		SpinLockAcquire(&data->slock);

#ifdef WAL_DEBUG
		PRDebug_log("Fetched: ser_no: %ld, xlogrecord: \"%s\"\n", data->reader->ser_no, data->reader->xlog_string);
#endif	/* WAL_DEBUG */
		data->n_remaining--;
		currRecPtr = data->reader->ReadRecPtr;

		if (data->n_remaining > 0)
		{
			/* This worker is not eligible to handle this */
			SpinLockRelease(&data->slock);
			/* Wait another BLOCK worker to handle this and sync to me */
#ifdef WAL_DEBUG
			PRDebug_log("Xlogrecord is assigned to multiple worker.  Waiting for other workers to finish.\n");
			sync_worker =
#endif	/* WAL_DEBUG */
			PR_recvSync();
#ifdef WAL_DEBUG
			PRDebug_log("Sync reeived from the worker (%d).\n", sync_worker);
#endif	/* WAL_DEBUG */
		}
		else
		{
			unsigned flags;

			/* Dequeue */

			/* OK. I should handle this. Nobody is handling this and safe to release the lock. */

			SpinLockRelease(&data->slock);

			/* REDO */

			PR_redo(data);

			/* Koichi: TBD: ここで、XLogHistory と txn history の更新を行うこと */
			
			if ((data->reader->record->xl_info & XLR_CHECK_CONSISTENCY) != 0)
				checkXLogConsistency(data->reader);

			/*
			 * Koichi:
			 *
			 * Now this worker is the sole worker handling this dispatch data.
			 * All other workers are simple waiting for sync from me.
			 * No need to acquire lock any longer.
			 */

			/*
			 * Update lastReplayedEndRecPtr after this record has been
			 * successfully replayed.
			 */
			PR_setXLogReplayed(data->xlog_history_el);

			/*
			 * If rm_redo called XLogRequestWalReceiverReply, then we wake
			 * up the receiver so that it notices the updated
			 * lastReplayedEndRecPtr and sends a reply to the primary.
			 */
			if (doRequestWalReceiverReply)
			{
				doRequestWalReceiverReply = false;
				WalRcvForceReply();
			}

			/* Sync with other workers */
			for (worker_list = data->worker_list; *worker_list > 0; worker_list++)
			{
				if (*worker_list != my_worker_idx)
				{
#ifdef WAL_DEBUG
					PRDebug_log("Now synch other assigned %s.\n", PR_worker_name(*worker_list, workername));
#endif
					PR_sendSync(*worker_list);
#ifdef WAL_DEBUG
					PRDebug_log("Sync to %s done.\n", PR_worker_name(*worker_list, workername));
#endif
				}
			}

			/*
			 * Then remove this WAL record from the transaction WAL table.
			 */
			removeDispatchDataFromTxn(data, true);

			/* Following steps */
			if (doRequestWalReceiverReply)
			{
				doRequestWalReceiverReply = false;
				WalRcvForceReply();
			}

			/*
			 * After Dispatch Sync
			 * We don't need slock here because this is exlusive to this worker
			 */
			flags = data->flags;
			if (flags & PR_WK_SYNC)
			{
				if (flags & PR_WK_SYNC_READER)
					PR_sendSync(PR_READER_WORKER_IDX);
				if (flags & PR_WK_SYNC_DISPATCHER)
					PR_sendSync(PR_DISPATCHER_WORKER_IDX);
				if (flags & PR_WK_SYNC_TXN)
					PR_sendSync(PR_TXN_WORKER_IDX);
			}
			freeDispatchData(data);
		}
		SpinLockAcquire(&my_worker->slock);
		my_worker->handledRecPtr = currRecPtr;
		SpinLockRelease(&my_worker->slock);
	}
#ifdef WAL_DEBUG
	PRDebug_log("Terminating %s()\n", __func__);
#endif
	return loop_exit_code;
}

/*
 ****************************************************************************************************************
 * Transaction Worker
 ****************************************************************************************************************
 */

/*
 * Need to add hash key to take care of block worker data
 */
static int
txnWorkerLoop(void)
{
	PR_queue_el			*el;
	XLogDispatchData_PR *dispatch_data;
	XLogReaderState		*xlogreader;
	XLogRecord			*record;
	TransactionId		 xid;
	txn_cell_PR			*txn_cell;

#ifdef WAL_DEBUG
	PRDebug_log("Started %s()\n", __func__);
#endif
#ifdef PR_IGNORE_REPLAY_ERROR
	pr_during_redo = false;
	set_sigsegv_handler();
#endif
#ifdef PR_SKIP_REPLAY
	init_wait_time();
#endif	/* PR_SKIP_REPLAY */
	for (;;)
	{
		XLogRecPtr	currRecPtr;
		unsigned	flags;

#ifdef WAL_DEBUG
		PRDebug_log("%s: fetching queue\n", __func__);
		if (PR_loop_count >= PR_loop_num)
		{
			PR_loop_count = 0;
			PR_breakpoint();
		}
		else
			PR_loop_count++;
#endif
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != XLogDispatchData)
		{
			PR_breakpoint();		/* GDB breakpoint */
			PR_failing();
			elog(PANIC, "Invalid internal status for transaction worker.");
		}
		dispatch_data = (XLogDispatchData_PR *)(el->data);
		record = dispatch_data->reader->record;
		xlogreader = dispatch_data->reader;
		currRecPtr = xlogreader->ReadRecPtr;
		txn_cell = isTxnSyncNeeded(dispatch_data, record, &xid, true);
#ifdef WAL_DEBUG
		PRDebug_log("Fetched: ser_no: %ld, xlogrecord: \"%s\"\n", xlogreader->ser_no, xlogreader->xlog_string);
#endif
		if (txn_cell)
		{
			/*
			 * Here waits until all the preceding WAL records for this trancation
			 * are handled by block workers.
			 */
#ifdef WAL_DEBUG
			PRDebug_log("Need to wait until all the other workers handling this txn.\n");
#endif
			syncTxn(txn_cell);
#ifdef WAL_DEBUG
			PRDebug_log("Synch done.\n");
#endif

			/* Deallocate the trancaction cell */
			free_txn_cell(txn_cell, true);
		}

		/* REDO */

		PR_redo(dispatch_data);

		/*
		 * After redo, check whether the backup pages associated with
		 * the WAL record are consistent with the existing pages. This
		 * check is done only if consistency check is enabled for this
		 * record.
		 */
		if ((record->xl_info & XLR_CHECK_CONSISTENCY) != 0)
			checkXLogConsistency(xlogreader);

		/*
		 * Update lastReplayedEndRecPtr after this record has been
		 * successfully replayed.
		 */
		PR_setXLogReplayed(dispatch_data->xlog_history_el);

		/*
		 * If rm_redo called XLogRequestWalReceiverReply, then we wake
		 * up the receiver so that it notices the updated
		 * lastReplayedEndRecPtr and sends a reply to the primary.
		 */
		if (doRequestWalReceiverReply)
		{
			doRequestWalReceiverReply = false;
			WalRcvForceReply();
		}

		/*
		 * After Dispatch Sync
		 * We don't need slock here because this is exlusive to this worker
		 */
		flags = dispatch_data->flags;
		if (flags & PR_WK_SYNC)
		{
			if (flags & PR_WK_SYNC_READER)
				PR_sendSync(PR_READER_WORKER_IDX);
			if (flags & PR_WK_SYNC_DISPATCHER)
				PR_sendSync(PR_DISPATCHER_WORKER_IDX);
			if (flags & PR_WK_SYNC_TXN)
				PR_sendSync(PR_TXN_WORKER_IDX);
		}
		SpinLockAcquire(&my_worker->slock);
		my_worker->handledRecPtr = currRecPtr;
		SpinLockRelease(&my_worker->slock);
		
		freeDispatchData(dispatch_data);
	}
#ifdef WAL_DEBUG
	PRDebug_log("Terminating %s()\n", __func__);
#endif
	return loop_exit_code;
}

static void
getXLogRecordRmgrInfo(XLogReaderState *reader, RmgrId *rmgrid, uint8 *info)
{
	*rmgrid = XLogRecGetRmid(reader);
	if (info)
	{
		switch(*rmgrid)
		{
			case RM_XLOG_ID:
				*info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
				break;
			case RM_XACT_ID:
				*info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
				break;
			case RM_SMGR_ID:
			case RM_CLOG_ID:
			case RM_DBASE_ID:
			case RM_TBLSPC_ID:
			case RM_MULTIXACT_ID:
			case RM_RELMAP_ID:
			case RM_STANDBY_ID:
			case RM_HEAP2_ID:
			case RM_HEAP_ID:
			case RM_BTREE_ID:
			case RM_HASH_ID:
			case RM_GIN_ID:
			case RM_GIST_ID:
			case RM_SEQ_ID:
			case RM_SPGIST_ID:
			case RM_BRIN_ID:
			case RM_COMMIT_TS_ID:
			case RM_REPLORIGIN_ID:
				*info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
				break;
			case RM_GENERIC_ID:
				*info = 0;
				break;
			case RM_LOGICALMSG_ID:
				*info = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
				break;
			default:
				*info = 0;
				break;
		}
	}
}

static txn_cell_PR *
isTxnSyncNeeded(XLogDispatchData_PR *dispatch_data, XLogRecord *record, TransactionId *xid, bool remove_myself)
{
	txn_cell_PR		*txn_cell;
	RmgrId			 rmgr_id;
	uint8			 xact_info;


	*xid = dispatch_data->xid;
	if (dispatch_data->xid == InvalidTransactionId)
		return NULL;
	txn_cell = find_txn_cell(dispatch_data->xid, false, true);
	if (txn_cell == NULL)
		return NULL;
	if (txn_cell->head == NULL)
		goto return_null;
	if (remove_myself)
	{
		SpinLockAcquire(&pr_txn_hash->slock);
		if (txn_cell->tail == dispatch_data)
			removeDispatchDataFromTxn(dispatch_data, false);
		SpinLockRelease(&pr_txn_hash->slock);
	}
	getXLogRecordRmgrInfo(dispatch_data->reader, &rmgr_id, &xact_info);
	if (!checkRmgrTxnSync(rmgr_id, xact_info))
		goto return_null;
	return txn_cell;

return_null:
	free_txn_cell(txn_cell, true);
	return NULL;
}

static void
syncTxn(txn_cell_PR *txn_cell)
{
	txn_hash_el_PR	*txn_hash;
#ifdef WAL_DEBUG
	int	synched_worker;
	char workername[64];
#endif

	txn_hash = get_txn_hash(txn_cell->xid);

	SpinLockAcquire(&txn_hash->slock);
	if (txn_cell->head)
	{
		/*
		 * Koichi:
		 * Txn history chain の中の最後の Dispatch Data を処理した BLOCK WORKER は、txn_worker_waiting を
		 * false にして、ここに同期メッセージを書き込む
		 */
		txn_cell->txn_worker_waiting = true;
		SpinLockRelease(&txn_hash->slock);
#ifdef WAL_DEBUG
		PRDebug_log("Need to wait until all the xlogrecords of this transaction has been replayed.\n");
		synched_worker =
#endif
		PR_recvSync();
#ifdef WAL_DEBUG
		PRDebug_log("Synch done from %s\n", PR_worker_name(synched_worker, workername));
#endif
	}
	else
		/* All the preceding WALs have already been replayed */
		SpinLockRelease(&txn_hash->slock);

}

/*
 *************************************************************************************************************
 * Dispatcher worker
 *************************************************************************************************************
 */

static int
dispatcherWorkerLoop(void)
{
	PR_queue_el *el;
	XLogReaderState *reader;
	XLogRecord *record;
	XLogDispatchData_PR	*dispatch_data;
	int	*worker_list;
#ifdef WAL_DEBUG
	int	synched_worker;
	char workername[64];
#endif

#ifdef WAL_DEBUG
	PRDebug_log("Started %s()\n", __func__);
#endif
	for (;;)
	{
		XLogRecPtr	currRecPtr;
		bool		sync_needed;

		/* Dequeue */
#ifdef WAL_DEBUG
		PRDebug_log("Fetching queue\n");
#endif
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != ReaderState)
		{
			PR_breakpoint();
			PR_failing();
			elog(PANIC, "Invalid internal status for dispatcher worker.");
		}
		reader = (XLogReaderState *)el->data;
		currRecPtr = reader->ReadRecPtr;
#ifdef WAL_DEBUG
		PRDebug_log("Fetched.  Ser_no(%ld), xlog: \"%s\"\n", reader->ser_no, reader->xlog_string);
#endif
		PR_freeQueueElement(el);
		record = reader->record;
		dispatch_data = PR_analyzeXLogReaderState(reader, record);
		if (isSyncBeforeDispatchNeeded(reader))
			PR_syncBeforeDispatch();
		dispatchDataToXLogHistory(dispatch_data);
		addDispatchDataToTxn(dispatch_data, false);
		if (isSyncAfterDispatchNeeded(reader))
		{
			dispatch_data->flags = PR_WK_SYNC_DISPATCHER;
			sync_needed = true;
		}
		else
		{
			dispatch_data->flags = 0;
			sync_needed = false;
		}
		for (worker_list = dispatch_data->worker_list; *worker_list > PR_DISPATCHER_WORKER_IDX; worker_list++)
		{
#ifdef WAL_DEBUG
			PRDebug_log("Enqueue, ser_no(%ld) to %s, XLOGrecord: \"%s\"\n",
					reader->ser_no, PR_worker_name(*worker_list, workername), reader->xlog_string);
#endif
			PR_enqueue(dispatch_data, XLogDispatchData, *worker_list);
#ifdef WAL_DEBUG
			PRDebug_log("Enqueue done, ser_no(%ld) to %s\n", reader->ser_no, PR_worker_name(*worker_list, workername));
#endif
		}
		if (sync_needed)
		{
#ifdef WAL_DEBUG
			PRDebug_log("Syc needed for this xlog record.\n");
			synched_worker =
#endif
			PR_recvSync();	/* Only one WORKER actually handled this data sends sync. */
#ifdef WAL_DEBUG
			PRDebug_log("Sync from %s received.\n", PR_worker_name(synched_worker, workername));
#endif
		}

		SpinLockAcquire(&my_worker->slock);
		my_worker->handledRecPtr = currRecPtr;
		SpinLockRelease(&my_worker->slock);
	}
#ifdef WAL_DEBUG
	PRDebug_log("Terminating %s()\n", __func__);
#endif
	return loop_exit_code;
}

/*
 * Body of REDO, with various debug facility
 */
static void
PR_redo(XLogDispatchData_PR	*data)
{
#ifdef WAL_DEBUG
	PRDebug_log("Now I can replay the XLOGRecord.\n");
#endif	/* WAL_DEBUG */

	/* REDO */

#ifdef PR_SKIP_REPLAY
	wait_a_bit();
	return;
#else	/* PR_SKIP_REPLAY */
#ifdef PR_IGNORE_REPLAY_ERROR
	pr_during_redo = true;
	if (!setjmp(pr_jmpbuf))
	{
#endif	/* PR_IGNORE_REPLAY_ERROR */
	RmgrTable[data->reader->record->xl_rmid].rm_redo(data->reader);
#ifdef PR_IGNORE_REPLAY_ERROR
	}
#ifdef WAL_DEBUG
	else
		PRDebug_log("Error in replay function, ser_no: %ld, XLogRecord: %s",
				data->reader->ser_no, data->reader->xlog_string);
#endif	/* WAL_DEBUG */
	pr_during_redo = false;
#endif	/* PR_IGNORE_REPLAY_ERROR */
#endif	/* PR_SKIP_REPLAY */

#ifdef WAL_DEBUG
	PRDebug_log("Replay DONE. Ser_no(%ld)\n", data->reader->ser_no);
#endif	/* WAL_DEBUG */
}

/*
 * This is called by DISPATCHER WORKER before assigned to any other workers.
 * No lock is needed here.
 */
static void
dispatchDataToXLogHistory(XLogDispatchData_PR *dispatch_data)
{
	XLogReaderState		*reader;
	PR_XLogHistory_el	*el;

	reader = dispatch_data->reader;
	el = PR_addXLogHistory(reader->ReadRecPtr, reader->EndRecPtr, reader->timeline);
	dispatch_data->xlog_history_el = el;
}

/*
 * Handler for SIGSEGV, to isgnore this signal while in the replay process.   If not, then restore
 * the action to old state and signal SIGSEGV to myself.
 *
 * Due to ignorance of elog() for ERROR or higher error level, sugsequent redo routines may encounter
 * SIGSEGV due to inconsistent values in pages and other internal inforamtion.   To keep replay running
 * just for scalability estimation, we need to ignore such signal while redo code is running.
 *
 * If SIGSEGV occurs outside the redo routine, we need to handle this in usual way.
 */

#ifdef PR_IGNORE_REPLAY_ERROR
static pqsigfunc	oldfunc;

static void sigsegv_handler(int signo)
{
	Assert(signo == SIGSEGV);

	if (pr_during_redo)
		longjmp(pr_jmpbuf, 1);
	else
	{
		pqsignal(SIGSEGV, oldfunc);
		kill(getpid(), SIGSEGV);
	}
}

static void set_sigsegv_handler(void)
{
	oldfunc = pqsignal(SIGSEGV, sigsegv_handler);
}
#endif