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
#include <mqueue.h>
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
#include "storage/proc.h"
#include "utils/elog.h"



/*
 ************************************************************************************************
 * General definitions
 ************************************************************************************************
 */
/* When O0 is ised, it conflicts with inline function. */
#if 1
#define INLINE static
#else
#define INLINE inline
#endif

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
 * Parallel replay shared memory and common variables
 ************************************************************************************************
 */

static dsm_segment	*pr_shm_seg = NULL;

/* The following variables are initialized by PR_initShm() */
PR_shm   	*pr_shm = NULL;									/* Whole shared memory area */
static PR_txn_info			*pr_txn_info = NULL;			/* Chain of WAL for a transaction */
static PR_txn_hash_entry	*pr_txn_hash_entry = NULL;		/* Hash for TXN hash pool */
static PR_txn_cell			*pr_txn_cell_pool = NULL;		/* Pool of transaction track information */
static PR_invalidPages 		*pr_invalidPages = NULL;		/* Shared information of invalic page registration */
static PR_XLogHistory		*pr_history = NULL;				/* List of outstanding XLogRecord */
static PR_worker	*pr_worker = NULL;						/* Paralle replay workers */
static PR_queue		*pr_queue = NULL;						/* Dispatch queue element to workers */
static PR_buffer	*pr_buffer = NULL;						/* Buffer are for other usage */
static pid_t		 pr_reader_pid;							/* Reader PID */
#ifdef WAL_DEBUG
static StringInfo dump_txn_cell(StringInfo s, PR_txn_cell *txn_cell);
static void dump_worker_queue(StringInfo s, int worker_id, bool need_lock);
static char *queueTypeName(PR_QueueDataType type);
#endif

/* Queue lock */
#define lock_queue()	SpinLockAcquire(&pr_queue->slock)
#define unlock_queue()	SpinLockRelease(&pr_queue->slock)


/* My worker process knfo */
static int           my_worker_idx = 0;         /* Index of the worker.  Set when the worker starts. */
static PR_worker    *my_worker = NULL;      	/* My worker structure */
char				*my_worker_name = NULL;

/*
 ************************************************************************************************
 * Length definition
 ************************************************************************************************
 */

#define PR_MAXPATHLEN	512
#define PR_SYNC_RECV_BUF_SZ    64

/*
 ************************************************************************************************
 * Generral pointer and size operation
 ************************************************************************************************
 */

#define MemBoundary         	(sizeof(void *))
#define addr_forward(p, s)  	((void *)((uint64)(p) + (Size)(s)))
#define addr_backward(p, s) 	((void *)((uint64)(p) - (Size)(s)))
#define addr_after(a, b)    	((uint64)(a) > (uint64)(b))
#define addr_after_eq(a, b) 	((uint64)(a) >= (uint64)(b))
#define addr_before(a, b)   	((uint64)(a) < (uint64)(b))
#define addr_before_eq(a, b)    ((uint64)(a) <= (uint64)(b))

/* Addresses are casted to unsigned INT64 to calculate the distance */
INLINE Size
addr_difference(void *start, void * end)
{
	uint64 ustart = (uint64)start;
	uint64 uend = (uint64)end;

	if (ustart > uend)
		return (Size)(ustart - uend);
	else
		return (Size)(uend - ustart);
}


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

/* Lock macro */
#define lock_shm()		SpinLockAcquire(&pr_shm->shm_slock)
#define unlock_shm()	SpinLockRelease(&pr_shm->shm_slock)
#define lock_sync_status()		SpinLockAcquire(&pr_shm->sync_lock)
#define unlock_sync_status()	SpinLockRelease(&pr_shm->sync_lock)

/* Shared Memory Functions */

/*
 ***********************************************************************************************
 * Invalid Page worker
 ***********************************************************************************************
 */

/* Invalid Page info */
static void initInvalidPages(void);

/* Invalid page lock */
#define lock_invalid_page()		SpinLockAcquire(&pr_invalidPages->slock)
#define unlock_invalid_page()	SpinLockRelease(&pr_invalidPages->slock)


/*
 ***********************************************************************************************
 * XLog History functions
 ***********************************************************************************************
 */

INLINE Size xlogHistorySize(void);
static void initXLogHistory(void);

#define	lock_xlog_history()		SpinLockAcquire(&pr_history->slock)
#define	unlock_xlog_history()	SpinLockRelease(&pr_history->slock)
#define num_history_element		(num_preplay_worker_queue + 2)

/*
 ***********************************************************************************************
 * Worker functions
 ***********************************************************************************************
 */
/* Worker Functions and Macros */
INLINE Size worker_size(void);
static void initWorker(void);
#define lock_worker(idx)	SpinLockAcquire(&(pr_worker[idx].slock))
#define lock_my_worker()	SpinLockAcquire(&(my_worker->slock))
#define unlock_worker(idx)	SpinLockRelease(&(pr_worker[idx].slock))
#define unlock_my_worker()	SpinLockRelease(&(my_worker->slock))

/*
 ***********************************************************************************************
 * Queue functions
 ***********************************************************************************************
 */
INLINE Size queue_size(void);
static void initQueue(void);
static void initQueueElement(void);
#define queueData(el) ((el)->data)

/*
 ***********************************************************************************************
 * Buffer functions
 ***********************************************************************************************
 */
INLINE Size			 buffer_size(void);
static void			 initBuffer(void);
static void			*PR_allocBuffer_int(Size sz, bool need_lock, bool retry_opt);
static PR_BufChunk	*prev_chunk(PR_BufChunk *chunk);
static void			 free_chunk(PR_BufChunk *chunk);
static PR_BufChunk	*alloc_chunk(Size sz);
static PR_BufChunk	*concat_next_chunk(PR_BufChunk *chunk);
static PR_BufChunk	*concat_prev_chunk(PR_BufChunk *chunk);
#define concat_surrounding_chunks(c)	concat_prev_chunk(concat_next_chunk(c))
static void			*retry_allocBuffer(Size sz, bool need_lock);
INLINE PR_BufChunk	*next_chunk(PR_BufChunk *chunk);
#define CHUNK_OVERHEAD	(sizeof(PR_BufChunk) + sizeof(Size))
#define lock_buffer()	SpinLockAcquire(&pr_buffer->slock)
#define unlock_buffer()	SpinLockRelease(&pr_buffer->slock)
#ifdef WAL_DEBUG
static PR_BufChunk *find_chunk(StringInfo s, void *addr, bool need_lock);
#endif

/*
 ***********************************************************************************************
 * Dispatch data functions
 ***********************************************************************************************
 */
static void	freeDispatchData(XLogDispatchData_PR *dispatch_data, bool need_lock);
static bool checkRmgrTxnSync(RmgrId rmgrid, uint8 info);
static bool checkSyncBeforeDispatch(RmgrId rmgrid, uint8 info);
static bool isSyncBeforeDispatchNeeded(XLogReaderState *reader);
static bool checkSyncAfterDispatch(RmgrId rmgrid, uint8 info);
static bool isSyncAfterDispatchNeeded(XLogReaderState *reader);

#define DispatchDataGetXid(ddata)	XLogRecGetXid((ddata)->reader)
#define DispatchDataGetLSN(ddata)	((ddata)->reader->ReadRecPtr)
#define DispatchDataGetRecord(ddata)	((ddata)->reader->decoded_record)
#define DispatchDataGetReader(ddata)	((ddata)->reader)

#define DispatchDataSize	(pr_sizeof(XLogDispatchData_PR) \
		+ size_boundary((sizeof(bool) * num_preplay_workers)) \
		+ size_boundary((sizeof(int) * (num_preplay_workers + 1))))


/*
 ***********************************************************************************************
 * Transaction information functions
 ***********************************************************************************************
 */
static Size			 pr_txn_hash_size(void);
static void			 init_txn_hash(void);
static PR_txn_cell	*get_txn_cell(bool need_lock);
static void			 free_txn_cell(PR_txn_cell *txn_cell, bool need_lock);
static PR_txn_cell	*find_txn_cell(TransactionId xid, bool create, bool need_lock, bool *created);
static void			 addDispatchDataToTxn(XLogDispatchData_PR *dispatch_data, bool need_lock);
static void			 syncTxn_blockWorker(PR_txn_cell *txn_cell);
static void			 syncTxn(XLogDispatchData_PR *dispatch_data);
static void			 dispatchDataToXLogHistory(XLogDispatchData_PR *dispatch_data);
#ifdef WAL_DEBUG
static void dump_xlog_history(StringInfo s, bool need_lock);
#endif

#define NUM_HASH_ENTRY_FACTOR 10
#define num_hash_entry   (num_preplay_max_txn/NUM_HASH_ENTRY_FACTOR)
#define txn_cell_size()    (pr_sizeof(PR_txn_cell) + (pr_sizeof(XLogRecPtr) * (num_preplay_workers - PR_BLK_WORKER_MIN_IDX)))
#define get_txn_hash(xid) (&pr_txn_hash_entry[xid%num_hash_entry])
#define lock_txn_hash(xid)		SpinLockAcquire(&(get_txn_hash(xid)->slock))
#define unlock_txn_hash(xid)	SpinLockRelease(&(get_txn_hash(xid)->slock))
#define lock_txn_pool()		SpinLockAcquire(&pr_txn_info->cell_slock)
#define unlock_txn_pool()	SpinLockRelease(&pr_txn_info->cell_slock)
#define lock_dispatch_data(d)	SpinLockAcquire(&((d)->slock))
#define unlock_dispatch_data(d)	SpinLockRelease(&((d)->slock))

/*
 ***********************************************************************************************
 * XLogReaderState/XLogRecord functions
 ***********************************************************************************************
 */
static int	blockHash(int spc, int db, int rel, int blk, int n_max);
static void getXLogRecordRmgrInfo(XLogReaderState *reader, RmgrId *rmgrid, uint8 *info);


/*
 ***********************************************************************************************
 * Worker Loop
 ***********************************************************************************************
 */
static int loop_exit_code = 0;			/* Use this value as exit code of each worker */

static int dispatcherWorkerLoop(void);
static int txnWorkerLoop(void);
static int invalidPageWorkerLoop(void);
static int blockWorkerLoop(void);

/*
 ***********************************************************************************************
 * Redo function
 ***********************************************************************************************
 */

static void PR_redo(XLogDispatchData_PR	*data);


/*
 ************************************************************************************************
 * Test code: ignoring redo errors for test benchmark
 ************************************************************************************************
 */


/*
 ************************************************************************************************
 * Test code: skipping replay
 ************************************************************************************************
 */

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
 * Debug facililties
 ************************************************************************************************
 */

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

static bool dump_buffer(const char *funcname, StringInfo s, bool need_lock);
static void dump_chunk(PR_BufChunk *chunk, const char *funcname, StringInfo s, bool need_lock);
static char *now_timeofday(void);
static void dump_invalidPageData(XLogInvalidPageData_PR *page);
static char *forkNumName(ForkNumber forknum);
static char *invalidPageCmdName(PR_invalidPageCheckCmd cmd);
extern int PR_loop_num;
extern int PR_loop_count;

/* Function to stop at error detection */
void PR_error_here(void)
{
	return;
}

#endif

/*
 ************************************************************************************************
 *                                                                                              *
 * Function Bodies                                                                              *
 *                                                                                              *
 ************************************************************************************************
 */

/*
 *-----------------------------------------------------------------------------------------------
 * Synchronization Functions: synchronizing among worker processes
 *-----------------------------------------------------------------------------------------------
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
			PR_failing();
			elog(PANIC, "Failed to stat PR debug directory, %s", strerror(my_errno));
		}
        rv = mkdir(sync_sock_dir, S_IRUSR|S_IWUSR|S_IXUSR);
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
			ereport(FATAL,
					(errcode_for_socket_access(),
					 errmsg("Could not create the socket: %m")));
		}

		if (ii == my_worker_idx)
		{
			rc = bind(sync_sock_info[ii].syncsock, &sync_sock_info[ii].sockaddr, sizeof(struct sockaddr_un));
			if (rc < 0)
			{
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

    ll = sendto(sync_sock_info[worker_idx].syncsock, &my_worker_idx, sizeof(my_worker_idx), 0,
			&sync_sock_info[worker_idx].sockaddr, sizeof(struct sockaddr_un));
    if (ll != sizeof(my_worker_idx))
	{
#ifdef WAL_DEBUG
		PR_error_here();
#endif
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Can not send sync message from worker %d to %d: %m",
					 my_worker_idx, worker_idx)));
	}
}

/*
 *-------------------------------------------------------------------------------------------
 * Receive synchronization message from a worker.
 *
 * Return value indicates the worker sending synchronization message.
 *
 * This functions watches the timeout.   When timeout reaches, it checks if at least
 * one worker is running.   If not, reports error.
 *-------------------------------------------------------------------------------------------
 */
int
PR_recvSync(void)
{
	int		recv_data;
	int		ii;
	bool	is_some_running;
    Size    sz;
#if 0
	int		nn;
	static struct timeval *tv = NULL;
	fd_set	readfs;
	fd_set	fds;
#endif

    Assert (pr_shm);

#if 0
	if (tv == NULL)
	{
		tv = malloc(sizeof(struct timeval));
		tv->tv_sec = 5;	/* Recv Timeout */
		tv->tv_usec = 0;
	}
	FD_ZERO(&readfs);
	FD_SET(sync_sock_info[my_worker_idx].syncsock, &readfs);
#endif

	lock_sync_status();

	pr_worker[my_worker_idx].worker_waiting = true;

	for (ii = 0, is_some_running = false; ii < num_preplay_workers; ii++)
	{
		if (pr_worker[ii].worker_waiting == false)
		{
			is_some_running = true;
			break;
		}
	}

	unlock_sync_status();

	if (!is_some_running)
	{
#ifdef WAL_DEBUG
		PRDebug_log("**** All the parallel replay workers are waiting for something.  Cannot move forward.***\n");
#else
		PR_failing();
		elog(PANIC, "**** All the parallel replay workers are waiting for something.  Cannot move forward.***");
#endif
	}

    sz = recv(sync_sock_info[my_worker_idx].syncsock, &recv_data, sizeof(my_worker_idx), 0);

	lock_sync_status();

	pr_worker[my_worker_idx].worker_waiting = false;
	unlock_sync_status();

    if (sz < 0)
	{
#ifdef WAL_DEBUG
		PR_error_here();
#endif
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Could not receive message.  worker %d: %m", my_worker_idx)));
	}
    return(recv_data);
}

/*
 *-------------------------------------------------------------------------------------------
 * This is called only by the DISPATCHER worker when READER worker send SyncRequest.
 *
 * This function sends SyncRequest queue to all the undeylyinng workers, that is TXN and BLOCK
 * workers and get a request.  If "termiate" request is set to this worker by READERR .
 *
 * If terminate is true, then tell other workers to terminate and sync until they all terminates.
 *-------------------------------------------------------------------------------------------
 */

void
PR_syncAll(bool terminate)
{
	int		ii;
	bool	terminated;

	Assert(my_worker_idx == PR_DISPATCHER_WORKER_IDX);

	for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers; ii++)
	{
		PR_enqueue(NULL, terminate ? RequestTerminate : RequestSync, ii);
		if (terminate)
		{
			while(true)
			{
				lock_worker(ii);
				terminated = pr_worker[ii].worker_terminated;
				unlock_worker(ii);
				if (terminated == true)
					break;
				else
				{
					PR_enqueue(NULL, RequestTerminate, ii);
					PR_recvSync();
				}
			}
		}
	}
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
		+ (my_txn_hash_sz = pr_txn_hash_size())
		+ (my_invalidP_sz = pr_sizeof(PR_invalidPages))
		+ (my_history_sz = xlogHistorySize())
		+ (my_worker_sz = worker_size())
	   	+ (my_queue_sz = queue_size())
		+ buffer_size();

	pr_shm_seg = dsm_create(my_shm_size, 0);
	
	pr_shm = dsm_segment_address(pr_shm_seg);
	pr_shm->txn_info = pr_txn_info = addr_forward(pr_shm, pr_sizeof(PR_shm));
	pr_shm->invalidPages = pr_invalidPages = addr_forward(pr_txn_info, my_txn_hash_sz);
	pr_shm->history = pr_history = addr_forward(pr_invalidPages, pr_sizeof(PR_invalidPages));
	pr_shm->workers = pr_worker = addr_forward(pr_history, my_history_sz);
	pr_shm->queue = pr_queue = addr_forward(pr_worker, my_worker_sz);
	pr_shm->buffer = pr_buffer = addr_forward(pr_queue, my_queue_sz);
	pr_shm->some_failed = false;
	pr_shm->EndRecPtr = InvalidXLogRecPtr;
	pr_shm->MinTimeLineID = 0;
	pr_reader_pid = pr_shm->reader_pid = getpid();

	SpinLockInit(&pr_shm->shm_slock);

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
	pr_txn_info = NULL;
	pr_txn_hash_entry = NULL;
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

static Size
pr_txn_hash_size(void)
{
	/*
	 * Koichi: 実は、Standby で動作している場合、この MaxConnections は
	 * Primary のものでなければならない。この部分も後で見直す必要がある。
	 */
	if (num_preplay_max_txn < MaxConnections)
		num_preplay_max_txn = MaxConnections;
	return (pr_sizeof(PR_txn_info) + (pr_sizeof(PR_txn_hash_entry) * num_hash_entry)
			+ (txn_cell_size() * num_preplay_max_txn));
}

#ifdef WAL_DEBUG
static PR_txn_cell *txn_cell_head;
static PR_txn_cell *txn_cell_tail;
#endif

static void
init_txn_hash(void)
{
	int	ii, jj;
	PR_txn_cell			*pooled_cell;
	PR_txn_cell			*prev_cell;
	PR_txn_hash_entry	*txn_hash_entry;


	pr_txn_info = pr_shm->txn_info;
	pr_txn_info->free_in_pool = num_preplay_max_txn;
	pr_txn_info->total_available = num_preplay_max_txn;
	pr_txn_info->txn_hash = pr_txn_hash_entry = (PR_txn_hash_entry *)addr_forward(pr_txn_info, pr_sizeof(PR_txn_info));
	pr_txn_info->cell_pool = pr_txn_cell_pool = (PR_txn_cell *)addr_forward(pr_txn_info->txn_hash, pr_sizeof(PR_txn_hash_entry) * num_hash_entry);
	SpinLockInit(&pr_txn_info->cell_slock);

	/* Initialize txn_hash_entries */
	for (ii = 0, txn_hash_entry = pr_txn_info->txn_hash; ii < num_hash_entry; ii++)
	{
		SpinLockInit(&txn_hash_entry[ii].slock);
		txn_hash_entry[ii].head = txn_hash_entry[ii].tail = NULL;
	}

#ifdef WAL_DEBUG
	txn_cell_head = pr_txn_info->cell_pool;
#endif
	/* Initialize TXN cell pool */
	for (ii = 0, pooled_cell = pr_txn_info->cell_pool; ii < num_preplay_max_txn; ii++, pooled_cell = pooled_cell->next)
	{
		XLogRecPtr *lastLSN_array;

		prev_cell = pooled_cell;
		pooled_cell->next = addr_forward(pooled_cell, txn_cell_size());
		pooled_cell->prev = NULL;
		pooled_cell->xid = InvalidTransactionId;
		pooled_cell->blk_worker_to_wait = 0;
		lastLSN_array = &(pooled_cell->lastXLogLSN[0]);

		/* Initialize last LSN array */
		for (jj = 0; jj < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); jj++)
			lastLSN_array[jj] = InvalidXLogRecPtr;

	}
	prev_cell->next = NULL;

#ifdef WAL_DEBUG
	txn_cell_tail = addr_forward(prev_cell, txn_cell_size());
#endif

}

#define txn_hash_value(x)	((x) % txn_hash_size)

/*
 * It is very very rare but as a logic, we have a chance
 * to return NULL value.
 */
static PR_txn_cell *
get_txn_cell(bool need_lock)
{
	PR_txn_cell	*rv;
#ifdef WAL_DEBUG
	static long my_count = 0;

	my_count++;
#endif
	if (need_lock)
		lock_txn_pool();
#ifdef WAL_DEBUG
	if (pr_txn_cell_pool->next == NULL)
		PR_error_here();
#endif
	rv = pr_txn_cell_pool->next;
	if (rv)
	{
#ifdef WAL_DEBUG
		if (rv && (addr_before(rv, txn_cell_head) || addr_after_eq(rv, txn_cell_tail)))
		{
			PRDebug_log("%s(), my_count: %ld, return value %p is out of bounds, txn_cell_head: %p, txn_cell_tail: %p\n",
					__func__, my_count, rv, txn_cell_head, txn_cell_tail);
			PR_error_here();
		}
#endif
		pr_txn_cell_pool->next = rv->next;
		rv->next = NULL;
		pr_txn_info->free_in_pool--;
	}
	if (need_lock)
		unlock_txn_pool();

#ifdef WAL_DEBUG
	PRDebug_log("%s() returning txn_cell %p, totqal cell: %ld, remaining: %ld\n",
			__func__, rv, pr_txn_info->total_available, pr_txn_info->free_in_pool);
	if (rv == NULL)
		PR_error_here();
#endif
	return rv;
}

/*
 * The cell must have been chained from txn hash entry
 */
static void
free_txn_cell(PR_txn_cell *txn_cell, bool need_lock)
{
	PR_txn_hash_entry	*hash_entry;
	TransactionId		 xid;
	XLogRecPtr		    *lastLSN_array;
	int	ii;

#ifdef WAL_DEBUG
	if (addr_before(txn_cell, txn_cell_head) || addr_after(txn_cell, txn_cell_tail))
	{
		PRDebug_log("Returning cell %p is invalid.  Worker ID: %d\n", txn_cell, my_worker_idx);
		PR_error_here();
	}
	PRDebug_log("%s(): returning %p, xid=%d, worker_id: %d\n", __func__, txn_cell, txn_cell->xid, my_worker_idx);
#endif

	xid = txn_cell->xid;
	hash_entry = get_txn_hash(xid);
	if (need_lock)
		lock_txn_hash(xid);
	/*
	 * remove from hash chain.
	 */
	if (txn_cell->prev == NULL)
	{
		/* This is the first entry in the hash chain */
		hash_entry->head = txn_cell->next;
		if (hash_entry->head)
			hash_entry->head->prev = NULL;
		else
			/* No cell from the hash entry */
			hash_entry->tail = NULL;
	}
	else if (txn_cell->next == NULL)
	{
		/* This is the last cell in the hash chain */
		hash_entry->tail = txn_cell->prev;
		if (hash_entry->tail)
			hash_entry->tail->next = NULL;
		else
			/* No cell from the hash entry, this code may not be needed. */
			hash_entry->tail = NULL;
	}
	else
	{
		txn_cell->next->prev = txn_cell->prev;
		txn_cell->prev->next = txn_cell->next;
	}

	if (need_lock)
		unlock_txn_hash(xid);

	/* Reset the cell */
	txn_cell->xid = InvalidTransactionId;
	txn_cell->blk_worker_to_wait = 0;
	lastLSN_array= &txn_cell->lastXLogLSN[0];
	for (ii = 0; ii < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); ii++)
		lastLSN_array[ii] = InvalidXLogRecPtr;

	/* Return the cell to cell pool */
	if (need_lock)
		lock_txn_pool();
	txn_cell->prev = NULL;
	txn_cell->next = pr_txn_cell_pool->next;
	pr_txn_cell_pool->next = txn_cell;
	pr_txn_info->free_in_pool++;
#ifdef WAL_DEBUG
	PRDebug_log("%s(): returning txn_cell %p, total %ld, remaining %ld\n",
			__func__, txn_cell, pr_txn_info->total_available, pr_txn_info->free_in_pool);
#endif
	if(need_lock)
		unlock_txn_pool();
}

/*
 * When assigning new PR_txn_cell from the cell pool, this will be chaned from
 * corresponding hash entry.
 */
static PR_txn_cell *
find_txn_cell(TransactionId xid, bool create, bool need_lock, bool *created)
{
	PR_txn_hash_entry	*hash_entry;
	PR_txn_cell			*txn_cell;
	int	ii;

	Assert(created);

#ifdef WAL_DEBUG
	PRDebug_log("%s(xid: %d, create: %s)\n", __func__, xid, create == true ? "true" : "false");
#endif
	*created = false;

	hash_entry = get_txn_hash(xid);
	if (need_lock)
		lock_txn_hash(xid);
	for (txn_cell = hash_entry->head; txn_cell; txn_cell = txn_cell->next)
	{
#ifdef WAL_DEBUG
		if (addr_before(txn_cell, txn_cell_head) || addr_after_eq(txn_cell, txn_cell_tail))
		{
			PRDebug_log("%s(), txn_cell(%p) out of bounds, txn_cell_head: %p, txn_cell_tail: %p.\n",
					__func__, txn_cell, txn_cell_head, txn_cell_tail);
			PR_error_here();
		}
#endif
		if (txn_cell->xid == xid)
			goto return_value;
	}

	/* No cell found */

	if (!create)
		goto return_null;

	/* Allocate a txn cell */

	if (need_lock)
		unlock_txn_hash(xid);

	txn_cell = get_txn_cell(need_lock);

#ifdef WAL_DEBUG
	if (txn_cell == NULL)
	{
		PRDebug_log("%s(%d, true) returning NULL.\n", __func__, xid);
		PR_error_here();
	}
#endif

	if (need_lock)
		lock_txn_hash(xid);

	if (txn_cell == NULL)
		goto return_null;

	/* Initialize the cell data */
	txn_cell->xid = xid;
	txn_cell->blk_worker_to_wait = 0;
	for (ii = 0; ii < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); ii++)
	{
		txn_cell->lastXLogLSN[ii] = InvalidXLogRecPtr;
	}

	/* Chain the cell to txn hash entry */

	if (hash_entry->tail == NULL)
	{
		/* Nothing in the hash entry chain */
		hash_entry->head = hash_entry->tail = txn_cell;
		txn_cell->next = txn_cell->prev = NULL;
	}
	else
	{
		/* Already cell in the hash entry chain */
		txn_cell->prev = hash_entry->tail;
		hash_entry->tail->next = txn_cell;
		txn_cell->next = NULL;
		hash_entry->tail = txn_cell;
	}
	*created = true;
	goto return_value;

return_null:
	if (need_lock)
		unlock_txn_hash(xid);
	*created = false;
#ifdef WAL_DEBUG
	PRDebug_log("%s(): returning NULL.\n", __func__);
#endif
	return NULL;

return_value:
	if (need_lock)
		unlock_txn_hash(xid);

#ifdef WAL_DEBUG
	PRDebug_log("%s(): returning %p, created: %s\n", __func__, txn_cell, *created == true ? "true" : "false");
#endif
	return txn_cell;

}

/*
 * If need_lock is false, caller must acquire the lock on pr_buffer.
 */
static void
freeDispatchData(XLogDispatchData_PR *dispatch_data, bool need_lock)
{
#ifdef WAL_DEBUG
	static StringInfo s = NULL;
#endif
	Assert(dispatch_data);

	/*
	 * Now we have up to four shm area to free. 
	 * Let's acquire the lock here to avoid the chance of lock conflict.
	 */
	if (need_lock)
		lock_buffer();
#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);

	appendStringInfo(s, "=========== %s() freeing shared buffer ===========\n", __func__);

	if (dispatch_data->reader->xlog_string)
	{
		appendStringInfoString(s, "dispatch_data->reader->xlog_string: ");
		PR_bufferCheck(s, dispatch_data->reader->xlog_string, false);
		PRDebug_out(s);
		PR_freeBuffer(dispatch_data->reader->xlog_string, false);
		resetStringInfo(s);
	}
#endif
	if (dispatch_data->reader)
	{
		int ii;

		Assert(dispatch_data->reader->for_parallel_replay == true);

		if (dispatch_data->reader->main_data)
		{
#ifdef WAL_DEBUG
			appendStringInfoString(s, "dispatch_data->reader->main_data: ");
			PR_bufferCheck(s, dispatch_data->reader->main_data, false);
			PRDebug_out(s);
			resetStringInfo(s);
#endif
			PR_freeBuffer(dispatch_data->reader->main_data, false);
		}
		if (dispatch_data->reader->decoded_record)
		{
#ifdef WAL_DEBUG
			appendStringInfoString(s, "dispatch_data->reader->decoded_record: ");
			PR_bufferCheck(s, dispatch_data->reader->decoded_record, false);
			PRDebug_out(s);
			resetStringInfo(s);
#endif
			PR_freeBuffer(dispatch_data->reader->decoded_record, false);
		}
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
			if (dispatch_data->reader->blocks[ii].data)
			{
#ifdef WAL_DEBUG
				appendStringInfo(s, "blk: %d: ", ii);
				PR_bufferCheck(s, dispatch_data->reader->blocks[ii].data, false);
				PRDebug_out(s);
				resetStringInfo(s);
#endif
				PR_freeBuffer(dispatch_data->reader->blocks[ii].data, false);
			}
		}
#ifdef WAL_DEBUG
		appendStringInfoString(s, "dispatch_data->reader: ");
		PR_bufferCheck(s, dispatch_data->reader, false);
		PRDebug_out(s);
		resetStringInfo(s);
#endif
		PR_freeBuffer(dispatch_data->reader, false);
	}
#ifdef WAL_DEBUG
	PRDebug_out(s);
	resetStringInfo(s);
	appendStringInfoString(s, "dispatch_data: ");
	PR_bufferCheck(s, dispatch_data, false);
#endif
	PR_freeBuffer(dispatch_data, false);
	if (need_lock)
		unlock_buffer();
}

/*
 * Called from DISPATCER worker.   After analyzing XLogReaderState with XLogRecord and determine
 * which worker to assign, this function takes care of transaction cell information.
 *
 * Specified XLogRecord may be assigned to more than one BLOCK worker if it contains multiple block
 * information.
 */

#ifdef WAL_DEBUG
static StringInfo
dump_txn_cell(StringInfo s, PR_txn_cell *txn_cell)
{
	int	ii;
	appendStringInfo(s, "Transaction Cell: xid: %d, next: %p, prev: %p, blk_worker_to_wait: %d\n\t",
			txn_cell->xid, txn_cell->next, txn_cell->prev, txn_cell->blk_worker_to_wait);
	for (ii = 0; ii < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); ii++)
		appendStringInfo(s, "lastXLogLSN[%d]: %016lx, ", ii, txn_cell->lastXLogLSN[ii]);
	appendStringInfoChar(s, '\n');
	return s;
}
#endif

static void
updateTxnInfoAtAssign(TransactionId xid, XLogRecPtr lsn, bool *assigned_blk_workers, bool need_lock)
{
	PR_txn_cell		*txn_cell;
	bool			 created;
	int				 ii;
#ifdef WAL_DEBUG
	static StringInfo	s = NULL;
#endif

	Assert(my_worker_idx == PR_DISPATCHER_WORKER_IDX);

#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	appendStringInfo(s, "%s: xid = %d, lsn: %016lx, ", __func__, xid, lsn);
	for (ii = 0; ii < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); ii++)
		appendStringInfo(s, "wkr_idx: %d, assigned: %s,",
				ii, assigned_blk_workers[ii] == true ? "true" : "false");
	appendStringInfoChar(s, '\n');
#endif

	if (xid == InvalidTransactionId)
		return;

	txn_cell = find_txn_cell(xid, true, need_lock, &created);
	if (txn_cell == NULL)
		elog(PANIC, "Could not assign transaction cell for replay.");

#ifdef WAL_DEBUG
	dump_txn_cell(s, txn_cell);
	PRDebug_out(s);
	resetStringInfo(s);
#endif
	if (need_lock)
		lock_txn_hash(xid);

	/*
	 * Note that assigned_workers contains all the worker such as READER WORKER.
	 * We need information for BLOCK workers so need to begin with first BLOCK worker
	 * index to scan assigned_workers.
	 */
	for (ii = 0; ii < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); ii++)
	{
		/*
		 * If this block worker is not assigned for this XLogRecord,
		 * lastXLogLSN value remains as is, not cleared.
		 */
		if (assigned_blk_workers[ii] == true)
			txn_cell->lastXLogLSN[ii] = lsn;
	}

	if (need_lock)
		unlock_txn_hash(xid);
}

static void
addDispatchDataToTxn(XLogDispatchData_PR *dispatch_data, bool need_lock)
{
#ifdef WAL_DEBUG
	PRDebug_log("%s()\n", __func__);
#endif
	if (dispatch_data->xid == InvalidTransactionId || dispatch_data->n_involved == 0)
		return;
	updateTxnInfoAtAssign(dispatch_data->xid, dispatch_data->reader->ReadRecPtr, dispatch_data->worker_array + PR_BLK_WORKER_MIN_IDX, need_lock);
}

/*
 * Update TXN information after replaying WAL record, by BLOCK worker.
 */
static void
updateTxnInfoAfterReplay(TransactionId xid, XLogRecPtr lsn, bool need_lock)
{
	PR_txn_cell	*txn_cell;
	bool		 created;
#ifdef WAL_DEBUG
	static StringInfo	s = NULL;
#endif

	Assert(my_worker_idx >= PR_BLK_WORKER_MIN_IDX);
	Assert(xid != InvalidTransactionId);
#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
	appendStringInfo(s, "%s(): xid = %d, lsn: %016lx, ", __func__, xid, lsn);
#endif

	txn_cell = find_txn_cell(xid, false, need_lock, &created);
	if (txn_cell == NULL)
	{
		elog(LOG, "No transaction info found while replaying.");
		return;
	}
#ifdef WAL_DEBUG
	dump_txn_cell(s, txn_cell);
	PRDebug_out(s);
	resetStringInfo(s);
#endif
	if (need_lock)
		lock_txn_hash(xid);
	if (txn_cell->lastXLogLSN[my_worker_idx - PR_BLK_WORKER_MIN_IDX] == lsn)
	{
#if 0
#ifdef WALDEBUG
		/* Koichi: トランザクションの終わりの試験のため */
		PR_error_here();
#endif
#endif
		/* OK. This is the last LSN assigned to me in the list */
		txn_cell->lastXLogLSN[my_worker_idx - PR_BLK_WORKER_MIN_IDX] = InvalidXLogRecPtr;
		if (txn_cell->blk_worker_to_wait == my_worker_idx)
		{
			/* TXN worker is waiting for me */
			txn_cell->blk_worker_to_wait = 0;
			if (need_lock)
				unlock_txn_hash(xid);
			PR_sendSync(PR_TXN_WORKER_IDX);
			return;
		}
	}
	if (need_lock)
		unlock_txn_hash(xid);
	return;
}


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
	PR_recvSync();
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
	PR_recvSync();
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
	PR_recvSync();
}

bool
PR_XLogHaveInvalidPages(void)
{
	bool	rv;

	lock_invalid_page();
	rv = pr_invalidPages->invalidPageFound;
	unlock_invalid_page();
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
	PR_recvSync();
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
	return (pr_sizeof(PR_XLogHistory) + (pr_sizeof(PR_XLogHistory_el) * num_history_element));
}

static void
initXLogHistory(void)
{
	PR_XLogHistory_el	*el;
	int	ii;

	Assert(pr_history);

	el = (PR_XLogHistory_el *)addr_forward(pr_history, pr_sizeof(PR_XLogHistory));

	pr_history->head = pr_history->tail = &el[0];
	pr_history->hist_count = 0;
	pr_history->num_elements = 0;
	pr_history->num_whole_elements = num_history_element;

	/* Make el.next circular list */
	for (ii = 0; ii < num_history_element; ii++)
	{
		el[ii].next = &el[ii+1];
		el[ii].curr_ptr = InvalidXLogRecPtr;
		el[ii].end_ptr = InvalidXLogRecPtr;
		el[ii].my_timeline = 0;
		el[ii].replayed = false;
	}
	el[num_history_element - 1].next = &el[0];

	SpinLockInit(&pr_history->slock);
}

PR_XLogHistory_el *
PR_addXLogHistory(XLogRecPtr currPtr, XLogRecPtr endPtr, TimeLineID myTimeline, long ser_no, TransactionId xid)
{
	PR_XLogHistory_el	*el;
#ifdef WAL_DEBUG
	static StringInfo s = NULL;
#endif

	Assert(pr_history);

#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
#endif
	lock_xlog_history();

	if (pr_history->num_elements >= num_history_element)
	{
		unlock_xlog_history();
		elog(FATAL, "Could not allocate XLog replay history element.");
		return NULL;
	}

	if (pr_history->num_elements == 0)
	{
		el = pr_history->head;
		pr_history->tail = el->next;
	}
	else
	{
		el = pr_history->tail;
		pr_history->tail = el->next;
	}
	el->curr_ptr = currPtr;
	el->end_ptr = endPtr;
	el->my_timeline = myTimeline;
	el->replayed = false;
	el->ser_no = ser_no;
	el->xid = xid;
	pr_history->num_elements++;

	unlock_xlog_history();

#ifdef WAL_DEBUG
	appendStringInfo(s, "\n========== Add XLogHistory ================\n"
			    "%s: returning %p, num_elements: %d, currPtr %016lx, endPtr: %016lx, Timeline: %d, ser_no: %ld, xid: %d\n",
			    __func__, el, pr_history->num_elements, currPtr, endPtr, myTimeline, ser_no, xid);
	dump_xlog_history(s, true);
	PRDebug_out(s);
#endif
	
	return el;
}

void
PR_setXLogReplayed(PR_XLogHistory_el *el)
{
	PR_XLogHistory_el	*last_el;
	PR_XLogHistory_el	*curr_el;
	PR_XLogHistory_el	*head;
#ifdef WAL_DEBUG
	static StringInfo s = NULL;
#endif

	Assert(pr_history);

#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);

	appendStringInfoString(s, "\n=========== Cleaning up XLogHistory =================\n");
#endif

	lock_xlog_history();

	el->replayed = true;
	if (pr_history->head != el)
	{
		/* Still head is not replayed yet. */
		unlock_xlog_history();
		return;
	}


	for (curr_el = pr_history->head; curr_el != pr_history->tail; curr_el = curr_el->next)
	{
		if (curr_el->replayed == true)
		{
			last_el = curr_el;
			pr_history->num_elements--;
			head = curr_el->next;
			continue;
		}
		else
		{
			head = curr_el;
			break;
		}
	}
	pr_history->head = head;

	/*
	 * Then all the elements were replayed
	 * We can update EndPtr and cleanup the list
	 */
	XLogCtlDataUpdatePtr(last_el->end_ptr, last_el->my_timeline, true);

	/* May need unlock after pointer was updated */
	unlock_xlog_history();

#ifdef WAL_DEBUG
	dump_xlog_history(s, true);
	PRDebug_out(s);
#endif

}

#ifdef WAL_DEBUG
static void
dump_xlog_history(StringInfo s, bool need_lock)
{
	PR_XLogHistory_el *el;

#if 1
	return;
#endif
	if (need_lock)
		lock_xlog_history();
	appendStringInfo(s, "======== XLogHistory =================\n");
	appendStringInfo(s, "head: %p, tail: %p, hist_count: %ld, num_whole_elements: %d, num_elements: %d\n",
			pr_history->head, pr_history->tail, pr_history->hist_count, pr_history->num_whole_elements, pr_history->num_elements);
	for (el = pr_history->head; el != pr_history->tail; el = el->next)
	{
		appendStringInfo(s, "\tElement: %p, next: %p, curr_ptr: %016lx, end_ptr: %016lx, timeline: %d, ser_no: %ld, xid: %d,  replayed: %s\n",
				el, el->next, el->curr_ptr, el->end_ptr, el->my_timeline, el->ser_no, el->xid, el->replayed ? "true" : "false");
	}
	if (need_lock)
		unlock_xlog_history();
}
#endif

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
	lock_my_worker();
	my_worker->worker_failed = true;
	unlock_my_worker();
	lock_shm();
	pr_shm->some_failed = true;
	unlock_shm();
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
	my_worker_name = PR_worker_name(worker_idx, NULL);
	my_worker = &pr_worker[worker_idx];
}

char *
PR_worker_name(int worker_idx, char *buff)
{
	StringInfo s = NULL;

	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);

	if (worker_idx == PR_READER_WORKER_IDX)
		appendStringInfoString(s, "READER_0");
	else if (worker_idx == PR_DISPATCHER_WORKER_IDX)
		appendStringInfoString(s, "DISPATCHER_1");
	else if (worker_idx == PR_TXN_WORKER_IDX)
		appendStringInfoString(s, "TXN WORKER_2");
	else if (worker_idx == PR_INVALID_PAGE_WORKER_IDX)
		appendStringInfoString(s, "INVALID PAGE WORKER_3");
	else
		appendStringInfo(s, "BLOCK WORKER_%d(%d)", worker_idx, worker_idx - PR_BLK_WORKER_MIN_IDX);
	if (buff == NULL)
		buff = strdup(s->data);
	else
		strcpy(buff, s->data);
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
		worker->worker_terminated = false;
		worker->num_queued_el = 0;
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

	lock_my_worker();
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
	my_worker->worker_pid = getpid();
	unlock_my_worker();

	MyProc->isParallelReplayWorker = true;

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

	/*
	 * Send DISPATCHER terminate rquest
	 */
	PR_enqueue(NULL, RequestTerminate, PR_DISPATCHER_WORKER_IDX);

	/* First, need to terminate DISPATCHER worker */
	lock_worker(PR_DISPATCHER_WORKER_IDX);
	if (pr_worker[PR_DISPATCHER_WORKER_IDX].wait_dispatch == true)
	{
		pr_worker[PR_DISPATCHER_WORKER_IDX].wait_dispatch = false;
		unlock_worker(PR_DISPATCHER_WORKER_IDX);
		PR_sendSync(PR_DISPATCHER_WORKER_IDX);
		lock_worker(PR_DISPATCHER_WORKER_IDX);
	}
	unlock_worker(PR_DISPATCHER_WORKER_IDX);

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
	lock_worker(PR_INVALID_PAGE_WORKER_IDX);
	if (pr_worker[PR_INVALID_PAGE_WORKER_IDX].wait_dispatch == true)
	{
		pr_worker[PR_INVALID_PAGE_WORKER_IDX].wait_dispatch = false;
		unlock_worker(PR_INVALID_PAGE_WORKER_IDX);
		PR_sendSync(PR_INVALID_PAGE_WORKER_IDX);
		lock_worker(PR_INVALID_PAGE_WORKER_IDX);
	}
	unlock_worker(PR_INVALID_PAGE_WORKER_IDX);

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

	lock_worker(PR_DISPATCHER_WORKER_IDX);
	unlock_worker(PR_DISPATCHER_WORKER_IDX);

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
		el->data_type = PR_QueueInit;
		el->data = NULL;
	}
	el--;
	el->next = NULL;
}

static PR_queue_el *
getQueueElement(void)
{
	PR_queue_el	*el;

	lock_queue();
	if (pr_queue->element == NULL)
	{
		pr_queue->wait_worker_list[my_worker_idx] = true;
		unlock_queue();
		/* Wait for another worker to free a queue element */
		PR_recvSync();
		return getQueueElement();
	}
	else
	{
		el = pr_queue->element;
		pr_queue->element = el->next;
		unlock_queue();
		return el;
	}
	return NULL;	/* Never comes here */
}

void
PR_freeQueueElement(PR_queue_el *el)
{
	int ii;

	memset(el, 0, sizeof(PR_queue_el));
	lock_queue();
	el->next = pr_queue->element;
	pr_queue->element = el;
	for (ii = num_preplay_workers - 1; ii >= 0; ii--)
	{
		if (pr_queue->wait_worker_list[ii])
		{
			pr_queue->wait_worker_list[ii] = false;
			unlock_queue();
			PR_sendSync(ii);
			return;
		}
	}
	unlock_queue();
	return;
}


typedef enum PR_MqDataType
{
	PR_Mq_Init = 0,
	PR_MqData,
	PR_MqSyncData,
	PR_MqMax_Value
} PR_MqDataType;

typedef struct PR_mqueue_data PR_mqueue_data;

struct PR_mqueue_data
{
	PR_MqDataType	 cmd;
	int				 sender_worker_idx;
	int				 receiver_worker_idx;
	uint64			 ser_no;
	void			*data;
};

typedef struct PR_MqInfo PR_MqInfo;

struct PR_MqInfo
{
	mqd_t	 queue;
	bool	 opened;
	char	*queue_name;
};

static PR_MqInfo *queue_info = NULL;

/* Mqueue attributes */
static struct mq_attr queue_attr =
{
	.mq_flags = 0,
	.mq_maxmsg = 10,
	.mq_msgsize = sizeof(struct PR_mqueue_data),
	.mq_curmsgs = 0
};

/*
 * Wrapper for malloc().
 */
static void *
Malloc(Size sz)
{
	void *rv;

	rv = malloc(sz);
	if (rv == NULL)
		elog(PANIC, "Out of memory.  Exiting.");
	return rv;
}

#define	QUEUENAMLEN	128

/*
 * Create POSIX message queue to pass WAL records.
 */
void
PR_initMq(void)
{
	int ii;

	/* MQ information area */
	queue_info = (PR_MqInfo *)Malloc(sizeof(PR_MqInfo) * num_preplay_workers);
	for (ii = 0; ii < num_preplay_workers; ii++)
	{
		queue_info[ii].queue_name = (char *)Malloc(QUEUENAMLEN);
		snprintf(queue_info[ii].queue_name, QUEUENAMLEN, "/pg_%d_%03d", pr_reader_pid, ii);
		mq_unlink(queue_info[ii].queue_name);
		queue_info[ii].queue = mq_open(queue_info[ii].queue_name, O_CREAT | O_RDWR, 0600, &queue_attr);
		if (queue_info[ii].queue < 0)
			elog(PANIC, "Mq_open() error.\n");
		queue_info[ii].opened = true;
	}
}

/*
 * Remove all the message queues.
 */
void
PR_finishMq(void)
{
	int	ii;
	for (ii = 0; ii < num_preplay_workers; ii++)
	{
		if (queue_info[ii].opened == true)
			mq_close(queue_info[ii].queue);
		mq_unlink(queue_info[ii].queue_name);
	}
	free(queue_info);
	queue_info = NULL;
}


/* Message queue version . */
PR_queue_el *
PR_fetchQueue(void)
{
	PR_queue_el *el;
	PR_mqueue_data	mqueue_data;

#ifdef WAL_DEBUG
	PRDebug_log("****** %s() *********\n", __func__);
#endif
	for (;;)
	{
		mq_receive(queue_info[my_worker_idx].queue, (char *)&mqueue_data, sizeof(PR_mqueue_data), NULL);
#ifdef WAL_DEBUG
		PRDebug_log("*** FetchQueue: ser_no %02d-%ld, target: %d ***\n", mqueue_data.sender_worker_idx, mqueue_data.ser_no, mqueue_data.receiver_worker_idx);
#endif
		el = (PR_queue_el *)mqueue_data.data;
		if (el->data_type == RequestSync)
		{
			PR_sendSync(el->source_worker);
			PR_freeQueueElement(el);
			continue;
		}
		else if (el->data_type == RequestSyncAll)
		{
			if (my_worker_idx == PR_DISPATCHER_WORKER_IDX)
				PR_syncAll(false);
			PR_sendSync(el->source_worker);
			PR_freeQueueElement(el);
			continue;
		}
		else
			return el;
	}
}

/* Old version of PR_fetchQueue() */
#if 0
PR_queue_el *
PR_fetchQueue(void)
{
	PR_queue_el *el;


	for(;;)
	{
		lock_my_worker();
		el = my_worker->head;
		if (el)
		{
			/*
			 * Available queue.
			 */
			my_worker->head = el->next;
			if (my_worker->head == NULL)
			{
				my_worker->tail = NULL;
				my_worker->num_queued_el = 0;
			}
			else
				my_worker->num_queued_el--;

			unlock_my_worker();

			if (el->data_type == RequestSyncAll)
			{
				if (my_worker_idx == PR_DISPATCHER_WORKER_IDX)
				{
					/* Sync all the other workers, not terminating */
					PR_syncAll(false);
				}
				PR_sendSync(el->source_worker);
				PR_freeQueueElement(el);
				continue;
			}
			else if (el->data_type == RequestSync)
			{
				PR_sendSync(el->source_worker);
				PR_freeQueueElement(el);
				continue;
			}
			else
			{
#ifdef WAL_DEBUG
				PR_dump_fetchQueue(__func__, el);
#endif
				return el;
			}
		}
		else {
			my_worker->wait_dispatch = true;

			unlock_my_worker();

			PR_recvSync();
			lock_my_worker();
			my_worker->wait_dispatch = false;
			my_worker->num_queued_el = 0;	/* Just in case */
			unlock_my_worker();

			continue;

		}
	}
	return el;	/* Never reaches here */
}
#endif

#ifdef WAL_DEBUG
void
PR_dump_fetchQueue(const char *funcname, PR_queue_el *el)
{
	XLogReaderState		*state;
	XLogDispatchData_PR *dispatch_data;

	if (el == NULL)
		return;

	switch(el->data_type)
	{
		case ReaderState :
			state = (XLogReaderState *)(el->data);
			PRDebug_log("%s:(): ReaderState, ser_no: %ld.\n", funcname, state->ser_no);
			break;
		case XLogDispatchData :
			dispatch_data = (XLogDispatchData_PR *)(el->data);
			PRDebug_log("%s(): XLogDispatchData, ser_no: %ld.\n", funcname, dispatch_data->reader->ser_no);
			break;
		case InvalidPageData :
			PRDebug_log("%s(): InvalidPageData.\n", funcname);
			break;
		case RequestSync :
			PRDebug_log("%s(): RequestSync.\n", funcname);
			break;
		case RequestTerminate :
			PRDebug_log("%s(): RequestTerminate.\n", funcname);
			break;
		default:
			PRDebug_log("%s(): Unknown.\n", funcname);
	}
}
#endif

/*
 * Enqueue
 */

void
PR_enqueue(void *data, PR_QueueDataType type, int	worker_idx)
{
	PR_queue_el	*el;
	PR_worker	*target_worker;
	XLogRecPtr	 currRecPtr;
	PR_mqueue_data	mqueue_data;
	static uint64   ser_no = 0;

#ifdef WAL_DEBUG
	PRDebug_log("****** %s() *********\n", __func__);
	if (worker_idx == PR_INVALID_PAGE_WORKER_IDX)
		PR_breakpoint();
#endif

	target_worker = &pr_worker[worker_idx];

	el = getQueueElement();
	el->next = NULL;
	el->data_type = type;
	el->data = data;
	el->source_worker = my_worker_idx;
	switch(type)
	{
		case ReaderState:
			currRecPtr = ((XLogReaderState *)data)->ReadRecPtr;
			break;
		case XLogDispatchData:
			currRecPtr = ((XLogDispatchData_PR *)data)->reader->ReadRecPtr;
			break;
		default:
			currRecPtr = InvalidXLogRecPtr;
	}
	if (currRecPtr != InvalidXLogRecPtr)
	{
		lock_worker(worker_idx);
		target_worker->assignedRecPtr = currRecPtr;
		unlock_worker(worker_idx);
	}
	mqueue_data.cmd = PR_MqData;
	mqueue_data.sender_worker_idx = my_worker_idx;
	mqueue_data.data = (void *)el;
	mqueue_data.receiver_worker_idx = worker_idx;
	mqueue_data.ser_no = ser_no++;

#ifdef WAL_DEBUG
	PRDebug_log("*** Enqueue: ser_no: %02d-%ld, target: %d, queue: %s  ***\n", my_worker_idx, mqueue_data.ser_no, worker_idx, queue_info[worker_idx].queue_name);
#endif
	if (mq_send(queue_info[worker_idx].queue, (char *)&mqueue_data, sizeof(PR_mqueue_data), 0) == -1)
	{
#ifdef WAL_DEBUG
		PRDebug_log("mq_send() failed. %s.\n", strerror(errno));
		PR_error_here();
#endif
		elog(ERROR, "Failed to assign WAL.");
	}
	if (type == RequestSync)
		PR_recvSync();
	return;
}

#if 0
/* OLD VERSION PR_enqueue, based on only shm */

/* Flow control of the queue */

/* Max assigned element number in the queue */
static int max_elements_in_queue = 20;
/*
 * When max number is reached, number of queues to be handled by
 * the target worker before synching.
 */
static int num_elements_before_sync = 2;

void
PR_enqueue(void *data, PR_QueueDataType type, int	worker_idx)
{
	PR_queue_el	*el;
	PR_worker	*target_worker;
	XLogRecPtr	 currRecPtr;
#ifdef WAL_DEBUG
	char	workername[64];
#endif

#ifdef WAL_DEBUG
	PR_dump_enqueue(__func__, data, type, worker_idx);
#endif

	target_worker = &pr_worker[worker_idx];

	/* Check queue outstanding queue element */

	if (type != RequestSync)
	{
		PR_queue_el *sync_el;
		PR_queue_el *curr_el;
		int	ii;

		lock_worker(worker_idx);
		if (target_worker->num_queued_el >= max_elements_in_queue)
		{
			/* Insert RequestSync element in the middle of the queue */
			sync_el = getQueueElement();
			sync_el->next = NULL;
			sync_el->data_type = RequestSync;
			sync_el->data = NULL;
			sync_el->source_worker = my_worker_idx;
			lock_worker(worker_idx);
			/* We expect num_elements_before_queue is small enough to iterate simply. */
			for (ii = 1, curr_el = target_worker->head; ii < num_elements_before_sync; ii++, curr_el = curr_el->next);
			sync_el->next = curr_el->next;
			curr_el->next = sync_el;
			target_worker->num_queued_el++;
			unlock_worker(worker_idx);
			/* Wait the target worker to reach this element */
			PR_recvSync();
		}
		else
			unlock_worker(worker_idx);
	}

	/* Now enqueue */

	el = getQueueElement();
	el->next = NULL;
	el->data_type = type;
	el->data = data;
	el->source_worker = my_worker_idx;
	switch(type)
	{
		case ReaderState:
			currRecPtr = ((XLogReaderState *)data)->ReadRecPtr;
			break;
		case XLogDispatchData:
			currRecPtr = ((XLogDispatchData_PR *)data)->reader->ReadRecPtr;
			break;
		default:
			currRecPtr = InvalidXLogRecPtr;
	}
	lock_worker(worker_idx);
	target_worker->assignedRecPtr = currRecPtr;
	if (target_worker->head == NULL)
		target_worker->head = target_worker->tail = el;
	else
	{
		target_worker->tail->next = el;
		target_worker->tail = el;
	}
	target_worker->num_queued_el++;
	if (target_worker->wait_dispatch == true)
	{
		unlock_worker(worker_idx);
#ifdef WAL_DEBUG
		PRDebug_log("%s queue is empty and it is waiting. Sending sync.\n", PR_worker_name(worker_idx, workername));
#endif
		PR_sendSync(worker_idx);
#ifdef WAL_DEBUG
		PRDebug_log("Sync sent.\n");
#endif
	}
	else
		unlock_worker(worker_idx);
	if (type == RequestSync)
		PR_recvSync();
	return;
}
#endif

#ifdef WAL_DEBUG
void
PR_dump_enqueue(const char *funcname, void *data, PR_QueueDataType type, int	worker_idx)
{
	XLogReaderState		*state;
	XLogDispatchData_PR *dispatch_data;

	switch(type)
	{
		case ReaderState :
			state = (XLogReaderState *)data;
			PRDebug_log("%s:(): ReaderState, ser_no: %ld.\n", funcname, state->ser_no);
			break;
		case XLogDispatchData :
			dispatch_data = (XLogDispatchData_PR *)data;
			PRDebug_log("%s(): XLogDispatchData, ser_no: %ld.\n", funcname, dispatch_data->reader->ser_no);
			break;
		case InvalidPageData :
			PRDebug_log("%s(): InvalidPageData.\n", funcname);
			break;
		case RequestSync :
			PRDebug_log("%s(): RequestSync.\n", funcname);
			break;
		case RequestTerminate :
			PRDebug_log("%s(): RequestTerminate.\n", funcname);
			break;
		default:
			PRDebug_log("%s(): Unknown.\n", funcname);
	}
}

void
PR_dump_queue(bool need_lock)
{
	static 	StringInfo s = NULL;
	int		ii;

#if 1
	return;
#endif

	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);

	appendStringInfoString(s, "******** Dump queue **************\n");
	for (ii = 0; ii < num_preplay_workers; ii++)
		dump_worker_queue(s, ii, need_lock);
	appendStringInfoString(s, "******** End Dump queue **************\n");
	PRDebug_out(s);
}

static void
dump_worker_queue(StringInfo s, int worker_id, bool need_lock)
{
	PR_queue_el	*curr;

	appendStringInfo(s, "---- Dump Queue for worker: %d ------\n", worker_id);
	if (need_lock)
		lock_worker(worker_id);
	curr = pr_worker[worker_id].head;
	for(; curr; curr = curr->next)
		appendStringInfo(s, "queue %p, source: %d, type: %s\n", curr, curr->source_worker, queueTypeName(curr->data_type));
	appendStringInfo(s,  "---- End Dump Queue for worker: %d ------\n", worker_id);
	if (need_lock)
		unlock_worker(worker_id);
}

static char *
queueTypeName(PR_QueueDataType type)
{
	switch(type)
	{
		case PR_QueueInit :
			return "INIT";
		case ReaderState :
			return "ReaderState";
		case XLogDispatchData :
			return "XLogDispatchData";
		case InvalidPageData :
			return "InvalidPageData";
		case RequestSync :
			return "RequestSync";
		case RequestTerminate :
			return "RequestTerminate";
		case PR_QueueMAX_value :
			return "MAX_value";
		default:
			return "Unknown";
	}
	return "Unknown";
}
#endif

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

	dispatch_data = (XLogDispatchData_PR *)PR_allocBuffer(DispatchDataSize, true);
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
	dispatch_data->xid = XLogRecGetXid(reader);

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
void
PR_dump_buffer(const char *funcname, bool need_lock)
{
	static StringInfo	s = NULL;

#if 1
	return;
#endif

	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);

	dump_buffer(funcname, s, need_lock);
	PRDebug_out(s);
}

static bool
dump_buffer(const char *funcname, StringInfo outs, bool need_lock)
{
	static StringInfo	s;
	static StringInfo	ss = NULL;
	int	ii;
	PR_BufChunk	*curr_chunk;
	bool	rv = true;

#if 1
	return true;
#endif

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
		lock_buffer();
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
			break;
		}
	}
	if (need_lock)
		unlock_buffer();
	appendStringInfoString(s, "------<< Chunk List End >>---------------\n");
	appendStringInfoString(s, "===<< Buffer dump end >>=================\n\n");
	if (outs == NULL)
		PRDebug_out(s);
	return rv;
}

void
PR_dump_chunk(void *buf, const char *funcname, bool need_lock)
{
	static StringInfo s = NULL;
	PR_BufChunk	*chunk;

#if 1
	return;
#endif
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
	if (need_lock)
		lock_buffer();
	if (!PR_isInBuffer(buf))
	{
		appendStringInfo(s, "%s(): address %p is not in parallel replay shared buffer.\n", funcname, buf);
		goto returning;
	}
	chunk = Chunk(buf);
	if ((chunk->magic != PR_BufChunk_Allocated) && (chunk->magic != PR_BufChunk_Free))
	{
		appendStringInfo(s, "%s(): address %p is not the beginning of buffer.\n", funcname, buf);
		goto returning;
	}
	dump_chunk(chunk, funcname, s, false);
returning:
	if (need_lock)
		unlock_buffer();
	PRDebug_out(s);
	return;
}

static void
dump_chunk(PR_BufChunk *chunk, const char *funcname, StringInfo outs, bool need_lock)
{
	StringInfo	s;
	static StringInfo	ss = NULL;
	Size	*size_at_tail;

#if 1
	return;
#endif
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
		lock_buffer();

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
		unlock_buffer();
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

#define value_boundary(s, b)	((((s) + (b) - 1)/(b)) * (b))
#define normalized_size(size)	value_boundary(size, MemBoundary)

static void *
PR_allocBuffer_int(Size sz, bool need_lock, bool retry_opt)
{
	PR_BufChunk	*new_chunk;
	Size		 chunk_sz;
	void		*rv;
#if 0
#ifdef WAL_DEBUG
	static StringInfo	 s = NULL;
#endif
#endif


#if 0
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
#endif

	if (need_lock)
		lock_buffer();

#ifdef WAL_DEBUG
	pr_buffer->update_sno++;
#endif
	chunk_sz = normalized_size(sz) + CHUNK_OVERHEAD;

	new_chunk = alloc_chunk(chunk_sz);
	if (need_lock)
		unlock_buffer();
	if (new_chunk)
	{
		/* Successful to allocate buffer */
#if 0
#ifdef WAL_DEBUG
		dump_chunk(new_chunk, __func__, s, need_lock);
#endif
#endif
		return &new_chunk->data[0];
	}

	/* Now no free chunk was available */

	if (!retry_opt)
		return NULL;

	/* New we need to retry */

	rv = retry_allocBuffer(sz, need_lock);

#if 0
#ifdef WAL_DEBUG
	new_chunk = Chunk(rv);
	dump_chunk(new_chunk, __func__, s, need_lock);
#endif
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
#endif

	Assert(sz > 0);

#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
	dump_buffer(__func__, s, need_lock);
	PRDebug_out(s);
	resetStringInfo(s);
#endif
	if (need_lock)
		lock_buffer();
	pr_buffer->needed_by_worker[my_worker_idx] = sz;
	if (need_lock)
		unlock_buffer();

	PR_recvSync();

	if (need_lock)
		lock_buffer();
	rv = pr_buffer->allocated_buffer[my_worker_idx];
	pr_buffer->allocated_buffer[my_worker_idx] = NULL;
	if (need_lock)
		unlock_buffer();
	return rv;
}

/*
 * Allocate chunk in the free area.  pr_buffer->slock has to be acquired by the caller.
 */
#if 0
static PR_BufChunk *
alloc_chunk(Size chunk_sz)
{
	PR_BufChunk	*free_chunk;	/* Free chunk to allocate new chunk */
	PR_BufChunk	*new_free_chunk;
	PR_BufChunk *new_chunk;
	Size		 new_free_chunk_size;
	Size		*size_at_tail_free;
	Size		*size_at_tail_new;

	/* Arrange the size to boundary */
	chunk_sz = size_boundary(chunk_sz);

	free_chunk = (PR_BufChunk *)pr_buffer->alloc_start;

	Assert(free_chunk->magic == PR_BufChunk_Free);

	if (pr_buffer->alloc_start == pr_buffer->alloc_end)
		/* No more area to allocate */
		return NULL;

	if (free_chunk->size >= chunk_sz)
	{
		/* Can allocate chunk from this free chunk */
		if (free_chunk->size > (chunk_sz + CHUNK_OVERHEAD))
		{
			/* Next start will begin after this new chunk */
			/* Next block is almost identical to the following one. */
			/* Ned improvement for more portable code */
			new_free_chunk_size = free_chunk->size - chunk_sz;

			new_chunk = free_chunk;
			new_free_chunk = addr_forward(free_chunk, chunk_sz);

			new_chunk->size = chunk_sz;
			new_chunk->magic = PR_BufChunk_Allocated;
			new_chunk->sno = pr_buffer->curr_sno++;
			size_at_tail_new = SizeAtTail(new_chunk);
			*size_at_tail_new = chunk_sz;

			new_free_chunk->size = new_free_chunk_size;
			new_free_chunk->magic = PR_BufChunk_Free;
			size_at_tail_free = SizeAtTail(new_free_chunk);
			*size_at_tail_free = new_free_chunk_size;

			pr_buffer->alloc_start = new_chunk;
			return new_chunk;
		}
		else
		{
			/* Whole current free chunk has to be new chunk */
			free_chunk->magic = PR_BufChunk_Allocated;
			pr_buffer->alloc_start = next_chunk(free_chunk);
			if (pr_buffer->alloc_start == pr_buffer->tail)
			{
				pr_buffer->alloc_start = pr_buffer->head;
				if (pr_buffer->alloc_end == pr_buffer->tail)
					pr_buffer->alloc_end = pr_buffer->head;
			}
			return free_chunk;
		}
	}
	else if (addr_before(pr_buffer->alloc_end, pr_buffer->alloc_start))
	{
		/* We need to allocate the chunk from next free chunk, at the beginning */
		free_chunk = pr_buffer->head;

		if (free_chunk->magic != PR_BufChunk_Free)
			/* No more area to allocate */
			return NULL;
		if (free_chunk->size < chunk_sz)
			return NULL;
		if (free_chunk->size > (chunk_sz + CHUNK_OVERHEAD))
		{
			/* Next start will begin after this new chunk */
			/* Next block is almost identical to the above one. */
			/* Ned improvement for more portable code */
			new_free_chunk_size = free_chunk->size - chunk_sz;

			new_chunk = free_chunk;
			new_free_chunk = addr_forward(free_chunk, chunk_sz);

			new_chunk->size = chunk_sz;
			new_chunk->magic = PR_BufChunk_Allocated;
			new_chunk->sno = pr_buffer->curr_sno++;
			size_at_tail_new = SizeAtTail(new_chunk);
			*size_at_tail_new = chunk_sz;

			new_free_chunk->size = new_free_chunk_size;
			new_free_chunk->magic = PR_BufChunk_Free;
			size_at_tail_free = SizeAtTail(new_free_chunk);
			*size_at_tail_free = new_free_chunk_size;

			pr_buffer->alloc_start = new_free_chunk;
			return new_chunk;
		}
		else
		{
			/* Whole current free chunk has to be new chunk */
			free_chunk->magic = PR_BufChunk_Allocated;
			pr_buffer->alloc_start = next_chunk(free_chunk);
			if (pr_buffer->alloc_start == pr_buffer->tail)
			{
				pr_buffer->alloc_start = pr_buffer->head;
				if (pr_buffer->alloc_end == pr_buffer->tail)
					pr_buffer->alloc_end = pr_buffer->head;
			}
			return free_chunk;
		}
	}

	return NULL;
}
/* Old version */
#else
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
			else
				new_chunk->sno = pr_buffer->curr_sno++;
			return new_chunk;
		}
	}
	return NULL;	/* Does not reach here */
}
#endif

#ifdef WAL_DEBUG
/*
 * Check if the given address is the valid address of shared buffer.
 * Address information is written to StringInfo.
 *
 * Return true if the address is valid.   False otherwise.
 */
bool
PR_bufferCheck(StringInfo s, void *addr, bool need_lock)
{
	PR_BufChunk *chunk;

	chunk = find_chunk(s, addr, need_lock);
	return(chunk == NULL ? false : true);
}

/*
 * Returns the chunk the addr is included.   Note that the address may not be the first
 * address of the buffer.
 * If the address is pointing chunk header or size_at_tail, NULL will be returned too.
 * If out-of-bounds, NULL will be returned.
 */
static PR_BufChunk *
find_chunk(StringInfo s, void *addr, bool need_lock)
{
	PR_BufChunk	*curr;
	PR_BufChunk	*next;
	void		*buff_addr;

	if (addr_before(addr, pr_buffer->head) || addr_after_eq(addr, pr_buffer->tail))
		return NULL;

	if (need_lock)
		lock_buffer();

	for (curr = (PR_BufChunk *)pr_buffer->head, next = (PR_BufChunk *)addr_forward(curr, curr->size);
			addr_after(addr, next);
			curr = next, next = (PR_BufChunk *)addr_forward(curr, curr->size));
	buff_addr = &curr->data[0];
	if (buff_addr == addr)
	{
		/* The address is correst.  At the head of the buffer */
		appendStringInfo(s, "Address %p is correct buffer.   Chunk: %p, ser_no: %ld, size: %ld, magic: %s\n",
				addr, curr, curr->sno, curr->size,
				curr->magic == PR_BufChunk_Allocated ? "Allocated" :  (curr->magic == PR_BufChunk_Free ? "Free" : "Unknown"));
		if (need_lock)
			unlock_buffer();
		return curr;
	}
	else
	{
		if (addr_before(addr, &curr->data[0]) || addr_after_eq(addr, SizeAtTail(curr)))
			appendStringInfo(s, "Address %p is in the Chunk %p but in the control area. Chunk ser_no: %ld, size: %ld, magic: %s\n",
					addr, curr, curr->sno, curr->size,
					curr->magic == PR_BufChunk_Allocated ? "Allocated" :  (curr->magic == PR_BufChunk_Free ? "Free" : "Unknown"));
		else
			appendStringInfo(s, "Address %p is in the Chunk %p but not at the beginning. Chunk ser_no: %ld, size: %ld, magic: %s\n",
					addr, curr, curr->sno, curr->size,
					curr->magic == PR_BufChunk_Allocated ? "Allocated" :  (curr->magic == PR_BufChunk_Free ? "Free" : "Unknown"));
		if (need_lock)
			unlock_buffer();
		return curr;
	}
	if (need_lock)
		unlock_buffer();
	return NULL;	/* Never comes here. */
}
#endif


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
#if 0
#ifdef WAL_DEBUG
	static StringInfo	s = NULL;
#endif
#endif


	/*
	 * Initialize sync target structure if needed.
	 */
	if (sync_alloc_target == NULL)
		sync_alloc_target = (bool *)palloc(sizeof(bool) * num_preplay_workers);

	/* Do not forget to ping reader or dispatcher if there's free area available */
	/* Ping the reader worker only when dispatcher does not require the buffer */

#if 0
#ifdef WAL_DEBUG
	if (s == NULL)
		s = makeStringInfo();
	else
		resetStringInfo(s);
	appendStringInfo(s, "--- %s: freeing: %p ---\n", __func__, buffer);
	dump_chunk(Chunk(buffer), __func__, s, need_lock);
	dump_buffer(__func__, s, need_lock);
	PRDebug_out(s);
	resetStringInfo(s);

#endif
#endif
	if (addr_before(buffer, pr_buffer->head) || addr_after(buffer, pr_buffer->tail))
		return;

	if (need_lock)
		lock_buffer();
#ifdef WAL_DEBUG
	pr_buffer->update_sno++;
#endif
	chunk = Chunk(buffer);
	if (chunk->magic != PR_BufChunk_Allocated)
	{
		if (need_lock)
			unlock_buffer();
#ifdef WAL_DEBUG
		PRDebug_log("Attempt to free wrong buffer. Addr: 0x%p.\n", buffer);
#endif
		elog(LOG, "Attempt to free wrong buffer.");
		return;
	}
#if 0
#ifdef WAL_DEBUG
	resetStringInfo(s);
#endif
#endif

	free_chunk(chunk);

#if 0
#ifdef WAL_DEBUG
	dump_buffer(__func__, s, false);
	PRDebug_out(s);
#endif
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
		unlock_buffer();
	/*
	 * Now all the relevant spin locks were released.  Safe to send sync.
	 */
	for (ii = num_preplay_workers - 1; ii >= PR_READER_WORKER_IDX; ii--)
	{
		if (sync_alloc_target[ii] == true)
			PR_sendSync(ii);
	}

#if 0
#ifdef WAL_DEBUG
	dump_buffer(__func__, s, need_lock);
	PRDebug_out(s);
#endif
#endif
	return;

}

/* The caller should acquire the lock pr_buffer->slock */
static void
free_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk	*new_chunk;
	PR_BufChunk *head_chunk;

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
		goto returning;
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
		goto returning;

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
returning:
	head_chunk = (PR_BufChunk *)(pr_buffer->head);

	if ((pr_buffer->alloc_end == pr_buffer->tail) && (head_chunk->magic == PR_BufChunk_Free))
		pr_buffer->alloc_end = next_chunk(head_chunk);

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

	pr_buffer->area_size = buffer_size()
		- pr_sizeof(PR_buffer)
		- (pr_sizeof(Size) * num_preplay_workers)
		- (pr_sizeof(void *) * num_preplay_workers);
	pr_buffer->needed_by_worker = (Size *)addr_forward(pr_buffer, pr_sizeof(PR_buffer));
	pr_buffer->curr_sno = 0;

	needed_by_worker = &pr_buffer->needed_by_worker[0];
	for(ii = 0; ii < num_preplay_workers; ii++)
		needed_by_worker[ii] = 0;

	pr_buffer->allocated_buffer = (void **)addr_forward(pr_buffer->needed_by_worker, pr_sizeof(Size) * num_preplay_workers);
	for (ii = 0; ii < num_preplay_workers; ii++)
		pr_buffer->allocated_buffer[ii] = NULL;

	pr_buffer->head = addr_forward(pr_buffer->allocated_buffer, (pr_sizeof(void *) * num_preplay_workers));
	pr_buffer->tail = addr_forward(pr_buffer, buffer_size());
	pr_buffer->alloc_start = pr_buffer->head;
	pr_buffer->alloc_end = pr_buffer->tail;
	chunk = (PR_BufChunk *)pr_buffer->alloc_start;
	chunk->size = addr_difference(pr_buffer->tail, pr_buffer->head);
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
			    timeofday->tm_year + 1900, timeofday->tm_mon + 1, timeofday->tm_mday,
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

void
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
	pr_debug_log_hdr = PR_worker_name(my_worker_idx, NULL);
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
	PR_queue_el				*el;
	XLogInvalidPageData_PR	*page;
	bool 					 invalidPageFound;
	int						 source_worker;

#ifdef WAL_DEBUG
	PRDebug_log("Started %s()\n", __func__);
#endif /* WAL_DEBUG */

	lock_my_worker();
	my_worker->handledRecPtr = 0;
	unlock_my_worker();
	
	for (;;)
	{
#ifdef WAL_DEBUG
		PRDebug_log("Fetching queue\n");
#endif /* WAL_DEBUG */
		el = PR_fetchQueue();
		if (el->data_type == RequestTerminate)
		{
			lock_my_worker();
			my_worker->worker_terminated = true;
			unlock_my_worker();

			PR_sendSync(el->source_worker);
			PR_freeBuffer(page, true);

			break;	/* Process termination request */
		}
		if (el->data_type != InvalidPageData)
		{
			PR_failing();
			elog(PANIC, "Invalid internal status for invalid page worker.");
		}
		page = (XLogInvalidPageData_PR *)(el->data);
		source_worker = el->source_worker;
		PR_freeQueueElement(el);
#ifdef WAL_DEBUG
		PRDebug_log("Fetched\n");
		dump_invalidPageData(page);
#endif	/* WAL_DEBUG */
		PR_freeBuffer(page, true);
		PR_sendSync(source_worker);
#ifdef PR_IGNORE_REPLAY_ERROR
		continue;
#endif /* PR_IGNORE_REPLAY_ERROR */
		switch(page->cmd)
		{
			case PR_LOG:
				PR_log_invalid_page_int(page->node, page->forkno, page->blkno, page->present);
				break;
			case PR_FORGET_PAGES:
#ifdef WAL_DEBUG
				PRDebug_log("Received \"FORGET PAGES\"\n");
				PR_breakpoint();
#endif
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
			lock_invalid_page();
			pr_invalidPages->invalidPageFound = invalidPageFound;
			unlock_invalid_page();
		}
		PR_sendSync(source_worker);
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
	int		 n_remaining;
#ifdef WAL_DEBUG
	int		 n_involved;
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
		XLogRecPtr		currRecPtr;

#if 0
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
		if (el->data_type == RequestTerminate)
		{
			lock_my_worker();
			my_worker->worker_terminated = true;
			unlock_my_worker();

			PR_sendSync(el->source_worker);
			PR_freeQueueElement(el);

			break;	/* Process termination request */
		}
		else if (el->data_type != XLogDispatchData)
		{
			PR_failing();
			elog(PANIC, "Invalid internal status for block worker.");
		}
		data = (XLogDispatchData_PR *)(el->data);
		PR_freeQueueElement(el);

		lock_dispatch_data(data);

		data->n_remaining--;
		n_remaining = data->n_remaining;
		currRecPtr = data->reader->ReadRecPtr;

#ifdef WAL_DEBUG
		n_involved = data->n_involved;
		PRDebug_log("Fetched: ser_no: %ld, xlogrecord: \"%s\", n_involved: %d, n_remaining: %d\n",
				data->reader->ser_no, data->reader->xlog_string, n_involved, n_remaining);
#if 0
		if (n_involved > 1)
			PR_breakpoint();
#endif
#endif
		unlock_dispatch_data(data);

		if (n_remaining > 0)
		{
			/* This worker is not eligible to handle this */
			/* Wait another BLOCK worker to handle this and sync to me */
			updateTxnInfoAfterReplay(DispatchDataGetXid(data), DispatchDataGetLSN(data), true);
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

			/* Dequeue */

			/* OK. I should handle this. Nobody is handling this and safe to release the lock. */

			/* REDO */

			PR_redo(data);

			/* Koichi: TBD: ここで、XLogHistory と txn history の更新を行うこと */
			
			if ((data->reader->record->xl_info & XLR_CHECK_CONSISTENCY) != 0)
				checkXLogConsistency(data->reader);

			/*
			 * Update lastReplayedEndRecPtr after this record has been
			 * successfully replayed.
			 */
			PR_setXLogReplayed(data->xlog_history_el);

			/*
			 * Update transaction information
			 */
			updateTxnInfoAfterReplay(DispatchDataGetXid(data), DispatchDataGetLSN(data), true);

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
#ifdef WAL_DEBUG
				{
					PRDebug_log("Now synch other assigned %s.\n", PR_worker_name(*worker_list, workername));
#endif
					PR_sendSync(*worker_list);
#ifdef WAL_DEBUG
					PRDebug_log("Sync to %s done.\n", PR_worker_name(*worker_list, workername));
				}
#endif
			}

			/* Following steps */
			if (doRequestWalReceiverReply)
			{
				doRequestWalReceiverReply = false;
				WalRcvForceReply();
			}

			freeDispatchData(data, true);
		}
		lock_my_worker();
		my_worker->handledRecPtr = currRecPtr;
		unlock_my_worker();
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

#if 0
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
		if (el->data_type == RequestTerminate)
		{
			lock_my_worker();
			my_worker->worker_terminated = true;
			unlock_my_worker();

			PR_sendSync(el->source_worker);
			PR_freeQueueElement(el);

			break;	/* Process termination request */
		}
		else if (el->data_type != XLogDispatchData)
		{
#ifdef WAL_DEBUG
			PR_error_here();		/* GDB breakpoint */
#endif
			PR_failing();
			elog(PANIC, "Invalid internal status for transaction worker.");
		}
		dispatch_data = (XLogDispatchData_PR *)(el->data);
#ifdef WAL_DEBUG
		PRDebug_log("Fetched: ser_no: %ld, xlogrecord: \"%s\"\n", dispatch_data->reader->ser_no, dispatch_data->reader->xlog_string);
#endif
		PR_freeQueueElement(el);
		xlogreader = DispatchDataGetReader(dispatch_data);
		record = DispatchDataGetRecord(dispatch_data);
		currRecPtr = DispatchDataGetLSN(dispatch_data);

		syncTxn(dispatch_data);
		
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
		lock_my_worker();
		my_worker->handledRecPtr = currRecPtr;
		unlock_my_worker();
		
		freeDispatchData(dispatch_data, true);
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

/*
 * Check if dispatch data terminates the transaction.
 * If so, then wait until all the dispatched data of this transaction to
 * block workers are replayed.
 *
 * Otherwise, do nothing.
 *
 * If just XID was assgined but no real update were done, no txn_cell might have been
 * created.  In such case, we don't have to do anything either.
 */
static void
syncTxn(XLogDispatchData_PR *dispatch_data)
{
	RmgrId		 rmgr_id;
	uint8		 xact_info;
	bool		 created;
	PR_txn_cell	*txn_cell;

	Assert(my_worker_idx == PR_TXN_WORKER_IDX);

	getXLogRecordRmgrInfo(dispatch_data->reader, &rmgr_id, &xact_info);

	if (!checkRmgrTxnSync(rmgr_id, xact_info))
		/* No need to sync BLK workers */
		return;

	txn_cell = find_txn_cell(dispatch_data->xid, false, true, &created);

	if (txn_cell == NULL)
		return;

	syncTxn_blockWorker(txn_cell);		/* Wait untill all the relevant WAL records have been replayed. */


	free_txn_cell(txn_cell, true);			/* Release txn_cell */
}


/*
 * Assumes no other workers are reading/writing txn_cell.   It is reasonable assumption
 * because this function is called from TXN worker before COMMIT/ABORT/PREPARE WAL record
 * is replayed.    All the WAL records for this transaction should have been dispatched
 * by DISPATCHER worker and there should be no further update to the txn_cell.
 */
static void
syncTxn_blockWorker(PR_txn_cell *txn_cell)
{
	TransactionId		 xid;
	int	ii;
#ifdef WAL_DEBUG
	char	workername[64];
#endif

#ifdef WAL_DEBUG
	PRDebug_log("%s(): Beginning.  txn_cell=%p, xid=%d.\n", __func__, txn_cell, txn_cell->xid);
#endif
	Assert(my_worker_idx == PR_TXN_WORKER_IDX);
	Assert(txn_cell);

	xid = txn_cell->xid;

	for (ii = 0; ii < (num_preplay_workers - PR_BLK_WORKER_MIN_IDX); ii++)
	{
		/* We need to lock/unlock txn_hash in each iteration so that block workers can handle txn_cell too. */
		lock_txn_hash(xid);

		if (txn_cell->lastXLogLSN[ii] != InvalidXLogRecPtr)
		{
#ifdef WAL_DEBUG
			PRDebug_log("Waiting for BLK WORKER(%d) (worker_id %d) to replay LSN:%016lx.\n",
					ii, (ii + PR_BLK_WORKER_MIN_IDX), txn_cell->lastXLogLSN[ii]);
			/* Koichi: 以下、トランザクションの終わりのチェックのため */
#endif
			txn_cell->blk_worker_to_wait = ii + PR_BLK_WORKER_MIN_IDX;
			unlock_txn_hash(xid);
			PR_recvSync();
#ifdef WAL_DEBUG
			PRDebug_log("Synch done from %s\n", PR_worker_name(ii + PR_BLK_WORKER_MIN_IDX, workername));
#endif
		}
		else
			unlock_txn_hash(xid);
	}
#ifdef WAL_DEBUG
	PRDebug_log("%s(): Done. txn_cell=%p, xid=%d.\n", __func__, txn_cell, txn_cell->xid);
#endif
}

/*
 *************************************************************************************************************
 * Dispatcher worker
 *************************************************************************************************************
 */

long PR_handled_wal_records = 0;

static int
dispatcherWorkerLoop(void)
{
	PR_queue_el *el;
	XLogReaderState *reader;
	XLogRecord *record;
	XLogDispatchData_PR	*dispatch_data;
	int	*worker_list;

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
		if (el->data_type == RequestTerminate)
		{
			/* Wait all other workers to terminate */

			PR_syncAll(true);

			/* Terminate myself */
			lock_my_worker();
			my_worker->worker_terminated = true;
			unlock_my_worker();

			PR_sendSync(el->source_worker);
			PR_freeQueueElement(el);

			break;	/* Process termination request */
		}
		else if (el->data_type != ReaderState)
		{
#ifdef WAL_DEBUG
			PR_error_here();
#endif
			PR_failing();
			elog(PANIC, "Invalid internal status for dispatcher worker.");
		}
		reader = (XLogReaderState *)el->data;
		currRecPtr = reader->ReadRecPtr;
#ifdef WAL_DEBUG
		PRDebug_log("Fetched.  Ser_no(%ld), xlog: \"%s\"\n", reader->ser_no, reader->xlog_string);
#endif
		PR_freeQueueElement(el);
		record = reader->decoded_record;
		dispatch_data = PR_analyzeXLogReaderState(reader, record);

#ifdef WAL_DEBUG
#if 0
		if (dispatch_data->n_involved > 1)
			PR_breakpoint();
#endif
#endif
		if (isSyncBeforeDispatchNeeded(reader))
			PR_syncAll(false);

		dispatchDataToXLogHistory(dispatch_data);
		addDispatchDataToTxn(dispatch_data, true);

		sync_needed = isSyncAfterDispatchNeeded(reader) ?  true : false;
		for (worker_list = dispatch_data->worker_list; *worker_list > PR_DISPATCHER_WORKER_IDX; worker_list++)
		{
#ifdef WAL_DEBUG
			if (*worker_list == PR_TXN_WORKER_IDX)
			{
				PRDebug_log("Dispatching to TXN worker.\n");
				PR_breakpoint();
			}
#endif
			PR_enqueue(dispatch_data, XLogDispatchData, *worker_list);
		}
		if (sync_needed)
			PR_syncAll(false);

		lock_my_worker();
		my_worker->handledRecPtr = currRecPtr;
		unlock_my_worker();
		PR_handled_wal_records++;
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
#ifdef WAL_DEBUG
	XLogReaderState		*reader;
#endif
	PR_XLogHistory_el	*el = NULL;

#ifdef WAL_DEBUG
	reader = dispatch_data->reader;
	el = PR_addXLogHistory(reader->ReadRecPtr, reader->EndRecPtr, reader->timeline, reader->ser_no, DispatchDataGetXid(dispatch_data));
#endif
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
