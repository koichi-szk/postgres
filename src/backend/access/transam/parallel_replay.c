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
 ************************************************************************************************
 * Parallel replay shared memory
 ************************************************************************************************
 */

static dsm_segment	*pr_shm_seg = NULL;

/* The following variables are initialized by PR_initShm() */
PR_shm   	*pr_shm = NULL;
static txn_wal_info_PR	*pr_txn_wal_info = NULL;
static txn_hash_el_PR	*pr_txn_hash = NULL;
static txn_cell_PR		*pr_txn_cell_pool = NULL;
static PR_invalidPages 	*pr_invalidPages = NULL;
static PR_XLogHistory	*pr_history = NULL;
static PR_worker	*pr_worker = NULL;
static PR_queue		*pr_queue = NULL;
static PR_buffer	*pr_buffer = NULL;

/* My worker process info */
static int           my_worker_idx = 0;         /* Index of the worker.  Set when the worker starts. */
static PR_worker    *my_worker = NULL;      /* My worker structure */

/* For test code */
static FILE *pr_debug_log = NULL;
static char	*pr_debug_signal_file = NULL;
static char	*debug_log_dir = NULL;
static char	*debug_log_file_path = NULL;
static char	*debug_log_file_link = NULL;
static int	 my_worker_idx;

/* Module-global definition */
#define PR_MAXPATHLEN	512



/*
 * Size boundary, address calculation and boundary adjustment
 */

#define MemBoundary         (sizeof(void *))
#define addr_forward(p, s)  ((void *)((uint64)(p) + (Size)(s)))
#define addr_backward(p, s) ((void *)((uint64)(p) - (Size)(s)))
#if 0
#define addr_subtract(a, b) ((Size)((b) - (a)))
#endif
#define addr_after(a, b)    ((uint64)(a) > (uint64)(b))
#define addr_after_eq(a, b) ((uint64)(a) >= (uint64)(b))
#define addr_before(a, b)   ((uint64)(a) < (uint64)(b))
#define addr_before_eq(a, b)    ((uint64)(a) <= (uint64)(b))

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
/* Macros */
#define PR_SOCKNAME_LEN 127
#define PR_SYNCSOCKDIR  "pr_syncsock"
#define PR_SYNCSOCKFMT  "%s/%s/pr_%06d"
#define PR_SYNC_MSG_SZ 15
#define PR_MB	(2 << 19)

/* When O0 is ised, it conflicts with inline function. */
#define INLINE static


/*
 ***********************************************************************************************
 * Internal Variables
 ***********************************************************************************************
 */

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
static void set_syncFlag(int worker_id, uint32 flag_to_set);
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
static PR_BufChunk	*prev_chunk(PR_BufChunk *chunk);
static void			 free_chunk(PR_BufChunk *chunk);
static PR_BufChunk	*alloc_chunk(Size sz, void *start, void *end);
static void			 concat_next_chunk(PR_BufChunk *chunk);
static void			 concat_prev_chunk(PR_BufChunk *chunk);
static void			 chunk_arrange_free_wraparound(void);
static void			*retry_allocBuffer(Size sz, bool need_lock);
INLINE PR_BufChunk	*next_chunk(PR_BufChunk *chunk);
static Size			 available_size(PR_buffer *buffer);
static bool			 isOtherWorkersRunning(void);

/*
 * Dispatch Data function
 */
static void	freeDispatchData(XLogDispatchData_PR *dispatch_data);
static bool checkRmgrTxnSync(RmgrId rmgrid, uint8 info);
static bool checkSyncBeforeDispatch(RmgrId rmgrid, uint8 info);
static bool isSyncBeforeDispatchNeeded(XLogReaderState *reader);

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
#if 0
static bool			 removeTxnCell(txn_cell_PR *txn_cell);
#endif
static txn_cell_PR	*isTxnSyncNeeded(XLogDispatchData_PR *dispatch_data, XLogRecord *record, TransactionId *xid, bool remove_myself);
static void			 syncTxn(txn_cell_PR *txn_cell);
static void			 dispatchDataToXLogHistory(XLogDispatchData_PR *dispatch_data);
#define get_txn_hash(x) &pr_txn_hash[txn_hash_value(x)]

/* XLogReaderState/XLogRecord functions */
static int	blockHash(int spc, int db, int rel, int blk, int n_max);
INLINE int	fold_int2int8(int val);
static void getXLogRecordRmgrInfo(XLogReaderState *reader, RmgrId *rmgrid, uint8 *info);


/* Workerr Loop */
static void dispatcherWorkerLoop(void);
static void txnWorkerLoop(void);
static void invalidPageWorkerLoop(void);
static void blockWorkerLoop(void);

/* Test code */
#ifdef WAL_DEBUG
static void PRDebug_out(StringInfo s);
static void dump_buffer(const char *funcname, bool need_lock);
static void dump_chunk(PR_BufChunk *chunk, const char *funcname, bool need_lock);
#endif

/*
 ************************************************************************************************
 * Synchronization Functions: synchronizing among worker processes
 ************************************************************************************************
 */

/*
 * Initialize socket directory
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
			elog(PANIC, "Failed to stat PR debug directory, %s", strerror(my_errno));
        rv = mkdir(sync_sock_dir, S_IRUSR|S_IWUSR|S_IXUSR);
        local_errno = errno;
        if (rv != 0)
            elog(PANIC, "Failed to create PR debug directory, %s", strerror(local_errno));
    }
    else
	{
		/* Debug directory stat successfful */
		if ((statbuf.st_mode & S_IFDIR) == 0)
			/* It was not a directory */
			elog(PANIC, "%s must be a directory but not.", sync_sock_dir);
	}
	/* Remove all the sockets */
	rmtree(sync_sock_dir, false);
}

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
			ereport(FATAL,
					(errcode_for_socket_access(),
					 errmsg("Could not create the socket: %m")));

		if (ii == my_worker_idx)
		{
			rc = bind(sync_sock_info[ii].syncsock, &sync_sock_info[ii].sockaddr, sizeof(struct sockaddr_un));
			if (rc < 0)
				ereport(FATAL,
						(errcode_for_socket_access(),
						 errmsg("Could not bind the socket to \"%s\": %m",
							 sync_sock_info[ii].sockaddr.sun_path)));
		}
    }
    snprintf(my_worker_msg, PR_SYNC_MSG_SZ, "%04d\n", my_worker_idx);
    my_worker_msg_sz = strlen(my_worker_msg);
}

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

void
PR_sendSync(int worker_idx)
{
    Size    ll;

    Assert(pr_shm && worker_idx != my_worker_idx);

    ll = sendto(sync_sock_info[worker_idx].syncsock, my_worker_msg, my_worker_msg_sz, 0,
			&sync_sock_info[worker_idx].sockaddr, sizeof(struct sockaddr_un));
    if (ll != my_worker_msg_sz)
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Can not send sync message from worker %d to %d: %m",
					 my_worker_idx, worker_idx)));
}

int
PR_recvSync(void)
{
#define SYNC_RECV_BUF_SZ    64
    char    recv_buf[SYNC_RECV_BUF_SZ];
    Size    sz;

    Assert (pr_shm);

    sz = recv(sync_sock_info[my_worker_idx].syncsock, recv_buf, SYNC_RECV_BUF_SZ, 0);
    if (sz < 0)
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Could not receive message.  worker %d: %m", my_worker_idx)));
    return(atoi(recv_buf));
#undef SYNC_RECV_BUF_SZ
}

static void
set_syncFlag(int worker_id, uint32 flag_to_set)
{
	Assert (worker_id != my_worker_id);

	SpinLockAcquire(&pr_worker[worker_id].slock);
	pr_worker[worker_id].flags |= flag_to_set;
	if (pr_worker[worker_id].wait_dispatch)
	{
		pr_worker[worker_id].wait_dispatch = false;
		SpinLockRelease(&pr_worker[worker_id].slock);
		PR_sendSync(worker_id);
	}
	else
		SpinLockRelease(&pr_worker[worker_id].slock);
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
	int	ii;

	Assert(my_worker_idx == PR_READER_WORKER_IDX);

	for (ii = 1; ii < num_preplay_workers; ii++)
	{
		if (ii == PR_INVALID_PAGE_WORKER_IDX)
			continue;
		set_syncFlag(ii, PR_WK_SYNC_READER);
	}
	for (ii = 1; ii < (num_preplay_workers - 1); ii++)
		PR_recvSync();
	/*
	 * Finaly Invalid Block Worker
	 */
	set_syncFlag(PR_INVALID_PAGE_WORKER_IDX, PR_WK_SYNC_READER);

	PR_recvSync();
}

static void
PR_syncBeforeDispatch(void)
{
	int ii;

	Assert(my_worker_idx == PR_DISPATCHER_WORKER_IDX);

	for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers; ii++)
	{
		if (ii == PR_INVALID_PAGE_WORKER_IDX)
			continue;
		set_syncFlag(ii, PR_WK_SYNC_DISPATCHER);
	}
	for (ii = PR_TXN_WORKER_IDX; ii < (num_preplay_workers -1); ii++)
		PR_recvSync();

	set_syncFlag(PR_INVALID_PAGE_WORKER_IDX, PR_WK_SYNC_DISPATCHER);
	PR_recvSync();
}

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
			return false;
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
 ****************************************************************************
 *
 * Shared memory functions
 *
 * These function should be called from the READER WORKER.
 *
 ****************************************************************************
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


	Assert(my_worker_idx == PR_READER_WORKER);

	my_shm_size = pr_sizeof(PR_shm)
		+ (my_txn_hash_sz = pr_txn_hash_size())
		+ (my_invalidP_sz = pr_sizeof(PR_invalidPages))
		+ (my_history_sz = xlogHistorySize())
		+ (my_worker_sz = worker_size())
	   	+ (my_queue_sz = queue_size())
		+ buffer_size();

	pr_shm_seg = dsm_create(my_shm_size, 0);
	
	pr_shm = dsm_segment_address(pr_shm_seg);
	pr_shm->txn_wal_info = pr_txn_wal_info = addr_forward(pr_shm, pr_sizeof(PR_shm));
	pr_shm->invalidPages = pr_invalidPages = addr_forward(pr_txn_wal_info, my_txn_hash_sz);
	pr_shm->history = pr_history = addr_forward(pr_invalidPages, pr_sizeof(PR_invalidPages));
	pr_shm->workers = pr_worker = addr_forward(pr_history, my_history_sz);
	pr_shm->queue = pr_queue = addr_forward(pr_worker, my_worker_sz);
	pr_shm->buffer = pr_buffer = addr_forward(pr_queue, my_queue_sz);
	pr_shm->wk_EndRecPtr = InvalidXLogRecPtr;
	pr_shm->wk_TimeLineID = InvalidXLogRecPtr;

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

static txn_cell_PR *
get_txn_cell(bool need_lock)
{
	txn_cell_PR	*rv;

	if (need_lock)
		SpinLockAcquire(&pr_txn_wal_info->cell_slock);
	rv = pr_txn_cell_pool->next;
	if (rv == NULL)
		pr_txn_cell_pool->next = NULL;
	else
		pr_txn_cell_pool->next = rv->next;
	rv->next = NULL;
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

#ifdef WAL_DEBUG
	if (dispatch_data->reader->xlog_string)
		PR_freeBuffer(dispatch_data->reader->xlog_string, true);
#endif
	if (dispatch_data->reader)
	{
		if (dispatch_data->reader->record)
			PR_freeBuffer(dispatch_data->reader->record, true);
		PR_freeBuffer(dispatch_data->reader, true);
	}
	PR_freeBuffer(dispatch_data, true);
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
		Assert(dispatchh_data == txn_cell->tail);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(invalidData), true);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(invalidData), true);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(invalidData), true);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(pr_sizeof(invalidData), true);
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
	pr_history->hist_head = &el[0];
	pr_history->hist_end = &el[0];
	for (ii = 0; ii < num_preplay_worker_queue + 2; ii++)
	{
		el[ii].next = &el[ii+1];
		el[ii].curr_ptr = InvalidXLogRecPtr;
		el[ii].end_ptr = InvalidXLogRecPtr;
		el[ii].my_timeline = 0;
		el[ii].replayed = false;
	}
	el[ii].next = &el[0];
	SpinLockInit(&pr_history->slock);
}

PR_XLogHistory_el *
PR_addXLogHistory(XLogRecPtr currPtr, XLogRecPtr endPtr, TimeLineID myTimeline)
{
	PR_XLogHistory_el	*el;

	Assert(pr_history);

	SpinLockAcquire(&pr_history->slock);

	if (pr_history->hist_end->next == pr_history->hist_head)
	{
		SpinLockRelease(&pr_history->slock);
		elog(FATAL, "Could not allocate XLog replay history element.");
		return NULL;
	}
	el = pr_history->hist_end;
	pr_history->hist_end = el->next;

	el->curr_ptr = currPtr;
	el->end_ptr = endPtr;
	el->my_timeline = myTimeline;
	el->replayed = false;

	SpinLockRelease(&pr_history->slock);

	return el;
}

void
PR_setXLogReplayed(PR_XLogHistory_el *el)
{
	PR_XLogHistory_el *curr_el;
	PR_XLogHistory_el *last_el = NULL;
	PR_XLogHistory_el  work_el;

	Assert(pr_history);

	SpinLockAcquire(&pr_history->slock);

	el->replayed = true;
	for (curr_el = pr_history->hist_head; curr_el != pr_history->hist_end; curr_el = curr_el->next)
	{
		if (curr_el->replayed == true)
		{
			if (last_el)
			{
				last_el->curr_ptr = InvalidXLogRecPtr;
				last_el->end_ptr = InvalidXLogRecPtr;
				last_el->my_timeline = 0;
				last_el->replayed = false;
			}
			last_el = curr_el;
		}
		else
			break;
	}
	pr_history->hist_head = curr_el;
	if (last_el)
	{
		/*
		 * Koichi:
		 * pr_history の spin lock を取ったまま XLogCTL の spin lock を取っている。
		 * 安全サイドだが、deadlock を起こさないように気をつける必要がある。
		 */
		memcpy(&work_el, last_el, sizeof(PR_XLogHistory_el));
		XLogCtlDataUpdatePtr(work_el.end_ptr, work_el.my_timeline, true);
		SpinLockRelease(&pr_history->slock);
	}
	else
		SpinLockRelease(&pr_history->slock);
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

void
PR_setWorker(int worker_idx)
{
	Assert(pr_shm);

	if ((worker_idx <= 0) || (worker_idx >= num_preplay_workers))
		elog(PANIC, "Worker idex %d out of range.", worker_idx);
	my_worker_idx = worker_idx;
	my_worker = &pr_worker[worker_idx];
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
				elog(PANIC, "Invalid initial redo sequence.");
			else
				elog(LOG, "Parallel Redo Process (%d) start.", ii);
		}
		else if (pid <= 0)
		{
			/* Should not return here */
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

	/*
	 * Koichi:
	 *
	 * これは、READER 以外のすべての worker がその時点で READER から割り当てられた queue をすべて処理
	 * し終わったことの同期をとるためのものである。
	 *
	 * 同様に、まず DISPATCHER にこの指示を出し、DISPATCHER は、自分に割り当て済の全ての queue の処理を
	 * 完了したことを確認して、その他全ての worker の処理完了を監視する。
	 *
	 * これは、プロセスの完了を伴わないので、socket を使った同期処理を使うことができるし、どの worker で
	 * 同期をとってもいい。
	 */
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
	/*
	 * Note: READER worker is in fact Startup process and is handled by
	 * StartupProcessMain().
	 */
	Assert(idx > PR_READER_WORKER_IDX);


	if (idx == PR_DISPATCHER_WORKER_IDX)
	{
		PR_atStartWorker(PR_DISPATCHER_WORKER_IDX);
		dispatcherWorkerLoop();
	}
	else if (idx == PR_TXN_WORKER_IDX)
	{
		PR_atStartWorker(PR_TXN_WORKER_IDX);
		txnWorkerLoop();
	}
	else if (idx == PR_INVALID_PAGE_WORKER_IDX)
	{
		PR_atStartWorker(PR_INVALID_PAGE_WORKER_IDX);
		invalidPageWorkerLoop();
	}
	else if (PR_IS_BLK_WORKER_IDX(idx))
	{
		PR_atStartWorker(idx);
		blockWorkerLoop();
	}
	else
		elog(PANIC, "Internal error. Invalid Parallel Redo worker index: %d", idx);

	/* The worker does not return */

	/*
	 * Exit normally.  Exit code 0 tells that parallel redo process completed
	 * all the assigned recovery work.
	 */
	proc_exit(0);
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
		PR_recvSync();
		return PR_fetchQueue();
	}
	/*
	 * AKO sync flag is set.
	 */
	if (flags & PR_WK_TERMINATE)
	{
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
						PR_sendSync(ii);
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

		if (my_worker_idx == PR_DISPATCHER_WORKER_IDX)
		{
			int ii;

			/* Sync all workers but INVALID PAGE workers */
			for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers; ii++)
			{
				if (ii != PR_INVALID_PAGE_WORKER_IDX)
				{
					SpinLockAcquire(&pr_worker[ii].slock);
					pr_worker[ii].flags |= PR_WK_SYNC_DISPATCHER;
					if (pr_worker[ii].wait_dispatch == true)
					{
						pr_worker[ii].wait_dispatch = false;
						SpinLockRelease(&pr_worker[ii].slock);
						PR_sendSync(ii);
					}
					else
						SpinLockRelease(&pr_worker[ii].slock);
				}
			}
			for (ii = PR_TXN_WORKER_IDX; ii < num_preplay_workers - 1; ii++)
				PR_recvSync();

			/* Finally INVALID PAGE worker */
			SpinLockAcquire(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);
			pr_worker[PR_INVALID_PAGE_WORKER_IDX].flags |= PR_WK_SYNC_DISPATCHER;
			if (pr_worker[PR_INVALID_PAGE_WORKER_IDX].wait_dispatch == true)
			{
				pr_worker[PR_INVALID_PAGE_WORKER_IDX].wait_dispatch = false;
				SpinLockRelease(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);
				PR_sendSync(PR_INVALID_PAGE_WORKER_IDX);
			}
			else
				SpinLockRelease(&pr_worker[PR_INVALID_PAGE_WORKER_IDX].slock);
			PR_recvSync();
		}
		PR_sendSync(PR_READER_WORKER_IDX);
	}
	if (flags & PR_WK_SYNC_DISPATCHER)
	{
		Assert(my_worker_idx != PR_DISPATCHER_WORKER_IDX);

		my_worker->flags &= ~PR_WK_SYNC_DISPATCHER;
		SpinLockRelease(&my_worker->slock);
		PR_sendSync(PR_DISPATCHER_WORKER_IDX);
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
		PR_sendSync(worker_idx);
	}
	else
	{
		SpinLockRelease(&target_worker->slock);
	}
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

static int
blockHash(int spc, int db, int rel, int blk, int n_max)
{
	int wk_all;

	Assert(n_max > 0);

	/* Following code does not finish if (n_max == 1) */
	if (n_max <= 1)
		return 0;

	wk_all = fold_int2int8(spc) + fold_int2int8(db) + fold_int2int8(rel) + fold_int2int8(blk);
	wk_all = fold_int2int8(wk_all);

	while(wk_all >= n_max)
		wk_all = wk_all/n_max + wk_all%n_max;
	return wk_all;
}

INLINE int
fold_int2int8(int val)
{
	int ii;
	int rv;
	static const int num_repeat = sizeof(int);

	rv = 0;
	for (ii = 0; ii < num_repeat; ii++)
	{
		rv += val & 0xFF;
		val = val >> 8;
	}
	return rv;
}

 

/*
 ****************************************************************************
 *
 * Buffer functions
 *
 *
 ****************************************************************************
 */
/*
 * Koichi:
 *	バッファの管理の試験のため、現在のバッファエリアの CHUNK をダンプする
 *	関数を実装して、これを呼べるようにすること。
 *	これで、buffer allocation/free のたびにグローバルなバッファエリアの確認
 *	が可能となるようにする。
 */

#define SizeAtTail(chunk)   (Size *)(addr_backward(addr_forward((chunk), (chunk)->size), pr_sizeof(Size)))
#define Chunk(buffer)		(PR_BufChunk *)(addr_backward((buffer), pr_sizeof(PR_BufChunk)))
#define Tail(buffer)		SizeAtTail(Chunk(buffer))
#define Buffer(chunk)		(void *)addr_forward(chunk, pr_sizeof(PR_BufChunk))

INLINE PR_BufChunk *
next_chunk(PR_BufChunk *chunk)
{
	return (PR_BufChunk *)addr_forward(chunk, chunk->size);
}

#ifdef WAL_DEBUG
static void
dump_buffer(const char *funcname, bool need_lock)
{
	StringInfoData	s;
	int	ii;
	PR_BufChunk	*curr_chunk;

	if (!PR_test)
		return;

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	initStringInfo(&s);
	appendStringInfo(&s, "\n=== Buffer area dump: func: %s, worker: %d =================\n", funcname, my_worker_idx);
	appendStringInfo(&s, "Pr_buffer: 0x%016lx, updated: %ld,\n", (uint64)pr_buffer, pr_buffer->updated);
	appendStringInfo(&s, "head: 0x%016lx (%ld), tail: 0x%016lx (%ld)\n",
								(uint64)(pr_buffer->head), addr_difference(pr_buffer, pr_buffer->head),
								(uint64)(pr_buffer->tail), addr_difference(pr_buffer, pr_buffer->tail));
	appendStringInfo(&s, "alloc_start: 0x%016lx (%ld), alloc_end: 0x%016lx (%ld)\n",
								(uint64)(pr_buffer->alloc_start), addr_difference(pr_buffer, pr_buffer->alloc_start),
								(uint64)(pr_buffer->alloc_end), addr_difference(pr_buffer, pr_buffer->alloc_end));
	appendStringInfo(&s, "needed_by_worker: 0x%016lx (%ld) (n_worker: %d)\n    ", 
								(uint64)(pr_buffer->needed_by_worker),
								addr_difference(pr_buffer, pr_buffer->needed_by_worker),
								num_preplay_workers);
	for (ii = 0; ii < num_preplay_workers; ii++)
	{
		if (ii == 0)
			appendStringInfo(&s, "(idx: %d, value: %ld)", ii, pr_buffer->needed_by_worker[ii]);
		else
			appendStringInfo(&s, ", (idx: %d, value: %ld)", ii, pr_buffer->needed_by_worker[ii]);
	}
	appendStringInfoString(&s, "\n");
	appendStringInfoString(&s, "---Chunk---\n");
	for(curr_chunk = (PR_BufChunk *)(pr_buffer->head);
			addr_before(curr_chunk, pr_buffer->tail);
			curr_chunk = next_chunk(curr_chunk))
	{
		Size *size_at_tail;

		size_at_tail = SizeAtTail(curr_chunk);
		appendStringInfo(&s, "Addr: 0x%016lx (%ld), Size: %ld, Magic: %s, Size_at_tail: %ld\n",
								(uint64)curr_chunk,
								addr_difference(pr_buffer, curr_chunk),
								curr_chunk->size,
								curr_chunk->magic == PR_BufChunk_Allocated ? "Alloc"
									: (curr_chunk->magic == PR_BufChunk_Free ? "Free" : "Error"),
								*size_at_tail);
		if ((curr_chunk->magic != PR_BufChunk_Allocated && curr_chunk->magic != PR_BufChunk_Free) ||
				(curr_chunk->size != *size_at_tail))
		{
			appendStringInfoString(&s, "Error found in the chunk\n");
			break;
		}
	}
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	appendStringInfoString(&s, "\n=== Buffer dump end =================\n");
	PRDebug_out(&s);
	pfree(s.data);
	s.data = NULL;
}

static void
dump_chunk(PR_BufChunk *chunk, const char *funcname, bool need_lock)
{
	StringInfoData	s;
	Size	*size_at_tail;

	if (!PR_test)
		return;

	initStringInfo(&s);

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	size_at_tail = SizeAtTail(chunk);
	appendStringInfo(&s, "\n=== Chunk dump: func: %s =================\n", funcname);
	appendStringInfo(&s, "Buffer: 0x%016lx, head: 0x%016lx (%ld), tail: 0x%016lx (%ld)\n",
			(uint64)pr_buffer,
			(uint64)(pr_buffer->head), addr_difference(pr_buffer->head, pr_buffer),
			(uint64)(pr_buffer->tail), addr_difference(pr_buffer->tail, pr_buffer));
	appendStringInfo(&s, "Chunk: 0x%016lx (%ld), size: %ld, magic: %s, size_at_tail: %ld\n",
			(uint64)chunk, addr_difference(pr_buffer, chunk), chunk->size,
			chunk->magic == PR_BufChunk_Allocated ? "Alloc"
				: (chunk->magic == PR_BufChunk_Free ? "Free" : "Error"),
			*size_at_tail);
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	appendStringInfoString(&s, "\n=== Chunk dump end =================\n");
	PRDebug_out(&s);
	pfree(s.data);
	s.data = NULL;
}

#endif

void *
PR_allocBuffer(Size sz, bool need_lock)
{
	PR_BufChunk	*new;
	Size		 chunk_sz;
	void		*wk;
	void		*rv;

#ifdef WAL_DEBUG
	PRDebug_log("--- %s: allocation size: %lu ---\n", __func__, sz);
	dump_buffer(__func__, need_lock);
#endif
	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);

#ifdef WAL_DEBUG
	pr_buffer->updated++;
#endif
	if (pr_buffer->alloc_start == pr_buffer->alloc_end)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return retry_allocBuffer(sz, need_lock);
	}
	chunk_sz = sz + pr_sizeof(PR_BufChunk) + pr_sizeof(Size);
	if (addr_before(pr_buffer->alloc_start, pr_buffer->alloc_end))
	{
		new = alloc_chunk(chunk_sz, pr_buffer->alloc_start, pr_buffer->alloc_end);
		if (new == NULL)
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			rv = retry_allocBuffer(sz, need_lock);
#ifdef WAL_DEBUG
			dump_buffer(__func__, need_lock);
			PRDebug_log("--- %s: returning: %p ---\n", __func__, rv);
#endif
			return rv;
		}
		else
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			rv = addr_forward(new, pr_sizeof(PR_BufChunk));
#ifdef WAL_DEBUG
			dump_buffer(__func__, need_lock);
			PRDebug_log("--- %s: returning: %p ---\n", __func__, rv);
#endif
			return rv;
		}
	}
	else
	{
		new = alloc_chunk(chunk_sz, pr_buffer->alloc_start, pr_buffer->tail);
		if (new)
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			rv = addr_forward(new, pr_sizeof(PR_BufChunk));
#ifdef WAL_DEBUG
			dump_buffer(__func__, need_lock);
			PRDebug_log("--- %s: returning: %p ---\n", __func__, rv);
#endif
			return rv;
		}
		wk = pr_buffer->alloc_start;
		pr_buffer->alloc_start = pr_buffer->head;
		new = alloc_chunk(chunk_sz, pr_buffer->alloc_start, pr_buffer->alloc_end);
		if (new == NULL)
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			pr_buffer->alloc_start = wk;
			rv = retry_allocBuffer(sz, need_lock);
#ifdef WAL_DEBUG
			dump_buffer(__func__, need_lock);
			PRDebug_log("--- %s: returning: %p ---\n", __func__, rv);
#endif
			return rv;
		}
		else
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			rv = addr_forward(new, pr_sizeof(PR_BufChunk));
#ifdef WAL_DEBUG
			dump_buffer(__func__, need_lock);
			PRDebug_log("--- %s: returning: %p ---\n", __func__, rv);
#endif
			return rv;
		}
	}
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
#ifdef WAL_DEBUG
		dump_buffer(__func__, need_lock);
		PRDebug_log("--- %s: returning: NULL ---\n", __func__);
#endif
	return NULL;
}


static void *
retry_allocBuffer(Size sz, bool need_lock)
{
#ifdef WAL_DEBUG
	dump_buffer(__func__, need_lock);
#endif
	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	pr_buffer->needed_by_worker[my_worker_idx] = sz;
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	if (!isOtherWorkersRunning())
		elog(PANIC,
				"Shared buffer overflow.  "
				"Please consider to increase preplay_buffers configuation parameter.");
	PR_recvSync();

	return PR_allocBuffer(sz, need_lock);
}


static bool
isOtherWorkersRunning(void)
{
	int	ii;

	/*
	 * Koichi:
	 *
	 *  If preplay_buffers is quite small, we may have a situation as follows:
	 *
	 *  1. READER waits for preplay buffer,
	 *
	 *  2. All other workers are waiting for preplay buffer, or waiting for dispatch from
	 *     other workers.
	 *
	 *  As long as one worker is not waiting, at least, then buffer used such worker will be
	 *  released afterwards and will be available to other workers.   In such a way,
	 *  if preplay_buffers is reasonable, we can make balance of allocation/free of buffer
	 *  and running state becomes stable.
	 *
	 *  In the situation with very small preplay_buffers, we may have above situation.
	 *
	 *  This function detects such situation.  The situation will be captured by the final
	 *  worker to try to allcate the buffer and fail.
	 *
	 *  The function will be called when:
	 *
	 *  1. Fails to allocate the buffer and about to wait for another worker to free the buffer.
	 *  2. When all the queue has been handled and about to wait for dispatch.
	 *
	 *  In either case, if all the other workers are waiting for other worker for available buffer,
	 *  we have no means to proceed but to terminate whole database activity.
	 *
	 *  Here's some consideration about preplay_buffers value:
	 *
	 *  WAL may have multiple block image.  Maximum number of blocks in single WAL record is
	 *  XLR_MAX_BLOCK_ID, which is 32 at present.  So minimum value of preplay_buffers will be:
	 *		WAL_BLKSIZE * XLR_MAX_BLOCK_ID * num_preplay_workers + OVERHEAD
	 *  We need to consider another overhead like XLogReaderState or other common structure.
	 *  This will be at most:
	 *      WAL_BLKSIZE * num_preplay_workers
	 *  So recommended minumum value is:
	 *      WAL_BLISIXE * (XLR_MAX_BLOCK_ID + 1) * num_preplay_workers
	 *  If possible:
	 *      WAL_BLISIXE * (XLR_MAX_BLOCK_ID + 2) * num_preplay_workers
	 *
	 *   If num_preplay_workers is 12 (then eith BLK workers), this value is around 3MB.  For most case,
	 *   4MB looks reasonable minimum value.   This is about a quarter of WAL segment and is quite
	 *   smaller than minimum value of shared_buffers, 128MB.
	 */
	/* READER worker */
	SpinLockAcquire(&pr_buffer->slock);
	if (pr_buffer->needed_by_worker[PR_READER_WORKER_IDX] == 0)
	{
		/* READER worker is running */
		SpinLockRelease(&pr_buffer->slock);
		return true;
	}
	/* Now READER worker is waiting for free buffer */
	/* Other workers */
	for (ii = PR_DISPATCHER_WORKER_IDX; ii < num_preplay_workers; ii++)
	{
		if (ii == my_worker_idx)
			/* It's myself, skip */
			continue;
		if (pr_buffer->needed_by_worker[ii] != 0)
		{
			/* This worker is waiting for available buffer */
			continue;
		}
		SpinLockAcquire(&pr_worker[ii].slock);
		if (pr_buffer->needed_by_worker[ii] == 0)
		{
			if (pr_worker[ii].wait_dispatch == false)
			{
				/* This worker has assigned queue to handle, it's running. */
				SpinLockRelease(&pr_worker[ii].slock);
				SpinLockRelease(&pr_buffer->slock);
				return true;
			}
		}
		else
		{
			SpinLockRelease(&pr_worker[ii].slock);
			continue;
		}
	}

	SpinLockRelease(&pr_buffer->slock);
	return false;
}


void
PR_freeBuffer(void *buffer, bool need_lock)
{
	PR_BufChunk	*chunk;
	Size		 available;
	int			 ii;
	bool		 needed_by_lower_worker;

	/* Do not forget to ping reader or dispatcher if there's free area available */
	/* Ping the reader worker only when dispatcher does not require the buffer */

#ifdef WAL_DEBUG
	PRDebug_log("--- %s: freeing: %p ---\n", __func__, buffer);
	dump_chunk(Chunk(buffer), __func__, need_lock);
	dump_buffer(__func__, need_lock);
#endif
	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
#ifdef WAL_DEBUG
	pr_buffer->updated++;
#endif
	chunk = Chunk(buffer);
	if (chunk->magic != PR_BufChunk_Allocated)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		elog(PANIC, "Attempt to free wrong buffer.");
	}
	free_chunk(chunk);

	/*
	 * Check the available chunk size
	 *
	 * If we have sufficient space avaiable for request from another worker,
	 * which should be waiting with Sync socket, we send sync so that the
	 * worker can get buffer allocated.   There is still small chance that
	 * such available buffer can be stolen by another worker but the worker
	 * can try again yet.
	 */

	available = available_size(pr_buffer);

	for (ii = PR_TXN_WORKER_IDX, needed_by_lower_worker = false; ii < num_preplay_workers; ii++)
	{
		if (pr_buffer->needed_by_worker[ii] != 0)
		{
			needed_by_lower_worker = true;
			if ((pr_buffer->needed_by_worker[ii] <= available) && (ii != my_worker_idx))
			{
				if (need_lock)
					SpinLockRelease(&pr_buffer->slock);
				if (ii == my_worker_idx)
					elog(PANIC, "Conflicting situation: replay worker freeing the buffer is also waiting for the buffer.");
				PR_sendSync(ii);
#ifdef WAL_DEBUG
				dump_buffer(__func__, need_lock);
#endif
				return;
			}
		}
	}
	
	if (needed_by_lower_worker == true)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return;
	}

	for (ii = PR_READER_WORKER_IDX; ii < PR_TXN_WORKER_IDX; ii++)
	{
		if (pr_buffer->needed_by_worker[ii] != 0)
		{
			if ((pr_buffer->needed_by_worker[ii] <= available) && (ii != my_worker_idx))
			{
				if (need_lock)
					SpinLockRelease(&pr_buffer->slock);
				if (ii == my_worker_idx)
					elog(PANIC, "Conflicting situation: replay worker freeing the buffer is also waiting for the buffer.");
				PR_sendSync(ii);
#ifdef WAL_DEBUG
				dump_buffer(__func__, need_lock);
#endif
				return;
			}
		}
	}

	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
#ifdef WAL_DEBUG
	dump_buffer(__func__, need_lock);
#endif
	return;

}

/* Caller must obtain pr_buffer->slock lock */
static Size
available_size(PR_buffer *buffer)
{
	Size	sz1,
			sz2;

	if (addr_before(buffer->alloc_start, buffer->alloc_end))
		return ((PR_BufChunk *)buffer)->size - pr_sizeof(PR_BufChunk) - pr_sizeof(Size);

	sz1 = addr_difference(buffer->tail, buffer->alloc_start) - pr_sizeof(PR_BufChunk) - pr_sizeof(Size);
	sz2 = addr_difference(buffer->alloc_end, buffer->head) - pr_sizeof(PR_BufChunk) - pr_sizeof(Size);

	if (sz1 > sz2)
		return sz1;
	else
		return sz2;
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
	pr_buffer->updated = 0;
	dump_buffer(__func__, false);
#endif
	SpinLockInit(&pr_buffer->slock);
}


/* The caller should acquire the lock pr_buffer->slock */
static PR_BufChunk *
prev_chunk(PR_BufChunk *chunk)
{
	Size *size_at_tail;

	if (chunk == (PR_BufChunk *)pr_buffer->head)
		return NULL;
	size_at_tail = addr_backward(chunk, pr_sizeof(Size));
	return (PR_BufChunk *)addr_backward(chunk, *size_at_tail);
}


/* The caller should acquire the lock pr_buffer->slock */
static void
free_chunk(PR_BufChunk *chunk)
{
	if (chunk->magic != PR_BufChunk_Allocated)
		return;
	chunk->magic = PR_BufChunk_Free;
	if (chunk == pr_buffer->alloc_end)
		pr_buffer->alloc_end = addr_forward(pr_buffer->alloc_end, chunk->size);
	concat_next_chunk(chunk);
	concat_prev_chunk(chunk);
	chunk_arrange_free_wraparound();
}


/* Concatinate this free chunk with the next one */
/* Caller must acquire the lock pr_block->slock */
static void
concat_next_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk *next;
	PR_BufChunk *new_end;
	Size	*size_at_tail;

	Assert(pr_buffer && (chunk->magic == PR_BufChunk_Free));
	Assert(addr_after_eq(chunk, pr_buffer->head) && addr_before_eq(chunk, pr_buffer->tail));

	next = next_chunk(chunk);
	if (addr_after_eq(next, pr_buffer->tail))
		return;
	if (next->magic != PR_BufChunk_Free)
		return;
	new_end = next_chunk(next);
	chunk->size += next->size;
	size_at_tail = SizeAtTail(chunk);
	*size_at_tail = chunk->size;
	if (next == pr_buffer->alloc_start)
		pr_buffer->alloc_start = chunk;
	if (next == pr_buffer->alloc_end)
		pr_buffer->alloc_end = new_end;
}

/* Concatinate this free chunk with the previous one */
/* Caller must acquire the lock pr_block->slock */
static void
concat_prev_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk *prev;
	PR_BufChunk *new_end;
	Size	*size_at_tail;

	Assert(pr_buffer && (chunk->magic == PR_BufChunk_Free));
	Assert(addr_after_eq(chunk, pr_buffer->head) && addr_before_eq(chunk, pr_buffer->tail));

	if (addr_before_eq(chunk, pr_buffer->head))
		return;
	size_at_tail = SizeAtTail(chunk);
	prev = prev_chunk(chunk);
	if (prev->magic != PR_BufChunk_Free)
		return;
	new_end = next_chunk(chunk);
	prev->size += chunk->size;
	*size_at_tail = prev->size;
	if (chunk == pr_buffer->alloc_start)
		pr_buffer->alloc_start = prev;
	if (addr_after(pr_buffer->alloc_end, prev) && addr_before(pr_buffer->alloc_end, new_end))
		pr_buffer->alloc_end = new_end;
}

/* Rearrange pr_buffer->alloc_start */
static void
chunk_arrange_free_wraparound(void)
{
	PR_BufChunk	*head;

	if (pr_buffer->alloc_start == pr_buffer->tail)
		pr_buffer->alloc_start = pr_buffer->head;
	if (pr_buffer->alloc_end == pr_buffer->tail)
	{
		head = (PR_BufChunk *)pr_buffer->head;
		if (head->magic == PR_BufChunk_Free)
			pr_buffer->alloc_end = addr_forward(head, head->size);
	}
}


/* The caller should acquire the lock pr_buffer->slock */
static PR_BufChunk *
alloc_chunk(Size sz, void *start, void *end)
{
	Size		 available;
	PR_BufChunk	*chunk;
	PR_BufChunk	*next;
	Size *size_at_tail;

	Assert(addr_before(start, end));

	available = addr_difference(end, start);
	if (available < sz)
		return NULL;
	if ((available - sz) <= pr_sizeof(PR_BufChunk) + pr_sizeof(Size))
		sz = available;
	chunk = (PR_BufChunk *)start;
	chunk->size = sz;
	chunk->magic = PR_BufChunk_Allocated;
	size_at_tail = SizeAtTail(chunk);
	*size_at_tail = sz;
	next = next_chunk(chunk);
	if (available > sz)
	{
		next->size = available - sz;
		next->magic = PR_BufChunk_Free;
		size_at_tail = SizeAtTail(next);
		*size_at_tail = next->size;
	}
	pr_buffer->alloc_start = next;
	concat_next_chunk(next);
	return chunk;
}

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


#define	PRDEBUG_BUFSZ 4096
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
		{
			elog(PANIC, "Failed to stat PR debug directory, %s", strerror(my_errno));
		}
		rv = mkdir(debug_log_dir, S_IRUSR|S_IWUSR|S_IXUSR);
		local_errno = errno;
		if (rv != 0)
		{
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

	my_worker_idx = worker_idx;
	build_PRDebug_log_hdr();
	pr_debug_signal_file = (char *)malloc(DEBUG_LOG_FILENAME_LEN);
	sprintf(pr_debug_signal_file, "%s/%s/%d.signal", DataDir, DEBUG_LOG_DIRNAME, my_worker_idx);
	unlink(pr_debug_signal_file);
	PRDebug_log("\n-------------------------------------------\n"
				"Now ready to attach the debugger to pid %d.  "
				"Set the break point to PRDebug_sync()\n"
			    "My worker idx is %d.\nPlease touch %s to begin.  "
				"I'm waiting for it.\n\n"
				"Do following from another shell:\n"
				"sudo gdb\n"
				"attach %d\n"
				"tb PRDebug_sync\n"
				"source breaksymbol.gdb\n"
				"shell touch  %s\n"
				"c\n",
			getpid(), worker_idx, pr_debug_signal_file,
			getpid(), pr_debug_signal_file);
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
	elog(LOG, "%s%s", pr_debug_log_hdr, buf);
	fprintf(pr_debug_log, "%s%s", pr_debug_log_hdr, buf);
	fflush(pr_debug_log);

	error_context_stack = error_context_stack_backup;
}

static void
PRDebug_out(StringInfo s)
{
	fwrite(s->data, s->len, sizeof(char), pr_debug_log);
	fflush(pr_debug_log);
}

static void
build_PRDebug_log_hdr(void)
{
	pr_debug_log_hdr = (char *)malloc(PRDEBUG_HDRSZ);
	sprintf(pr_debug_log_hdr, "PRDdebug_LOG, worker_idx(%d): ", my_worker_idx);
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

#ifdef WAL_DEBUG

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

/*
 * When this starts, we assume that worker data have already been set up.
 *
 * So this is just a main loop.
 */
static void
invalidPageWorkerLoop(void)
{
	PR_queue_el	*el;
	XLogInvalidPageData_PR	*page;
	bool invalidPageFound;

	SpinLockAcquire(&my_worker->slock);
	my_worker->handledRecPtr = 0;
	SpinLockRelease(&my_worker->slock);
	
	for (;;)
	{
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != InvalidPageData)
			elog(PANIC, "Invalid internal status for invalid page worker.");
		page = (XLogInvalidPageData_PR *)(el->data);
		PR_freeQueueElement(el);
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
	return;
}


/*
 *****************************************************************************************************************
 * Block Worker
 *****************************************************************************************************************
 */

static void
blockWorkerLoop(void)
{
	PR_queue_el	*el;
	XLogDispatchData_PR	*data;
	XLogRecord	*record;
	int		*worker_list;
	
	for (;;)
	{
		XLogRecPtr	currRecPtr;

		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != XLogDispatchData)
			elog(PANIC, "Invalid internal status for block worker.");
		data = (XLogDispatchData_PR *)(el->data);
		PR_freeQueueElement(el);

		SpinLockAcquire(&data->slock);

		data->n_remaining--;
		currRecPtr = data->reader->ReadRecPtr;

		if (data->n_remaining > 0)
		{
			/* This worker is not eligible to handle this */
			SpinLockRelease(&data->slock);
			/* Wait another BLOCK worker to handle this and sync to me */
			PR_recvSync();
		}
		else
		{
			/* Dequeue */

			/* OK. I should handle this. Nobody is handling this and safe to release the lock. */
			record = data->reader->record;
			SpinLockRelease(&data->slock);

			/* REDO */

			RmgrTable[record->xl_rmid].rm_redo(data->reader);

			/* Koichi: TBD: ここで、XLogHistory と txn history の更新を行うこと */
			
			if ((record->xl_info & XLR_CHECK_CONSISTENCY) != 0)
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
					PR_sendSync(*worker_list);
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
			freeDispatchData(data);
		}
		SpinLockAcquire(&my_worker->slock);
		my_worker->handledRecPtr = currRecPtr;
		SpinLockRelease(&my_worker->slock);
	}
	return;
}

/*
 ****************************************************************************************************************
 * Transaction Worker
 ****************************************************************************************************************
 */

/*
 * Need to add hash key to take care of block worker data
 */
/*
 * Koichi: TXN 完了時点で、このTXNのWAL replay 完了同期を取る必要がある。この情報をどのように保持するか、検討要。
 *
 * 問題は、
 *			* メモリを節約するため、完了した WAL レコードは速やかに TXN の情報から除去する必要がある
 *			* 同期を取るのは、UDP ソケットの一方通行のみで行う
 *			* WAL の replay を行った worker は、実行されるまで決定されない
 */
static void
txnWorkerLoop(void)
{
	PR_queue_el			*el;
	XLogDispatchData_PR *dispatch_data;
	XLogReaderState		*xlogreader;
	XLogRecord			*record;
	TransactionId		 xid;
	txn_cell_PR			*txn_cell;

	for (;;)
	{
		XLogRecPtr currRecPtr;

		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != XLogDispatchData)
			elog(PANIC, "Invalid internal status for transaction worker.");
		dispatch_data = (XLogDispatchData_PR *)(el->data);
		record = dispatch_data->reader->record;
		xlogreader = dispatch_data->reader;
		currRecPtr = xlogreader->ReadRecPtr;
		txn_cell = isTxnSyncNeeded(dispatch_data, record, &xid, true);
		if (txn_cell)
		{
			/*
			 * Here waits until all the preceding WAL records for this trancation
			 * are handled by block workers.
			 */
			syncTxn(txn_cell);

			/* Deallocate the trancaction cell */
			free_txn_cell(txn_cell, true);
#if 0
			/* txn_cell is removed within syncTxn() */
			removeTxnCell(txn_cell);
#endif
		}
		/*
		 * Koichi: TBD
		 *		ここで replay して、txn ヒストリや XLogCtl のアップデートを行う
		 */
		/* Now apply the WAL record itself */
		RmgrTable[record->xl_rmid].rm_redo(xlogreader);

		SpinLockAcquire(&my_worker->slock);
		my_worker->handledRecPtr = currRecPtr;
		SpinLockRelease(&my_worker->slock);
		
		/* ここで redo 終わったので、終了した LSN の更新を行う */
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
		freeDispatchData(dispatch_data);
	}
	return;
}

static void
getXLogRecordRmgrInfo(XLogReaderState *reader, RmgrId *rmgrid, uint8 *info)
{
	*rmgrid = XLogRecGetRmid(reader);
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
		PR_recvSync();
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

static void
dispatcherWorkerLoop(void)
{
	PR_queue_el *el;
	XLogReaderState *reader;
	XLogRecord *record;
	XLogDispatchData_PR	*dispatch_data;
	int	*worker_list;

	for (;;)
	{
		XLogRecPtr currRecPtr;

		/* Dequeue */
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != ReaderState)
			elog(PANIC, "Invalid internal status for dispatcher worker.");
		reader = (XLogReaderState *)el->data;
		currRecPtr = reader->ReadRecPtr;
		PR_freeQueueElement(el);
		record = reader->record;
		dispatch_data = PR_analyzeXLogReaderState(reader, record);
		if (isSyncBeforeDispatchNeeded(reader))
			PR_syncBeforeDispatch();
		dispatchDataToXLogHistory(dispatch_data);
		addDispatchDataToTxn(dispatch_data, false);
		for (worker_list = dispatch_data->worker_list; *worker_list > PR_DISPATCHER_WORKER_IDX; worker_list++)
			PR_enqueue(dispatch_data, XLogDispatchData, *worker_list);

		SpinLockAcquire(&my_worker->slock);
		my_worker->handledRecPtr = currRecPtr;
		SpinLockRelease(&my_worker->slock);
	}
	return;
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

