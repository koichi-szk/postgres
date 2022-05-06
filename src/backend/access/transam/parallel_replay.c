/*-------------------------------------------------------------------------
 *
 * parallel_replay_util.c
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
#include "miscadmin.h"
#include "pg_config.h"
#include "postmaster/postmaster.h"
#include "replication/origin.h"
#include "replication/walreceiver.h"
#include "storage/dsm.h"
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

bool    parallel_replay = false;	/* If parallel replay is enabled */
int     num_preplay_workers;		/* Number of parallel replay worker */
int     num_preplay_worker_queue;	/* Number of total queue for parallel replay */
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
static txn_cell_pool_PR	*pr_txn_cell_pool = NULL;
static PR_invalidPages   	*pr_invalidPages = NULL;
static PR_XLogHistory	*pr_history = NULL;
static PR_worker	*pr_worker = NULL;
static PR_queue		*pr_queue = NULL;
static PR_buffer	*pr_buffer = NULL;

/* My worker process info */
static int           my_worker_idx = 0;         /* Index of the worker.  Set when the worker starts. */
static PR_worker    *my_worker = NULL;      /* My worker structure */

/* Socket for sync among workers */
static int          *worker_socket = NULL;

/* Module-global definition */
#define PR_MAXPATHLEN	512



/*
 * Size boundary, address calculation and boundary adjustment
 */

#define MemBoundary         (sizeof(void *))
#define addr_forward(p, s)  ((void *)((uint64)(p) + (Size)(s)))
#define addr_backward(p, s) ((void *)((uint64)(p) - (Size)(s)))
#define addr_subtract(a, b) ((Size)((uint64)(b) - (uint64)(a)))
#define size_boundary(s)    ((((s) + MemBoundary - 1) / MemBoundary) * MemBoundary)
#define address_boundary(s) (((((uint64)(s)) + MemBoundary - 1) / MemBoundary) * MemBoundary)
#define addr_after(a, b)    ((uint64)(a) > (uint64)(b))
#define addr_after_eq(a, b) ((uint64)(a) >= (uint64)(b))
#define addr_before(a, b)   ((uint64)(a) < (uint64)(b))
#define addr_before_eq(a, b)    ((uint64)(a) <= (uint64)(b))
#define Sizeof(s)           size_boundary(sizeof(s))

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
static void			*expandBuffer_new(void *buffer, Size newsz, bool need_lock); 

/*
 * Dispatch Data function
 */
static void	freeDispatchData(XLogDispatchData_PR *dispatch_data);

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
static bool			 removeTxnCell(txn_cell_PR *txn_cell);
static txn_cell_PR	*isTxnSyncNeeded(XLogDispatchData_PR *dispatch_data, XLogRecord *record, TransactionId *xid, bool remove_myself);
static void			 syncTxn(txn_cell_PR *txn_cell);
static void			 dispatchDataToXLogHistory(XLogDispatchData_PR *dispatch_data);
#define get_txn_hash(x) &pr_txn_hash[txn_hash_value(x)]

/* XLogReaderState/XLogRecord functions */
static int	blockHash(int spc, int db, int rel, int blk, int n_max);
INLINE int	fold_int2int8(int val);

/* Workerr Loop */
static void dispatcherWorkerLoop(void);
static void txnWorkerLoop(void);
static void invalidPageWorkerLoop(void);
static void blockWorkerLoop(void);


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
	snprintf(sync_sock_dir, PR_MAXPATHLEN, "%s/%s", DataDir, PR_SYNCSOCKDIR);
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

#define PR_SOCKF_LEN 128
void
PR_syncFinishSockDir(void)
{
	char	sockdir[PR_SOCKF_LEN];

	snprintf(sockdir, PR_SOCKF_LEN, "%s/%s", DataDir, PR_SYNCSOCKDIR);
	rmtree(sockdir, true);
}
#undef PR_SOCKF_LEN

/*
 * Create socket and bind to the sync socket for the local worker process.
 * This should be called inide each worker process.
 */
void
PR_syncInit(void)
{
    int     rc;
    int     ii;
    struct sockaddr_un  sockaddr;

    Assert(pr_shm && sync_sock_dir);

    worker_socket = (int *)palloc(sizeof(int) * num_preplay_workers);

    for (ii = 0; ii < num_preplay_workers; ii++)
    {
        worker_socket[ii] = socket(AF_UNIX, SOCK_DGRAM, 0);
        if (worker_socket[ii] < 0)
            ereport(FATAL,
                    (errcode_for_socket_access(),
                     errmsg("Could not create the socket: %m")));
        sprintf(sockaddr.sun_path, PR_SYNCSOCKFMT, DataDir, PR_SYNCSOCKDIR, ii);
        sockaddr.sun_family = AF_UNIX;
        rc = bind(worker_socket[ii], &sockaddr, sizeof(sockaddr));
        if (rc < 0)
            ereport(FATAL,
                    (errcode_for_socket_access(),
                     errmsg("Could not bind the socket to \"%s\": %m", sockaddr.sun_path)));
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
		close(worker_socket[ii]);
}

void
PR_sendSync(int worker_idx)
{
    Size    ll;

    Assert(pr_shm && worker_idx != my_worker_idx);

    ll = send(worker_socket[worker_idx], my_worker_msg, my_worker_msg_sz, 0);
    if (ll != my_worker_msg_sz)
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Could not send whole message from worker %d to %d: %m", my_worker_idx, worker_idx)));
}

int
PR_recvSync(void)
{
#define SYNC_RECV_BUF_SZ    64
    char    recv_buf[SYNC_RECV_BUF_SZ];
    Size    sz;

    Assert (pr_shm);

    sz = recv(worker_socket[my_worker_idx], recv_buf, SYNC_RECV_BUF_SZ, 0);
    if (sz < 0)
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("Could not receive message.  worker %d: %m", my_worker_idx)));
    return(atoi(recv_buf));
#undef SYNC_RECV_BUF_SZ
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

	for (ii = 1; ii < num_preplay_workers; ii++)
	{
		SpinLockAcquire(&pr_worker[ii].slock);
		pr_worker[ii].flags |= PR_WK_SYNC_READER;
		if (pr_worker[ii].wait_dispatch)
		{
			/*
			 * Be careful.  If there are no dispatched queue for the worker,
			 * worker might have asked for new queue.  In this case,
			 * sync the worker to move forward.
			 */
			pr_worker[ii].wait_dispatch = false;
			SpinLockRelease(&pr_worker[ii].slock);
			PR_sendSync(ii);
		}
		else
			SpinLockRelease(&pr_worker[ii].slock);
	}
	for (ii = 1; ii < num_preplay_workers; ii++)
		PR_recvSync();
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

	my_shm_size = Sizeof(PR_shm)
		+ (my_txn_hash_sz = pr_txn_hash_size())
		+ (my_invalidP_sz = Sizeof(PR_invalidPages))
		+ (my_history_sz = xlogHistorySize())
		+ (my_worker_sz = worker_size())
	   	+ (my_queue_sz = queue_size())
		+ buffer_size();

	pr_shm_seg = dsm_create(my_shm_size, 0);
	
	pr_shm = dsm_segment_address(pr_shm_seg);
	pr_shm->txn_wal_info = pr_txn_wal_info = addr_forward(pr_shm, Sizeof(pr_shm));
	pr_shm->invalidPages = pr_invalidPages = addr_forward(pr_txn_wal_info, my_txn_hash_sz);
	pr_shm->history = pr_history = addr_forward(pr_invalidPages, Sizeof(PR_invalidPages));
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
	return (Sizeof(txn_wal_info_PR) + (Sizeof(txn_hash_el_PR) * txn_hash_size)
			+ (Sizeof(txn_cell_PR) * num_preplay_max_txn));
}

static void
init_txn_hash(void)
{
	int	ii;
	txn_cell_PR		*cell_pool;
	txn_hash_el_PR	*txn_hash;


	pr_txn_wal_info = pr_shm->txn_wal_info;
	pr_txn_wal_info->txn_hash = pr_txn_hash = addr_forward(pr_txn_wal_info, Sizeof(txn_wal_info_PR));
	pr_txn_wal_info->cell_pool = pr_txn_cell_pool = addr_forward(pr_txn_wal_info->txn_hash, Sizeof(txn_hash_el_PR) * txn_hash_size);
	for (ii = 0, txn_hash = pr_txn_wal_info->txn_hash; ii < txn_hash_size; ii++)
	{
		SpinLockInit(&txn_hash[ii].slock);
		txn_hash[ii].head = txn_hash[ii].tail = NULL;
	}
	for (ii = 0, cell_pool = pr_txn_wal_info->cell_pool->next; ii < num_preplay_max_txn; ii++)
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
		SpinLockAcquire(&pr_txn_cell_pool->slock);
	rv = pr_txn_cell_pool->next;
	if (rv == NULL)
		pr_txn_cell_pool->next = NULL;
	else
		pr_txn_cell_pool->next = rv->next;
	rv->next = NULL;
	if (need_lock)
		SpinLockRelease(&pr_txn_cell_pool->slock);
	return rv;
}

static void
free_txn_cell(txn_cell_PR *txn_cell, bool need_lock)
{
	txn_cell->xid = InvalidTransactionId;
	txn_cell->head = txn_cell->tail = NULL;
	if (need_lock)
		SpinLockAcquire(&pr_txn_cell_pool->slock);
	txn_cell->next = pr_txn_cell_pool->next;
	pr_txn_cell_pool->next = txn_cell;
	if (need_lock)
		SpinLockRelease(&pr_txn_cell_pool->slock);
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

static bool
removeTxnCell(txn_cell_PR *txn_cell)
{
	txn_hash_el_PR	*hash_el;
	txn_cell_PR		*cell, *prev;

	Assert(txn_cell->head == NULL && txn_cell->tail == NULL);

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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(Sizeof(invalidData), true);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(Sizeof(invalidData), true);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(Sizeof(invalidData), true);
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

	invalidData = (XLogInvalidPageData_PR *)PR_allocBuffer(Sizeof(invalidData), true);
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
	return (Sizeof(PR_XLogHistory) + (Sizeof(PR_XLogHistory_el) * (num_preplay_worker_queue + 2)));
}

static void
initXLogHistory(void)
{
	PR_XLogHistory_el	*el;
	int	ii;

	Assert(pr_history);

	el = (PR_XLogHistory_el *)addr_forward(pr_history, Sizeof(PR_XLogHistory));
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
	return (Sizeof(PR_worker) * num_preplay_workers);
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

	if (idx != PR_READER_WORKER_IDX)
		/* PRDebug_start() has already called in READER WORKER */
		PRDebug_start(idx);	/* Do tiny things for gdb to attach this process */

	my_worker_idx = idx;
	my_worker = &pr_shm->workers[idx];

	PR_syncInit();	/* Initialize synchronization sockets */

	if (idx == PR_READER_WORKER_IDX)
		return;
	SpinLockAcquire(&my_worker->slock);
	my_worker->wait_dispatch = true;
	my_worker->flags = 0;
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
	/*
	 * Koichi:
	 *
	 * これは READER worker から呼ばれるもの。そこで、Dispatcher worker の終了だけを待つようにする。
	 * DISPATCHER WORKER は、READER から dispatch された全ての queue を他の worker に dispatch した後、
	 * 終了する。
	 *
	 * READER WORKER は、DISPATCH WORKER の終了を待った後、その他の WORKER の termiante フラグを立てて、
	 * これらの終了を待つ。
	 *
	 * このようにするのは、DISPATCHER がまだ割り当てる queue があるのに、他の WORKER が終了を勘違いして
	 * 集結するのを防ぐためで、DISPATCHER が全ての処理を終了し、これらが全て他の worker に割り当て済である
	 * ことを確実にするためである。
	 *
	 * また、全ての worker は READER の子プロセスなので、これらの終了を待つのは READER worker であるのが
	 * 望ましい。
	 */

}

/* See xlog.c */
void
PR_WaitDispatcherQueueHandling(void)
{
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
	/* The worker does not return */
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

	return;
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
	return (Sizeof(PR_queue)									/* Queue sructure */
			+ (Sizeof(int) * (num_preplay_workers + 1))			/* Worker array after Queue */
			+ (Sizeof(PR_queue_el) * num_preplay_worker_queue));/* Queue element */
}

static void
initQueue(void)
{
	Assert(pr_queue);

	pr_queue->num_queue_element = num_preplay_worker_queue;
	pr_queue->wait_worker_list = addr_forward(pr_queue, Sizeof(PR_queue));
	initQueueElement();
	SpinLockInit(&pr_queue->slock);
}

static void
initQueueElement(void)
{
	PR_queue_el	*el;
	int	ii;

	Assert(pr_queue);

	pr_queue->element = addr_forward(pr_queue->wait_worker_list, Sizeof(int) * (num_preplay_workers + 1));
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

	SpinLockAcquire(&my_worker->slock);
	rv = my_worker->head;
	if (my_worker->head == NULL)
	{
		/*
		 * The following looks unnecessarily long. Needs simplification.
		 */
		if (my_worker->flags & PR_WK_SYNC_READER)
		{
			SpinLockRelease(&my_worker->slock);
			PR_sendSync(PR_READER_WORKER_IDX);
			return PR_fetchQueue();
		}
		else if (my_worker->flags & PR_WK_SYNC_DISPATCHER)
		{
			SpinLockRelease(&my_worker->slock);
			PR_sendSync(PR_DISPATCHER_WORKER_IDX);
			return PR_fetchQueue();
		}
		else if (my_worker->flags & PR_WK_SYNC_TXN)
		{
			SpinLockRelease(&my_worker->slock);
			PR_sendSync(PR_TXN_WORKER_IDX);
			return PR_fetchQueue();
		}
		else if (my_worker->flags & PR_WK_TERMINATE)
		{
			SpinLockRelease(&my_worker->slock);
			return NULL;
		}
		else
		{
			my_worker->wait_dispatch = true;
			SpinLockRelease(&my_worker->slock);
			PR_recvSync();
			return PR_fetchQueue();
		}
	}
	my_worker->head = rv->next;
	if (my_worker->head == NULL)
		my_worker->tail = NULL;
	SpinLockRelease(&my_worker->slock);
	return rv;
}

/*
 * Enqueue
 */
void
PR_enqueue(void *data, PR_QueueDataType type, int	worker_idx)
{
	PR_queue_el	*el;
	PR_worker	*target_worker;

	target_worker = &pr_worker[worker_idx];
	el = getQueueElement();
	el->next = NULL;
	el->data_type = type;
	el->data = data;
	SpinLockAcquire(&target_worker->slock);
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

	state_shm = PR_allocBuffer(Sizeof(XLogReaderState), true);
	memcpy(state_shm, state, sizeof(XLogReaderState));
	state_shm->record = record;
	PR_enqueue(state_shm, ReaderState, worker_idx);
}

XLogDispatchData_PR *
PR_allocXLogDispatchData(void)
{
	XLogDispatchData_PR *dispatch_data;
	Size	sz;
   
	sz = Sizeof(XLogDispatchData_PR)
		+ Sizeof((sizeof(bool) * num_preplay_workers))
		+ Sizeof((sizeof(int) * (num_preplay_workers + 1)));

	dispatch_data = (XLogDispatchData_PR *)PR_allocBuffer(sz, true);
	dispatch_data->worker_array = addr_forward(dispatch_data, Sizeof(XLogDispatchData_PR));
	dispatch_data->worker_list = addr_forward(dispatch_data->worker_array, Sizeof((sizeof(bool) * num_preplay_workers)));
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
	dispatch_data->reader->record = record;

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
		block_hash = blockHash(rnode.spcNode, rnode.dbNode, rnode.relNode, blk, num_preplay_workers - PR_BLK_WORKER_MIN_IDX);
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
 ****************************************************************************
 */

#define SizeAtTail(chunk)   (Size *)(addr_backward(addr_forward((chunk), (chunk)->size), Sizeof(Size)))
#define Chunk(buffer)		(PR_BufChunk *)(addr_backward((buffer), Sizeof(PR_BufChunk)))
#define Tail(buffer)		SizeAtTail(Chunk(buffer))
#define Buffer(chunk)		addr_forward(chunk, Sizeof(PR_BufChunk))

INLINE PR_BufChunk *
next_chunk(PR_BufChunk *chunk)
{
	return (PR_BufChunk *)addr_forward(chunk, chunk->size);
}

void *
PR_allocBuffer(Size sz, bool need_lock)
{
	PR_BufChunk	*new;
	Size		 chunk_sz;
	void		*wk;

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);

	if (pr_buffer->alloc_start == pr_buffer->alloc_end)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return retry_allocBuffer(sz, need_lock);
	}
	chunk_sz = sz + Sizeof(PR_BufChunk) + Sizeof(Size);
	if (addr_before(pr_buffer->alloc_start, pr_buffer->alloc_end))
	{
		new = alloc_chunk(chunk_sz, pr_buffer->alloc_start, pr_buffer->alloc_end);
		if (new == NULL)
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			return retry_allocBuffer(sz, need_lock);
		}
	}
	else
	{
		new = alloc_chunk(chunk_sz, pr_buffer->alloc_start, pr_buffer->tail);
		if (new)
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			return addr_forward(new, Sizeof(PR_BufChunk));
		}
		wk = pr_buffer->alloc_start;
		pr_buffer->alloc_start = pr_buffer->head;
		new = alloc_chunk(chunk_sz, pr_buffer->alloc_start, pr_buffer->alloc_end);
		if (new == NULL)
		{
			if (need_lock)
				SpinLockRelease(&pr_buffer->slock);
			pr_buffer->alloc_start = wk;
			return retry_allocBuffer(sz, need_lock);
		}
		return addr_forward(new, Sizeof(PR_BufChunk));
	}
	return NULL;
}


static void *
retry_allocBuffer(Size sz, bool need_lock)
{
	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	if (my_worker_idx == PR_READER_WORKER_IDX)
		pr_buffer->needed_by_reader = sz;
	else if (my_worker_idx == PR_DISPATCHER_WORKER_IDX)
		pr_buffer->needed_by_dispatcher = sz;
	else
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		elog(PANIC, "%s called by wrong worker, %d", __func__, my_worker_idx);
	}
	if (need_lock)
		SpinLockRelease(&pr_buffer->slock);
	PR_recvSync();
	return PR_allocBuffer(sz, need_lock);
}

/* Size is "buffer size", not chunk size */
void	*
PR_expandBuffer(void *buffer, Size newsz, bool need_lock)
{
	PR_BufChunk	*chunk,
				*next;

	Assert(buffer && Chunk(buffer)->magic == PR_BufChunk_Allocated);

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	chunk = Chunk(buffer);
	if (newsz <= chunk->size)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return buffer;
	}
	next = (PR_BufChunk *)addr_forward(chunk, chunk->size);
	if (addr_after_eq(next, pr_buffer->tail))
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return expandBuffer_new(buffer, newsz, need_lock); 
	}
	

			
	/* Koichi: WIP */
	return NULL;
}


static void *
expandBuffer_new(void *buffer, Size newsz, bool need_lock)
{
	return NULL;
}


void
PR_freeBuffer(void *buffer, bool need_lock)
{
	PR_BufChunk	*chunk;
	Size		 available;
	/* Do not forget to ping reader or dispatcher if there's free area available */
	/* Ping the reader worker only when dispatcher does not require the buffer */

	if (need_lock)
		SpinLockAcquire(&pr_buffer->slock);
	chunk = Chunk(buffer);
	if (chunk->magic != PR_BufChunk_Allocated)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		elog(PANIC, "Attempt to free wrong buffer.");
	}
	free_chunk(chunk);
	/* Check the available chunk size */
	if ((!pr_buffer->needed_by_reader) && (!pr_buffer->needed_by_dispatcher))
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return;
	}
	available = available_size(pr_buffer);
	if (available >= pr_buffer->needed_by_dispatcher)
	{
		pr_buffer->needed_by_dispatcher = 0;
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		PR_sendSync(PR_DISPATCHER_WORKER_IDX);
		return;
	}
	if (pr_buffer->needed_by_dispatcher)
	{
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		return;
	}
	if (available >= pr_buffer->needed_by_reader)
	{
		pr_buffer->needed_by_reader = 0;
		if (need_lock)
			SpinLockRelease(&pr_buffer->slock);
		PR_sendSync(PR_READER_WORKER_IDX);
		return;
	}
}

/* Caller must obtain pr_buffer->slock lock */
static Size
available_size(PR_buffer *buffer)
{
	Size	sz1,
			sz2;

	if (addr_before(buffer->alloc_start, buffer->alloc_end))
		return ((PR_BufChunk *)buffer)->size - Sizeof(PR_BufChunk) - Sizeof(Size);

	sz1 = addr_subtract(buffer->tail, buffer->alloc_start) - Sizeof(PR_BufChunk) - Sizeof(Size);
	sz2 = addr_subtract(buffer->alloc_end, buffer->head) - Sizeof(PR_BufChunk) - Sizeof(Size);

	if (sz1 > sz2)
		return sz1;
	else
		return sz2;
}

INLINE Size
buffer_size(void)
{
	return(PR_buf_size_mb * PR_MB);
}

static void
initBuffer(void)
{
	PR_BufChunk	*chunk;
	Size	*size_at_tail;

	Assert(pr_buffer);

	pr_buffer->area_size = buffer_size() - Sizeof(PR_buffer);
	pr_buffer->head = addr_forward(pr_buffer, Sizeof(PR_buffer));
	pr_buffer->tail = addr_forward(pr_buffer, buffer_size());
	pr_buffer->alloc_start = pr_buffer->head;
	pr_buffer->alloc_end = pr_buffer->tail;
	chunk = (PR_BufChunk *)pr_buffer->alloc_start;
	chunk->size = pr_buffer->area_size;
	chunk->magic = PR_BufChunk_Free;
	size_at_tail = SizeAtTail(chunk);
	*size_at_tail = chunk->size;
	SpinLockInit(&pr_buffer->slock);
}


/* The caller should acquire the lock pr_buffer->slock */
static PR_BufChunk *
prev_chunk(PR_BufChunk *chunk)
{
	Size *size_at_tail;

	if (chunk == (PR_BufChunk *)pr_buffer->head)
		return NULL;
	size_at_tail = addr_backward(chunk, Sizeof(Size));
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
	Size	*size_at_tail;

	Assert(pr_buffer && (chunk->magic == PR_BufChunk_Free));
	Assert(addr_after_eq(chunk, pr_buffer->head) && addr_before_eq(chunk, pr_buffer->tail));

	next = next_chunk(chunk);
	if (addr_after_eq(next, pr_buffer->tail))
		return;
	if (next->magic != PR_BufChunk_Free)
		return;
	chunk->size += next->size;
	size_at_tail = SizeAtTail(chunk);
	*size_at_tail = chunk->size;
	if (next == pr_buffer->alloc_start)
		pr_buffer->alloc_start = chunk;
}

/* Concatinate this free chunk with the previous one */
/* Caller must acquire the lock pr_block->slock */
static void
concat_prev_chunk(PR_BufChunk *chunk)
{
	PR_BufChunk *prev;
	Size	*size_at_tail;

	Assert(pr_buffer && (chunk->magic == PR_BufChunk_Free));
	Assert(addr_after_eq(chunk, pr_buffer->head) && addr_before_eq(chunk, pr_buffer->tail));

	if (addr_before_eq(chunk, pr_buffer->head))
		return;
	size_at_tail = SizeAtTail(chunk);
	prev = prev_chunk(chunk);
	if (prev->magic != PR_BufChunk_Free)
		return;
	prev->size += chunk->size;
	*size_at_tail = prev->size;
	if (chunk == pr_buffer->alloc_start)
		pr_buffer->alloc_start = prev;
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

	Assert(addr_before(start, end));

	available = addr_subtract(end, start);
	if (available < sz)
		return NULL;
	if ((available - sz) <= Sizeof(PR_BufChunk) + Sizeof(Size))
		sz = available;
	chunk = (PR_BufChunk *)start;
	chunk->size = sz;
	chunk->magic = PR_BufChunk_Allocated;
	next = next_chunk(chunk);
	if (available > sz)
	{
		Size *size_at_tail;

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
 ******************************************************************************************************
 */

static FILE *pr_debug_log = NULL;
static char	*pr_debug_signal_file = NULL;
static char	*debug_log_dir = NULL;
static char	*debug_log_file_path = NULL;
static char	*debug_log_file_link = NULL;
static int	 my_worker_idx;

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
	PRDebug_log("Now ready to attach the debugger to pid %d.  "
				"Set the break point to PRDebug_sync()\n"
			    "My worker idx is %d.\nPlease touch %s to begin.  "
				"I'm waiting for it.\n",
			getpid(), worker_idx, pr_debug_signal_file);
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
			break;
	}
	if (found_debug_file == true)
		PRDebug_log("Detected %s.  Can begin debug\n", pr_debug_signal_file);
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

	va_start(arg_ptr, fmt);
	vsprintf(buf, fmt, arg_ptr);
	elog(LOG, "%s%s", pr_debug_log_hdr, buf);
	fprintf(pr_debug_log, "%s%s", pr_debug_log_hdr, buf);
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


/*
 *******************************************************************************************************************************
 *
 * Parallel worker functions
 *
 *******************************************************************************************************************************
 */

/*
 *******************************************************************************************************************************
 * Invalid Page Worker
 *******************************************************************************************************************************
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
 *******************************************************************************************************************************
 * Block Worker
 *******************************************************************************************************************************
 */

static void
blockWorkerLoop(void)
{
	PR_queue_el	*el;
	XLogDispatchData_PR	*data;
	XLogRecord	*record;
	bool	*worker_array;
	
	for (;;)
	{
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != XLogDispatchData)
			elog(PANIC, "Invalid internal status for block worker.");
		data = (XLogDispatchData_PR *)(el->data);
		PR_freeQueueElement(el);

		SpinLockAcquire(&data->slock);

		data->n_remaining--;
		if (data->n_remaining > 0)
		{
			/* This worker is not eligible to handle this */
			SpinLockRelease(&data->slock);
			/* Wait another BLOCK worker to handle this and sync to me */
			PR_recvSync();
			continue;
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
			for (worker_array = data->worker_array; *worker_array > PR_READER_WORKER_IDX; worker_array++)
			{
				if (*worker_array != my_worker_idx)
				{
					PR_sendSync(*worker_array);
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
	}
	return;
}

/*
 *******************************************************************************************************************************
 * Transaction Worker
 *******************************************************************************************************************************
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
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != XLogDispatchData)
			elog(PANIC, "Invalid internal status for transaction worker.");
		dispatch_data = (XLogDispatchData_PR *)(el->data);
		record = dispatch_data->reader->record;
		xlogreader = dispatch_data->reader;
		txn_cell = isTxnSyncNeeded(dispatch_data, record, &xid, true);
		if (txn_cell)
		{
			/*
			 * Here waits until all the preceding WAL records for this trancation
			 * are handled by block workers.
			 */
			syncTxn(txn_cell);
			removeTxnCell(txn_cell);
		}
		/*
		 * Koichi: TBD
		 *		ここで replay して、txn ヒストリや XLogCtl のアップデートを行う
		 */
		/* Now apply the WAL record itself */
		RmgrTable[record->xl_rmid].rm_redo(xlogreader);
		
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

static txn_cell_PR *
isTxnSyncNeeded(XLogDispatchData_PR *dispatch_data, XLogRecord *record, TransactionId *xid, bool remove_myself)
{
	txn_cell_PR		*txn_cell;
	RmgrIds			 rmgr_id;
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
	rmgr_id = XLogRecGetRmid(dispatch_data->reader);
	/*
	 * Koichi:
	 *	おそらく、ここまで来る XLogRec は RM_XACT_ID しかないと思われるが、まだチョット心配
	 */
	if (rmgr_id != RM_XACT_ID)
		goto return_null;
	/*
	 * Koichi:
	 *	この後、command 調べて commit/abort/prepare なら txn_cellを返す
	 *  そうでなければ、txn_cell を解放する
	 */
	xact_info = XLogRecGetInfo(dispatch_data->reader) & XLOG_XACT_OPMASK;

	switch (xact_info)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_ABORT:
		case XLOG_XACT_PREPARE:
			return txn_cell;
	}

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

	/* Deallocate the trancaction cell */
	free_txn_cell(txn_cell, true);
}

/*
 *******************************************************************************************************************************
 * Dispatcher worker
 *******************************************************************************************************************************
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
		el = PR_fetchQueue();
		if (el == NULL)
			break;	/* Process termination request */
		if (el->data_type != ReaderState)
			elog(PANIC, "Invalid internal status for dispatcher worker.");
		/* Dequeue */
		PR_freeQueueElement(el);
		reader = (XLogReaderState *)el->data;
		record = reader->record;
		dispatch_data = PR_analyzeXLogReaderState(reader, record);
		dispatchDataToXLogHistory(dispatch_data);
		addDispatchDataToTxn(dispatch_data, false);
		for (worker_list = dispatch_data->worker_list; *worker_list > PR_DISPATCHER_WORKER_IDX; worker_list++)
			PR_enqueue(dispatch_data, XLogDispatchData, *worker_list);
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

