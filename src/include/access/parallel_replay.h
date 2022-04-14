/*
 * parallel_replay.h
 *
 * Postgres parallel recovery struct and function definitions
 *
 * Portions Copyright (c) 2021 PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel_replay.h
 */
#ifndef PARALLEL_REPLAY_H
#define PARALLEL_REPLAY_H

#include "postgres.h"

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/spin.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"

/*
 * Parallel replay configuration to be taken from GUC
 */
extern bool	parallel_replay;
extern int	num_preplay_workers;		/* Number of workers, must be >=2 and one of them
									 	 * is for common resources, including TXN */
extern int	num_preplay_worker_queue;	/* Total queue size */
extern int	num_preplay_max_txn;		/* If less than max_connections, max_connections will be taken */
extern int	PR_buf_size_mb;				/* Buffer size in MB (2^^10)  */
extern bool PR_test;					/* Option to sync to the debugger */

#define MAX_PR_NUM_WORKERS	127			/* Max number of replay worker */


/*
 ************************************************************************************************
 * 
 *	Sharared memory area
 *
 ************************************************************************************************
 */

typedef struct PR_shm		PR_shm;
typedef struct PR_invalidPages	PR_invalidPages;
typedef struct PR_history	PR_history;
typedef struct PR_hist_el	PR_hist_el;
typedef struct PR_worker	PR_worker;
typedef struct PR_queue		PR_queue;
typedef struct PR_queue_el	PR_queue_el;
typedef struct PR_buffer	PR_buffer;
typedef struct PR_queue_el	PR_queue_el;
typedef struct PR_RecChunk	PR_RecChunk;
typedef struct XLogReaderState_PR	XLogReaderState_PR;

typedef struct txn_wal_info_PR	txn_wal_info_PR;
typedef struct txn_hash_el_PR	txn_hash_el_PR;
typedef struct txn_cell_PR		txn_cell_PR;
typedef struct txn_cell_pool_PR	txn_cell_pool_PR;
typedef struct XLogDispatchData_PR XLogDispatchData_PR;

struct PR_shm
{
	PR_worker	*workers;
	txn_wal_info_PR	*txn_wal_info;
	PR_invalidPages	*invalidPages;
	PR_history	*history;
	PR_queue	*queue;
	PR_buffer	*buffer;
	slock_t		slock;			/* Spin lock for EndRecPtr and MinTimeLineID */
	XLogRecPtr	EndRecPtr;		/* Minimum EndRecPtr among workers */
	TimeLineID  MinTimeLineID;	/* Min Timeline ID among workers */
	XLogRecPtr	*wk_EndRecPtr;
	TimeLineID	*wk_TimeLineID;
};

/*
 * Keep track of invalid pages found in replay.
 */
struct PR_invalidPages
{
	slock_t	slock;
	bool	invalidPageFound;
};

/*
 * Keep track of outstanding WAL record.
 *
 * This is ring-shaped link of the element.
 * If all the elements from head to some point is replayed,
 * this is reflected to XCloCtl.
 */
struct PR_history
{
	PR_hist_el	*hist_head;
	PR_hist_el	*hist_end;	/* Last + 1 */
	slock_t		 slock;
};

/*
 * Num of element is num_preplay_worker_queue.
 */
struct PR_hist_el
{
	XLogRecPtr	 curr_ptr;
	XLogRecPtr	 end_ptr;
	TimeLineID	 my_timeline;
	bool		 replayed;
	PR_hist_el	*next;
};


/*
 ************************************************************************************************
 * 
 * Worker Area
 *
 * Each Replay worker information.
 *
 ************************************************************************************************
 */ 


typedef struct PR_worker
{
	uint16	 	worker_idx;		/* Initialized by READER WORKER when the worker process is forked. */
	pid_t	 	worker_pid;		/* Initialized by READER WORKER when the worker process is forked. */
	slock_t	 	slock;			/* TXN and other BLK worker need to read worker status */
								/* Dispatcher uses this lock to assign new XLogRec */
								/* Can be spinlock */
	XLogRecPtr 	finishedLSN;	/* Just finished LSN */
	XLogRecPtr 	waitLSN;		/* TXN worker is waiting for this LSN to finish */
	bool	 	wait_dispatch;	/* Flag to indicate the worker is waiting for xlogrec to handle */
								/* Dispatcher check this and sync. */
	unsigned	flags;			/* Indicates instructions from outside */
	PR_queue_el	*head;			/* Dispatched queue head.   Pick queue element from here. */
	PR_queue_el	*tail;			/* Dispatched queue tail.   Append queue element after this. */
} PR_worker;

#define PR_worker_sz	(Sizeof(PR_worker) * num_preplay_workers)

/* Flag values */
/*
 * In addition to the flag values below, each BLK worker process need to sync
 * to others if XLogRecord is assigned to more than one worker.
 * This happens if XLogRecord contains multiple block update image.
 */
/*
 * Following flags work as follows:
 *	When each worker handles all the assigned queues and no queue is assigned,
 *	each worker checks flags.
 *  If PR_WK_TERMINATE is set, then the worker terminates.
 *  If PR_WK_SYNC_READER is set, then the worker writes sync message to READER worker.
 *  If PR_WK_SYNC_DISPATCHER is set, then the worker writes sync message to DISPATCHER worker.
 *  If PR_WK_SYNC_TXN is set, then the worker writes sync message to TXN worker.
 */

#define PR_WK_TERMINATE			0x00000001	/* Instruction to terminate the worker process */
#define PR_WK_SYNC_READER		0x00000002	/* Sync to the READER worker needed */
#define PR_WK_SYNC_DISPATCHER	0x00000004	/* Sync to the DISPATCHER worker needed */
#define PR_WK_SYNC_TXN			0x00000008	/* Sync to the TXN worker needed */

/* Worker Idx */
/*
 * READER worker is usual backends performing single-thread redo.
 * BLOCK worker idx value is assigned from three.
 */
#define	PR_READER_WORKER_IDX		0
#define PR_DISPATCHER_WORKER_IDX	1
#define PR_TXN_WORKER_IDX			2
#define PR_INVALID_PAGE_WORKER_IDX	3
#define PR_BLK_WORKER_MIN_IDX		4
#define PR_IS_BLK_WORKER_IDX(i)		((i) >= PR_BLK_WORKER_MIN_IDX)


/*
 ************************************************************************************************
 *
 * Structure to keep track of XLogRecord for each transaction.   Needed to synchronize
 * replay for speciic transaction.
 *
 ************************************************************************************************
 */


struct txn_wal_info_PR
{
	txn_hash_el_PR		*txn_hash;		/* Hash */
	txn_cell_pool_PR	*cell_pool;		/* Cell pool */
};

/*
 * Transaction hash entry.
 */
struct txn_hash_el_PR
{
	slock_t		 slock;
	txn_cell_PR	*head;
	txn_cell_PR	*tail;
};

/*
 * Entry for each transaction
 */
struct txn_cell_PR
{
	TransactionId		 xid;
	txn_cell_PR			*next;
	XLogDispatchData_PR *head;
	XLogDispatchData_PR *tail;
};

struct txn_cell_pool_PR
{
	slock_t		 slock;
	txn_cell_PR	*next;
};

/*
 ************************************************************************************************
 *
 * Invalid page registration
 *
 ************************************************************************************************
 */
typedef enum PR_invalidPageCheckCmd
{
	PR_LOG,
	PR_FORGET_PAGES,
	PR_FORGET_DB,
	PR_CHECK_INVALID_PAGES,
	MAXVALUE
} PR_invalidPageCheckCmd;

/*
 * Invalid page interface data
 */
typedef struct XLogInvalidPageData_PR
{
	PR_invalidPageCheckCmd	cmd;
	RelFileNode	node;
	ForkNumber	forkno;
	BlockNumber	blkno;
	bool		present;
	Oid			dboid;
} XLogInvalidPageData_PR;

/*
 ************************************************************************************************
 *
 * XLog dispatch data.
 *
 * This is additional to XLogReaderState.   This is allocated in the buffer area
 *
 ************************************************************************************************
 */
struct XLogDispatchData_PR
{
	slock_t	 	 slock;
	XLogReaderState	*reader;		/* Allocated in the separated buffer area */
	PR_hist_el	*history_el;		/* PTR to history data */
	XLogDispatchData_PR	*next;		/* Chain in txh_cell_PR */
	XLogDispatchData_PR	*prev;		/* Chain in txh_cell_PR */
	TransactionId	xid;
	bool	 	 txn_waiting;		/* Indicates that TXN worker is waiting for this dispatch to be done */
	bool	 	 txn_registered;	/* Indicates that TXN worker registered this dispatch */
	bool		 done;				/* Indicates that this dispatch has been replayed */
	int		 	 n_remaining;		/* If this becomes zero, then this worker should replay */
	int		 	 n_involved;		/* Total number of BLK workers assigned */
	int			*worker_list;		/* Allocated as a part of this struct */
	bool		*worker_array;		/* Allocated as a part of this struct */
};

/* Dispatch Data Functions */

extern XLogDispatchData_PR *PR_alloc_XLogDispatchData_PR(void);
extern void			     	PR_free_XLogDispatchData_PR(XLogDispatchData_PR *dispatch_data);

/*
 ************************************************************************************************
 * 
 * Replay Queue and queue area
 *
 ************************************************************************************************
 */ 

/*
 * After XLogReader reads XLogRec into PReplayShm.xlogrec_area, it creates a queue element
 * pointing to this XLogRec area and assigns to the dispatcher worker.  Then dispatcher
 * assigns this queue element to specific workers.
 *
 * If assigned XLogRec updates more than one block, this queue element will be copied for
 * remaining workers and assigned so that each worker can handle its own queue independently
 * from other workers.
 */

typedef enum PR_QueueDataType
{
	Init = 0,
	ReaderState,			/* XLogReaderState */
	XLogDispatchData,		/* XLogDispatchData_PR */
	InvalidPageData,		/* XLogInvalidPageData_PR */
	MAX_value
} PR_QueueDataType;

/*
 * To indicate there are no more XLogRec to replay,
 * queue element with xlogrec == NULL will be assigned.
 */
struct PR_queue_el
{
	PR_queue_el			*next;	/* Next element */
	PR_QueueDataType	 data_type;
	void				*data;
};

#define PR_queue_el_sz	(Sizeof(queue_el) * num_preplay_worker_queue)

struct PR_queue
{
	slock_t		 slock;
	int			 num_queue_element;
	int			*wait_worker_list;	/* Allocated as a part of this struct */
	PR_queue_el	*element;
};

#define PR_queue_sz		(PR_queue_el_sz + (Sizeof(PR_queue) * num_preplay_worker_queue))




/*
 ************************************************************************************************
 * 
 * Buffer Area
 *
 ************************************************************************************************
 */ 


/*
 * Overall XlogRec area, appear in global shared memory for parallel replay once.
 *
 * Initially alloc_start == head and alloc_end == tail.  When memory is allocated, alloc_start advances.
 * When memory is freed, alloc_end advabces too.   Please note that both goes back to head when it attempts
 * to go beyond tail.  Therefore, there can be both cases, alloc_start <= allocend and alloc_start > alloc_end.
 *
 * Because memory is allocated in head -> tail direction and is allocated in the order of LSN, even though
 * no area is available, xlog is consumed basically in the address order and will be eventually freed.
 * Therefore, allocator can wait until WAL is replayed and consumed so that alloc_end is updated and there's
 * new area avaiable.
 *
 * Buffer allocation can be done either by READER or DISPATCHER worker.
 * DISPATCHER worker will be allocated buffer first.
 *
 * Also the data will be consumed by TXN/BLOCK worker and and released.
 *
 * Therefore, if no space is available, we can wait they are consumed and
 * released.
 *
 * If buffer is requred by reader or dispatcher, assignment to dispatcher
 * will be done first, to accerelate buffer consumption.
 */

struct PR_buffer
{
	Size		 area_size;			/* Exluding this buffer */
	slock_t		 slock;				/* For allocation/free operation */
	void		*head;				/* Start of the area: Initialized and then static */
	void		*tail;				/* Next of the end of the area: Initialized and then static */
	void		*alloc_start;		/* Can allocate from here. */
	void		*alloc_end;			/* Can allocate up to here. */
	Size		 needed_by_reader;	/* Size of the buffer (not chunk) required by reader worker. */
	Size		 needed_by_dispatcher;	/* Size of the buffer (not chunk) required by dispatcher worker. */
	char		 data[0];			/* Assiciated data */
};

typedef struct PR_BufChunk
{
	/*
	 * XLogRec begins this header and ends with the same information for backward search.
	 * XLogRec is stored between these.
	 */
	 
	Size		size;	/* Whole chunk size */
	uint64		magic;
	char		data[0];
	/*
	 * The trailor appears after the data[].
	 * This is just Size and is the same value as
	 * size member.
	 * trailor is placed at the end of the chunk containing
	 * the size of the chunk.
	 * This helps to scan chunks backwards.
	 */
}PR_BufChunk;


/* Magic number */
#define	PR_BufChunk_Allocated	0x0123456789ABCD00
#define	PR_BufChunk_Free		0x0123456789ABCDFF

/*
 ****************************************************************************
 * Test Codes
 ****************************************************************************
 */

extern void PRDebug_init(bool force_init);
extern void PRDebug_start(int worker_idx);
extern void PRDebug_attach(void);
extern void PRDebug_sync(void);
extern void PRDebug_log(char *fmt, ...) __attribute__ ((format (printf, 1, 2)));
extern void PRDebug_finish(void);
extern bool	PR_needTestSync(void);

/*
 ****************************************************************************
 * Global Function Definitions
 ****************************************************************************
 */

/*
 * Shared memory functions
 */
extern void PR_initShm(void);
extern void PR_finishShm(void);

/*
 * History data function
 */
extern void PR_setXLogReplayed(PR_hist_el *el);
extern PR_hist_el *PR_addXLogHistory(XLogRecPtr currPtr, XLogRecPtr endPtr, TimeLineID my_timeline);

/* Worker functions */

extern void	PR_initWorker(PR_worker *worker, int n_worker);
extern void	PR_WorkerStartup(void);
extern void	PR_WorkerFinish(void);
extern void	ParallelRedoProcessMain(int idx);
extern void	PR_setWorker(int worker_idx);
extern int	PR_myWorkerIdx(void);
extern PR_worker	*PR_myWorker(void);
extern void PR_WaitDispatcherQueueHandling(void);

/* Invalid Page Worker functions */
extern void PR_log_invalid_page(RelFileNode node, ForkNumber forkno, BlockNumber blkno, bool present);
extern void PR_forget_invalid_pages(RelFileNode node, ForkNumber forkno, BlockNumber minblkno);
extern void PR_forget_invalid_pages_db(Oid dbid);
extern bool PR_XLogHaveInvalidPages(void);
extern void PR_XLogCheckInvalidPages(void);

/* Synchronization functions */
extern void	 PR_syncInitSockDir(void);
extern void	 PR_syncFinishSockDir(void);
extern void  PR_syncInit(void);
extern void  PR_syncAll(void);
extern void  PR_sendSync(int worker_idx);
extern int	 PR_recvSync(void);
extern void	 PR_syncFinish(void);

/* Buffer functions */
extern void	*PR_allocBuffer(Size sz, bool need_lock);
extern void	*PR_expandBuffer(void *buffer, Size newsz, bool need_lock);
extern void	 PR_freeBuffer(void *buffer, bool need_lock);

/* Queue functions */
extern void	PR_queue_init(void);
extern void	PR_initQueue(PR_queue *queue, int n_queue, int n_worker);
extern void	PR_enqueue(void *data, PR_QueueDataType type, int	worker_idx);
extern void	PR_freeQueueElement(PR_queue_el *el);
extern PR_queue_el	*PR_fetchQueue(void);

/* Dispatch functions */
extern void  PR_dispatch(XLogDispatchData_PR *data, int worker_idx);
extern void  PR_dispatch_state(XLogReaderState_PR *state, int worker_idx);
extern void	 PR_enqueueXLogReaderState(XLogReaderState *state, XLogRecord *record, int worker_idx);
extern XLogDispatchData_PR	*PR_allocXLogDispatchData(void);
extern XLogDispatchData_PR	*PR_analyzeXLogReaderState(XLogReaderState *state, XLogRecord *record);

/* Miscellaneous */
extern int	PR_myWorkerIdx(void);
extern void	PR_setWorker(int worker_idx);
extern bool PR_isInParallelRecovery(void);

#endif /* PARALLEL_REPLAY_H */
