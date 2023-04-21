/*
 * parallel_replay.h
 *
 * Postgres parallel recovery struct and function definitions
 *
 * Portions Copyright (c) 2022 EDB
 * Portions Copyright (c) 2021 PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel_replay.h
 */
#ifndef PARALLEL_REPLAY_H
#define PARALLEL_REPLAY_H
#ifdef PR_IGNORE_REPLAY_ERROR
#include <setjmp.h>
extern	jmp_buf	pr_jmpbuf;
#endif

#include "postgres.h"

#include "lib/stringinfo.h"
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

/*
 * Flag to ignore error during parallel replay.
 */
#ifdef PR_IGNORE_REPLAY_ERROR
extern bool pr_during_redo;
#endif


#define MAX_PR_NUM_WORKERS	127			/* Max number of replay worker */

/*
 ************************************************************************************************
 *
 * Fundamental Macros
 *
 ************************************************************************************************
 */
#define size_boundary(s)    ((((s) + MemBoundary - 1) / MemBoundary) * MemBoundary)
#define address_boundary(s) (((((uint64)(s)) + MemBoundary - 1) / MemBoundary) * MemBoundary)
#define pr_sizeof(s)           size_boundary(sizeof(s))

/*
 ************************************************************************************************
 * 
 *	Sharared memory area
 *
 ************************************************************************************************
 */

typedef struct PR_shm		PR_shm;
typedef struct PR_invalidPages	PR_invalidPages;
typedef struct PR_XLogHistory		PR_XLogHistory;
typedef struct PR_XLogHistory_el	PR_XLogHistory_el;
typedef struct PR_worker	PR_worker;
typedef struct PR_queue		PR_queue;
typedef struct PR_queue_el	PR_queue_el;
typedef struct PR_buffer	PR_buffer;
typedef struct PR_RecChunk	PR_RecChunk;
#if 0
typedef struct XLogReaderState_PR	XLogReaderState_PR;
#endif
typedef struct XLogInvalidPageData_PR XLogInvalidPageData_PR;
typedef struct PR_BufChunk PR_BufChunk;

typedef struct PR_txn_info			PR_txn_info;
typedef struct PR_txn_hash_entry	PR_txn_hash_entry;
typedef struct PR_txn_cell			PR_txn_cell;
typedef struct PR_txn_cell_pool		PR_txn_cell_pool;
typedef struct XLogDispatchData_PR XLogDispatchData_PR;

/*
 * Shared memory for parallel replay
 */
extern PR_shm *pr_shm;

struct PR_shm
{
	PR_worker		*workers;
	PR_txn_info		*txn_info;
	PR_invalidPages	*invalidPages;
	PR_XLogHistory	*history;
	PR_queue	*queue;
	PR_buffer	*buffer;
	slock_t		shm_slock;			/* Spin lock for EndRecPtrand MinTimeLineID */
	slock_t		sync_lock;		/* Spin lock for PR_recvSync(). Protects worker->worker_waiting */
	pid_t		reader_pid;		/* Reader worker PID */
	bool		some_failed;	/* Indicates if some worker failed and exited. */
	XLogRecPtr	EndRecPtr;		/* Minimum EndRecPtr among workers */
	TimeLineID  MinTimeLineID;	/* Min Timeline ID among workers */
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
 *
 */
struct PR_XLogHistory
{
	PR_XLogHistory_el	*head;			/* Head of the history element */
	PR_XLogHistory_el	*tail;			/* Tail of the history element */
	unsigned long		 hist_count;	/* Count of the update of whole history */
	int					 num_whole_elements;	/* Whole number of elements */
	int					 num_elements;	/* Number of occupied elements.  Initial: 0. */
	slock_t		 slock;
};

/*
 * Num of element is num_preplay_worker_queue + 2
 *
 * This number can be insufficient though.  This is chaind in ring.
 */
struct PR_XLogHistory_el
{
	PR_XLogHistory_el	*next;
	XLogRecPtr	 		 curr_ptr;		/* from xlogreader->ReadRecPtr */
	XLogRecPtr	 		 end_ptr;		/* from xlogreader->EndRecPtr */
	TimeLineID	 		 my_timeline;	/* from xlogreader->timeline */
	unsigned long		 ser_no;		/* Ser no of this WAL */
	TransactionId		 xid;			/* xid of this WAL */
	bool		 		 replayed;
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
	bool	 	 wait_dispatch;	/* Flag to indicate the worker is waiting for xlogrec to handle */
								/* Dispatcher check this and sync. */
	bool		 worker_failed;	/* Indicates this worker failed and exited. */
	bool		 worker_waiting;	/* Flag to indicate the work is waiting for sync.  Protected by pr_shm->wait_flag_lock. */
	bool		 worker_terminated;	/* Indicates the workerr has successfully terminated. */
	XLogRecPtr	 assignedRecPtr;	/* Latest assigned XLOG record ptr. */
								/* Set by enqueue side worker. */
	XLogRecPtr	 handledRecPtr;	/* Last andled XLOG record ptr by this worker. */
								/* Set by fetchqueue side worker */
	int			 num_queued_el;	/* Number of queue element in the que */
	PR_queue_el	*head;			/* Dispatched queue head.   Pick queue element from here. */
	PR_queue_el	*tail;			/* Dispatched queue tail.   Append queue element after this. */
} PR_worker;

#define PR_worker_sz	(pr_sizeof(PR_worker) * num_preplay_workers)

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
 * Sync timeout between workers: at present 1sec.
 */
#define PR_SYNC_TIMEOUT_SEC		1
#define PR_SYNC_TIMEOUT_USEC	0


/*
 ************************************************************************************************
 *
 * Structure to keep track of XLogRecord for each transaction.   Needed to synchronize
 * replay for speciic transaction.
 *
 ************************************************************************************************
 */


struct PR_txn_info
{
	PR_txn_hash_entry	*txn_hash;		/* Hash */
	PR_txn_cell			*cell_pool;		/* Cell pool */
	PR_txn_cell			*cell_head;
	PR_txn_cell			*cell_tail;
	slock_t				 cell_slock;	/* Slock for cell pool */
	uint64				 free_in_pool;
	uint64				 total_available;
};

/*
 * Transaction hash entry.
 */
struct PR_txn_hash_entry
{
	slock_t		 slock;
	PR_txn_cell	*head;
	PR_txn_cell	*tail;
};

/*
 * Entry for each transaction
 */
struct PR_txn_cell
{
	PR_txn_cell			*next;
	PR_txn_cell			*prev;					/* Used only when the cell is from PR_txn_hash_entry */
	TransactionId		 xid;
	int					 blk_worker_to_wait;	/* Indicates id of the block worker which txn worker is waiting for. */
												/* If TXN worker is waiting for specific block worker to finish assigned LSN, */
												/* TXN worker specifies worker id of such BLOCK worker. (if not waiting for */
												/* any, */
												/*zero) If block worker replies WAL with lastXLogLSN, it checks if the BLOCK */
												/* worker is here. If so, then the BLOCK worker writes a synch to TXN worker. */
   												/* When TXN worker get sync, then it checks next BLOCK worker. */
												/* Protected with spinlock at PR_txn_hash_entry */
	XLogRecPtr			 lastXLogLSN[0];		/* LSN of last outstanding WAL record.  If WAL record of this LSN is replayed, */
												/* This i an array of LSN assigned to each BLOCK WORKER.  If a block worker */
												/* replays a WAL record with this LSN, then it will be reset to Invalid LSN (zero) */
												/* Nuber of meber is (num_preplay_workers - PR_BLK_WORKER_MIN_IDX) and is */
												/* determined at initialization */
												/* This LSN is set by the DIDPATCHER worker and reset by BLOCK worker. */
												/* Use XLogReaderState->ReadRecPtr for this value. */
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
struct XLogInvalidPageData_PR
{
	PR_invalidPageCheckCmd	cmd;
	RelFileNode	node;
	ForkNumber	forkno;
	BlockNumber	blkno;
	bool		present;
	Oid			dboid;
};

/*
 ************************************************************************************************
 *
 * XLog dispatch data.
 *
 * This is additional to XLogReaderState.   This is allocated in the buffer area
 *
 ************************************************************************************************
 */
/*
 * next and prev is used only for XLogRecord with valid xid.
 * Tey are set by DISPATCHER worker to non-NULL value.   If it's NULL, we don't need
 * lock txn_hash_el_PR->slock.
 */
struct XLogDispatchData_PR
{
	slock_t	 	 		 slock;
#if 0		/* Now synchronization is done using separate dispatched data */
	unsigned			 flags;		/* Flag,same as PR_worker->flags */
#endif
	XLogReaderState		*reader;	/* Allocated in the separated buffer area */
	PR_XLogHistory_el	*xlog_history_el;	/* XLog history to maintain XLogRecPtr */
	RmgrId				 rmid;
	uint8				 info;
	uint8				 rminfo;
	TransactionId		 xid;
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
	PR_QueueInit = 0,
	ReaderState,			/* XLogReaderState */
	XLogDispatchData,		/* XLogDispatchData_PR */
	InvalidPageData,		/* XLogInvalidPageData_PR */
	RequestSync,			/* Requesting sync */
	RequestSyncAll,			/* Requesting sync */
	RequestTerminate,		/* Request to terminate relevant workers */
	PR_QueueMAX_value
} PR_QueueDataType;

/*
 * To indicate there are no more XLogRec to replay,
 * queue element with xlogrec == NULL will be assigned.
 */
struct PR_queue_el
{
	PR_queue_el			*next;	/* Next element */
	PR_QueueDataType	 data_type;
	int					 source_worker;	/* Worker id which enqueued this element */
	void				*data;
};

#define PR_queue_el_sz	(pr_sizeof(queue_el) * num_preplay_worker_queue)

struct PR_queue
{
	slock_t		 slock;
	int			 num_queue_element;
	int			*wait_worker_list;	/* Allocated as a part of this struct */
	PR_queue_el	*element;
};

#define PR_queue_sz		(PR_queue_el_sz + (pr_sizeof(PR_queue) * num_preplay_worker_queue))




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
#ifdef WAL_DEBUG
	bool		 dump_opt;			/* Control to dump buffer area or not, control in debugging. */
	uint64		 update_sno;
#endif
	uint64		 curr_sno;			/* Serial No. for the chunk */
	void		*head;				/* Start of the area: Initialized and then static */
	void		*tail;				/* Next of the end of the area: Initialized and then static */
	void		*alloc_start;		/* Can allocate from here. */
	void		*alloc_end;			/* Can allocate up to here. */
	Size		*needed_by_worker;	/* Size of the buffer needed by each worker */
	void	   **allocated_buffer;	/* Allocated buffer for each worker, in response to needed_by_worker flag */
};

struct PR_BufChunk
{
	/*
	 * XLogRec begins this header and ends with the same information for backward search.
	 * XLogRec is stored between these.
	 */
	 
	Size		size;	/* Whole chunk size, including this header */
	uint64		magic;
	uint64		sno;	/* Serial number of the chunk. Cout from the start */
	char		data[0];
	/*
	 * The trailor appears after the data[].
	 * This is just Size and is the same value as
	 * size member.
	 * trailor is placed at the end of the chunk containing
	 * the size of the chunk.
	 * This helps to scan chunks backwards.
	 */
};


/* Magic number */
#define	PR_BufChunk_Allocated	0x0123456789ABCD00
#define	PR_BufChunk_Free		0x0123456789ABCDFF

/*
 ****************************************************************************
 * Test Codes
 ****************************************************************************
 */

#ifdef WAL_DEBUG
extern void PRDebug_init(bool force_init);
extern void PRDebug_start(int worker_idx);
extern void PRDebug_attach(void);
extern void PRDebug_sync(void);
extern void PRDebug_log(char *fmt, ...) __attribute__ ((format (printf, 1, 2)));
extern void PRDebug_out(StringInfo s);
extern void PRDebug_finish(void);
extern void PR_debug_buffer(void);
extern void PR_debug_buffer2(void);
extern void PR_debug_analyzeState(XLogReaderState *state, XLogRecord *record);
extern void PR_breakpoint_func(void);
extern void PR_error_here(void);
extern void PR_dump_buffer(const char *funcname, bool need_lock);
extern void PR_dump_chunk(void *buf, const char *funcname, bool need_lock);
extern void PR_dump_queue(bool need_lock);
extern void PR_dump_enqueue(const char *funcname, void *data, PR_QueueDataType type, int  worker_idx);
extern void PR_dump_fetchQueue(const char *funcname, PR_queue_el *el);
#endif

#ifdef WAL_DEBUG
#define PR_breakpoint()	PR_breakpoint_func()
#else
#define PR_breakpoint() do{}while(0)
#endif

/*
 ****************************************************************************
 * Global Function Definitions and Macros
 ****************************************************************************
 */

#define PR_isInParallelRecovery()	(InRecovery && pr_shm)
#define PR_needTestSync()			(parallel_replay && PR_test)

/*
 * Shared memory functions
 */
extern void PR_initShm(void);
extern void PR_finishShm(void);
extern void PR_initMq(void);
extern void PR_finishMq(void);

/*
 * History data function
 */
extern void PR_setXLogReplayed(PR_XLogHistory_el *el);
extern PR_XLogHistory_el
	*PR_addXLogHistory(XLogRecPtr currPtr, XLogRecPtr endPtr, TimeLineID myTimeline,
			long ser_no, TransactionId xid);

/* Worker functions */

extern void	PR_initWorker(PR_worker *worker, int n_worker);
extern void	PR_WorkerStartup(void);
extern void	PR_WorkerFinish(void);
extern void	ParallelRedoProcessMain(int idx);
extern void	PR_setWorker(int worker_idx);
extern char *PR_worker_name(int worker_idx, char *buff);
#if 0
extern char *PR_worker_name(int idx);
#endif
extern int	PR_myWorkerIdx(void);
extern PR_worker	*PR_myWorker(void);
extern void PR_WaitDispatcherQueueHandling(void);
extern void PR_atStartWorker(int idx);
void PR_failing(void);


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
extern void  PR_syncAll(bool terminate);
extern void  PR_sendSync(int worker_idx);
extern int	 PR_recvSync(void);
extern void	 PR_syncFinish(void);

/* Buffer functions */
extern void	*PR_allocBuffer(Size sz, bool need_lock);
extern void	 PR_freeBuffer(void *buffer, bool need_lock);
bool PR_isInBuffer(void *addr);
#ifdef WAL_DEBUG
extern bool	PR_bufferCheck(StringInfo s, void *addr, bool need_lock);
#endif


/* Queue functions */
extern void	PR_queue_init(void);
extern void	PR_initQueue(PR_queue *queue, int n_queue, int n_worker);
extern void	PR_enqueue(void *data, PR_QueueDataType type, int	worker_idx);
extern void	PR_freeQueueElement(PR_queue_el *el);
extern PR_queue_el	*PR_fetchQueue(void);

/* Dispatch functions */
#if 0
extern void  PR_dispatch(XLogDispatchData_PR *data, int worker_idx);
extern void  PR_dispatch_state(XLogReaderState_PR *state, int worker_idx);
#endif
extern void	 PR_enqueueXLogReaderState(XLogReaderState *state, XLogRecord *record, int worker_idx);
extern XLogDispatchData_PR	*PR_allocXLogDispatchData(void);
extern XLogDispatchData_PR	*PR_analyzeXLogReaderState(XLogReaderState *state, XLogRecord *record);
extern void	PR_setBlocks(XLogReaderState *shared, XLogReaderState *orig);

/* Miscellaneous */
extern int	PR_myWorkerIdx(void);
extern void	PR_setWorker(int worker_idx);

#endif /* PARALLEL_REPLAY_H */
