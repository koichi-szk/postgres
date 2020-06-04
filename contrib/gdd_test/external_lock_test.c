/*-------------------------------------------------------------------------
 *
 * external lock test functions
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"

#include "catalog/catalog.h"
#include "catalog/pg_type.h"

#include "funcapi.h"

#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/proc.h"

#include "utils/typcache.h"

/* Function definitions */
PG_FUNCTION_INFO_V1(pg_acquire_external_lock_tuple);
PG_FUNCTION_INFO_V1(pg_acquire_external_lock_array);
PG_FUNCTION_INFO_V1(pg_set_external_lock_tuple);
PG_FUNCTION_INFO_V1(pg_set_external_lock_array);
PG_FUNCTION_INFO_V1(pg_wait_external_lock);
PG_FUNCTION_INFO_V1(pg_unwait_external_lock);
PG_FUNCTION_INFO_V1(pg_release_exteranal_lock);
PG_FUNCTION_INFO_V1(pg_wait_external_lock_array);

static	bool parse_locktag_array(ArrayType *array, LOCKTAG *locktag);
static	bool parse_locktag_tuple(HeapTupleHeader rec, LOCKTAG *locktag);
static Datum locktag2Datum_tuple(LOCKTAG *locktag);
static Datum locktag2Datum_array(LOCKTAG *locktag);

/*
 * Create and acquire external lock, returns LOCKTAG
 *
 * Does not receive an argument.   Just acquire external
 * lock and returns its LOCKTAG as LOCKTAG type row.
 * In the case of failure, no row will be returned.
 */
Datum
pg_acquire_external_lock_tuple(PG_FUNCTION_ARGS)
{
	LOCKTAG	locktag;

	if (ExternalLockAcquire(MyProc, &locktag) == LOCKACQUIRE_OK)
	{
		/* Return LOCKTAG as LOCKTAG type row */
	}
	PG_RETURN_NULL();
}

Datum
pg_acquire_external_lock_array(PG_FUNCTION_ARGS)
{
	LOCKTAG		locktag;

	if (ExternalLockAcquire(MyProc, &locktag) == LOCKACQUIRE_OK)
		PG_RETURN_ARRAYTYPE_P(locktag2Datum_array(&locktag));
	else
		PG_RETURN_NULL();
}

/*
 * INPUT: INT array, four elements representing each locktag member value.
 * 		  dsn to the remote server
 *		  pid of the target transaction
 *		  pgprocno of the target transaction
 *		  transaction id of the target transaction
 * OUTPU: true: successful
 *		  false: failed
 */
Datum
pg_set_external_lock_array(PG_FUNCTION_ARGS)
{
	ArrayType		*locktag_array;
	LOCKTAG			 locktag;
	text			*dsn_text;
	char			*dsn;
	int				 target_pgprocno;
	int				 target_pid;
	TransactionId	 target_xid;
	bool			 rv;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Invalid null argument as LOCKTAG data");
	locktag_array = PG_GETARG_ARRAYTYPE_P(0);
	if (!parse_locktag_array(locktag_array, &locktag))
		elog(ERROR, "Error in locktag parsing.");
	/* Parse dsn */
	dsn_text = PG_GETARG_TEXT_P(1);
	dsn = pstrdup(dsn_text->vl_dat);
	/* Parse pid */
	target_pid = PG_GETARG_INT32(2);
	target_pgprocno = PG_GETARG_INT32(3);
	target_xid = PG_GETARG_INT32(4);

	rv = ExternalLockSetProperties(&locktag, MyProc,
				dsn,
				target_pgprocno, target_pid, target_xid,
				true);
	PG_RETURN_BOOL(rv == true ? true : false);
}

/*
 * INPUT: LOCKTAG record
 *		  dsn to the remote server
 *		  pid of the target transaction
 *		  pgprocno of the target transaction
 *        transaction id of the target transaction
 * OUTPUT: true: successful
 *		   false: failed
 */
Datum
pg_set_external_lock_tuple(PG_FUNCTION_ARGS)
{
	LOCKTAG			locktag;
	text		   *dsn_text;
	char		   *dsn;
	int				target_pgprocno;
	int				target_pid;
	TransactionId	target_xid;
	bool			rv;
	HeapTupleHeader htup;

	/* Parse LOCKTAG */
	htup = (HeapTupleHeader)PG_GETARG_HEAPTUPLEHEADER(0);
	if (!parse_locktag_tuple(htup, &locktag))
		elog(ERROR, "Error in parsing LOCKTAG data");
	/* Parse dsn */
	dsn_text = PG_GETARG_TEXT_P(1);
	dsn = pstrdup(dsn_text->vl_dat);
	/* Parse pid */
	target_pid = PG_GETARG_INT32(2);
	/* Parse pgprocno */
	target_pgprocno = PG_GETARG_INT32(3);
	/* Parse TXN no */
	target_xid = PG_GETARG_INT32(4);

	rv = ExternalLockSetProperties(&locktag, MyProc,
				dsn,
				target_pgprocno, target_pid, target_xid,
				true);
	PG_RETURN_BOOL(rv == true ? true : false);
}

/*
 * INPUT: LOCKTAG record
 * OUTPUT: true/false
 */
Datum
pg_wait_external_lock_array(PG_FUNCTION_ARGS)
{
	LOCKTAG		 locktag;
	ArrayType	*locktag_array;
	
	/* Parse input lock tag */
	locktag_array = PG_GETARG_ARRAYTYPE_P(0);
	if (!parse_locktag_array(locktag_array, &locktag))
		PG_RETURN_BOOL(false);

	/* Find lock tag */
	PG_RETURN_BOOL(ExternalLockWait(&locktag) ? true : false);
}

/*
 * Parse LOCKTAG input from functions.
 *
 * locktag_heaptuple can be obtained using PG_GETARG_HEAPTUPLEHEADER(n)
 */
static	bool
parse_locktag_tuple(HeapTupleHeader rec, LOCKTAG *locktag)
{
	int				ii;
	Oid				tupType;
	int32			tupTypemod;
	TupleDesc		tupdesc;
	HeapTupleData	tuple;
	int				ncolumns;
	Datum		   *values;
	bool		   *nulls;

	tupType = HeapTupleHeaderGetTypeId(rec);
	tupTypemod = HeapTupleHeaderGetTypMod(rec);
	tupdesc = lookup_rowtype_tupdesc_domain(tupType, tupTypemod, false);
	ncolumns = tupdesc->natts;
	/* Trivial check of input column */
	if (ncolumns != 4)
		elog(ERROR, "Invalid number of LOCKTAG column.");

	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	for (ii = 0; ii < 4; ii++)
	{
		if (nulls[ii] == true)
			elog(ERROR, "Invalid NULL column value in LOCKTAG type.");
	}
	locktag->locktag_field1 = DatumGetUInt32(values[0]);
	locktag->locktag_field2 = DatumGetUInt32(values[1]);
	locktag->locktag_field3 = DatumGetUInt32(values[2]);
	locktag->locktag_field3 = DatumGetUInt16(values[3]);
	locktag->locktag_type = LOCKTAG_EXTERNAL;
	locktag->locktag_lockmethodid = DEFAULT_LOCKMETHOD;
	return true;
}

/*
 * parse_locktag() の配列バージョン
 */
static bool
parse_locktag_array(ArrayType *array, LOCKTAG *locktag)
{

	Datum  *datum;
	int		n_array;

	n_array = array->ndim;
	if (n_array != 4)
		elog(ERROR, "Invalied array LOCKTAG array size.");

	datum = (Datum *)ARR_DATA_PTR(array);

	locktag->locktag_field1 = DatumGetUInt32(datum[0]);
	locktag->locktag_field2 = DatumGetUInt32(datum[1]);
	locktag->locktag_field3 = DatumGetUInt32(datum[2]);
	locktag->locktag_field3 = DatumGetUInt16(datum[3]);
	locktag->locktag_type = LOCKTAG_EXTERNAL;
	locktag->locktag_lockmethodid = DEFAULT_LOCKMETHOD;
	
	return true;
}

static Datum
locktag2Datum_array(LOCKTAG *locktag)
{
	int				 ii;
	uint32			 locktagdata[4];
	ArrayBuildState	*astate = NULL;

	locktagdata[0] = locktag->locktag_field1;
	locktagdata[1] = locktag->locktag_field2;
	locktagdata[2] = locktag->locktag_field3;
	locktagdata[3] = locktag->locktag_field4;

	for (ii = 0; ii < 4; ii++)
		astate = accumArrayResult(astate, (Datum)(locktagdata[ii]), 
										  false,
										  INT4OID,
										  CurrentMemoryContext);
	return(makeArrayResult(astate, CurrentMemoryContext));
}

#define SZ 32
static Datum
locktag2Datum_tuple(LOCKTAG *locktag)
{
	TupleDesc		 tupd;
	HeapTupleData	 tupleData;
	HeapTuple		 tuple = &tupleData;
	char			 value0[SZ], value1[SZ], value2[SZ], value3[SZ];
	char 			*values[4] = {value0, value1, value2, value3};

	tupd = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupd, 1, "locktag_field1", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 2, "locktag_field2", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 3, "locktag_field3", INT4OID, -1, 0);
	TupleDescInitEntry(tupd, 4, "locktag_field4", INT4OID, -1, 0);
	snprintf(values[0], SZ, "%d", locktag->locktag_field1);
	snprintf(values[1], SZ, "%d", locktag->locktag_field2);
	snprintf(values[2], SZ, "%d", locktag->locktag_field3);
	snprintf(values[3], SZ, "%d", locktag->locktag_field4);

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupd), values);
	return(TupleGetDatum(TupleDescGetSlot(tupd), tuple));
}
#undef SZ
