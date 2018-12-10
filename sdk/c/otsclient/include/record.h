#ifndef OTS_RESULT_H_
#define OTS_RESULT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"
#include "macros.h"

/**
 * Create ots_record_t object.
 * This record must be destroyed by calling ots_record_destroy().
 */
OTS_API int32_t
ots_record_create(
    ots_record_t *record_ptr);	  /* [out] record object*/

/**
 * Destroy ots_record_t object.
 */
OTS_API int32_t
ots_record_destroy(
    const ots_record_t record);	/* [in] record object*/

/**
 * Set hash key for ots_record_t object.if the table primary key type is hash,the range_key value is ignored,otherwise the range key must be given
 */
OTS_API int32_t
ots_record_set_primary_key(
	ots_record_t record,	    /* [in] record object*/
    const byte_t *hash_key,		/* [in] hash key */
	size_t hash_key_len,		/* [in] hash key length*/
	const byte_t *range_key,	/* [in] range key */
	size_t range_key_len);		/* [in] range key length*/

/**
 * Add cell for ots_record_t object,call this function multiple times to add all cells if the record
 */
OTS_API int32_t
ots_record_add_row_cell(
	ots_record_t record,	/* [in] record object*/
    ots_cell *cell);		/* [in] ots cell*/

/**
 * Returns the row key of this ots_record_t object.
 * This key_ptr is valid until ots_record_destroy() is called.
 * Callers should not modify this buffer.
 */
OTS_API int32_t
ots_record_get_key(
    const ots_record_t record,	  /* [in] record object which returned by calling ots_record_query_next*/
    ots_primary_key **key_ptr); /* [in] row key object*/

/**
 * Returns the array of pointers to constant ots_cell structures with the cells
 * of the record. The cells_ptr are valid until ots_record_destroy() is called. The
 * variable pointed by num_cells_ptr is set to the number of cells in the record.
 *
 * Calling this function multiple times for the same ots_record_t may return
 * the same buffers. Callers should not modify these buffers.
 */
OTS_API int32_t
ots_record_get_cells(
    const ots_record_t record,     /* [in]  ots record object*/
    ots_cell **cells_ptr,   /* [in, out] cells related with the same record row key */
    uint32_t *num_cells_ptr);      /* [in, out] cells number*/

/**
 * Frees any resources held by the ots_record_t object.
 */
OTS_API int32_t
ots_record_destroy(
				   ots_record_t record);	/* [in]  record to destroy*/

/**
 * Frees any resources held by the ots_primary_key object.
 */
OTS_API void 
ots_record_destroy_primary_key(
	ots_primary_key* key_ptr);			/* [in]  key to destroy*/

/**
 * Frees any resources held by the ots_cell object.
 */
OTS_API void 
ots_record_destroy_cells(
	ots_cell* sells_ptr,			/* [in]  cells to destroy*/
	uint32_t num_cells);			/* [in]  cell num to destroy*/

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* OTS_RESULT_H_*/
