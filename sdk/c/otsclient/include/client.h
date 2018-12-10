#ifndef OTS_CLIENT_H_
#define OTS_CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "macros.h"
#include "types.h"

/**
 * Creates an ots client object,the ots client is always used as a global handle,
 * the following request depends on this object,after finished use,we must call
 * ots_client_destroy to destroy this object
 * @returns 0 on success, non-zero error code in case of failure.
 */
OTS_API int32_t
ots_client_create(
	const char *server_addr,			/* [in]  Null terminated server address*/
	const uint16_t port,				/* [in]  server port*/
	const uint32_t connection_timeout,  /* [in]  connection time out(unit:second)*/
	const uint32_t read_timeout,		/* [in]  read time out(unit:second)*/
	const uint32_t write_timeout,		/* [in]  write time out(unit:second)*/
    ots_client_t *client_ptr);			/* [out] client handle*/

/**
 * Destroy the ots client                                                                 
**/
OTS_API int32_t
ots_client_destroy(
	ots_client_t ots_client);			/* [in]  ots client handle*/

/**
 * Login to ots server,before we use ots_client,we must login to ots or will return user not login                                                             
**/
OTS_API int32_t
ots_client_login(
	ots_client_t ots_client,            /* [in]  ots client handle*/
	const char * user_name,             /* [in]  Null terminated ots user name*/
	const char * password);             /* [in]  Null terminated ots password*/			

/**
 * Creates a client side query. The returned query is not thread safe.
 * @returns 0 on success, non-zero error code in case of failure.
 */
OTS_API int32_t
ots_record_query_create(
    ots_client_t client,         /* [in] client handle*/
    ots_query_t* query_ptr);	 /* [out] query handle*/


/**
 * Request the next set of results from the server. You can set the maximum
 * number of rows returned by this call using ots_record_query_set_num_max_rows().
 * if the query type is OTS_QUERY_BY_ROW_KEY,all result will return at first
 * iterate and the maximum of rows returned setting by ots_record_query_set_num_max_rows() is invalid
 * @returns 0 on success, non-zero error code in case of failure.
 */
OTS_API int32_t
ots_record_query_next(
    ots_query_t query,					/* [in] query handle*/
	ots_record_t **records,				/* [out] query records*/
	size_t *num_records,				/* [out] the number of the records*/
	ots_query_iterator_t *iter_ptr);	/* [in,out] the iterator for the next query*/

/*
* Free queried records
*/
OTS_API int32_t
ots_free_records(
	ots_record_t* records,
	size_t num_records
);

/**
 * Destroy the query.
 */
OTS_API void
ots_record_query_destroy(
    ots_query_t query,				/* [in] query handle*/
	ots_query_iterator_t iter		/* [in] query iterator*/
);

/**
 * Set the query type for this query,the default query type for this query is OTS_QUERY_BY_PRIMARY_KEY
 */
OTS_API int32_t
ots_record_query_set_type(
	ots_query_t query,				/* [in] query handle*/
	OTS_QUERY_TYPE type);			/* [in] query type*/

/**
 * Set the table name for this query,when OTS_QUERY_TYPE is OTS_QUERY_BY_INDEX,the table name needn't set,otherwise the table name must be set
 */
OTS_API int32_t
ots_record_query_set_table(
    ots_query_t query,				/* [in] query handle */
    const char *table);				/* [in] table name */


/**
 * Specifies the primary key for this query,the hash_key must be set if only if the query type is OTS_QUERY_BY_PRIMARY_KEY
 */
OTS_API int32_t
ots_record_query_set_primary_key(
    ots_query_t query,					/* [in] query handle */
    const ots_primary_key *primary_key);	/* [in] hash key*/

/**
 * Specifies the range key for this query,the rang_key must be set if and only if the query type is OTS_QUERY_BY_RANGE_KEY_INTERVAL
 */
OTS_API int32_t
ots_record_query_set_range_key_interval(
    ots_query_t query,						 /* [in] query handle */
	const char *hash_key,					 /* [in] hash key value*/
	const size_t hash_key_length,			 /* [in] hash key value length */
    const char *range_key_start,			 /* [in] range key start value*/
    const size_t range_key_start_length,	 /* [in] range key start value length */
	const char *range_key_end,			     /* [in] range key end value*/
	const size_t range_key_end_length);	     /* [in] range key end value length */

/**
 * Specifies the range key prefix for this query,the rang_key_prefix must be set if the query type is OTS_QUERY_BY_RANGE_KEY_PREFIX
 */
OTS_API int32_t
ots_record_query_set_range_key_prefix(
    ots_query_t query,						/* [in] query handle */
	const char *hash_key,					 /* [in] hash key value*/
	const size_t hash_key_length,			 /* [in] hash key value length */
    const char *range_key_prefix,			/* [in] range key prefix*/
    const size_t range_key_prefix_length);  /* [in] range key prefix length */

/**
 * Add the retrieve column for this query,call this function multiple times to add the column needed retrieve,
 * if none of the column added, the all column and its value will be retrieved 
 */
OTS_API int32_t
ots_record_query_set_columns(
    ots_query_t query,				 /* [in] query handle */
    const char *columns);				 /* [in] columns name */

/**
 * Sets the maximum number of rows to retrieve per call to ots_record_query_next().
 */
OTS_API int32_t
ots_record_query_set_num_max_rows(
    ots_query_t query,				/* [in] query handle */
    const size_t cache_size);		/* [in] the maximum number of row to retrieve by an iterate */

/**
 * Specifies the index for this query,this parameter is valid only the OTS_QUERY_TYPE is OTS_QUERY_BY_INDEX
 */
OTS_API int32_t
ots_record_query_set_index(
    ots_query_t query,				/* [in] query handle */
    const char *index_name);		/* [in] query index name */

/**
 * Specifies the index condition for this query,this parameter is valid only the OTS_QUERY_TYPE is OTS_QUERY_BY_INDEX
 */
OTS_API int32_t
ots_record_query_set_index_condition(
    ots_query_t query,				/* [in] query handle */
    const char *condition);			/* [in] query index condition */

/**
 * insert or update a record,if the record row exist in ots, the ots record is updated by this record,
 * otherwise the record is inserted to ots.
 * @returns 0 on success, non-zero error code in case of failure.
 */
OTS_API int32_t
ots_record_insert_or_update(
	ots_client_t ots_client,		/* [in] client handle*/
	const char *table_name,		    /* [in] Null terminated table name */
    ots_record_t *records,			/* [in] ots records */
	uint32_t record_num				/* [in] ots records number*/
	);

/**
 * delete record by given row keys.
 * @returns 0 on success, non-zero error code in case of failure.
 */
OTS_API int32_t
ots_record_delete_by_primary_keys(
    ots_client_t *ots_client,				/* [in] client handle*/
	const char *table_name,					/* [in] Null terminated table name */
	const ots_primary_key *primary_keys,    /* [in] primary keys for delete*/
	uint32_t primary_keys_num);				/* [in] primary keys number*/

/**
 * delete record by row key range(the range is [start_row, end_row)).
 * @returns 0 on success, non-zero error code in case of failure.
 */
OTS_API int32_t
ots_record_delete_by_range_key_interval(
    ots_client_t *ots_client,		/* [in] client handle*/
	const char *table_name,			/* [in] Null terminated table name */
	const byte_t *hash_key,					 /* [in] hash key value*/
	const size_t hash_key_length,			 /* [in] hash key value length */
	const char *range_key_start,	/* [in] start row */
	const size_t range_key_start_length,	/* [in] start row length */
	const char *range_key_end,			/* [in] end row */
	const size_t range_key_end_length);	/* [in] end row length*/

#ifdef __cplusplus
} /* extern "C" */

#endif

#endif  /* OTS_CLIENT_H_*/
