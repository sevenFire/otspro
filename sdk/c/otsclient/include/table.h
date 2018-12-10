#ifndef OTS_TABLE_H_
#define OTS_TABLE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"

/**
 * Create a new ots table handle,the table handle is used by ots_admin_xx function.
 * all fields are initialized to the defaults,the table must be destroyed using ots_table_destroy 
 */
OTS_API int32_t
ots_table_create(
    ots_client_t ots_client,    /* [in] ots_client handle */
    ots_table_t *table_ptr);	/* [out] new table handle */

/**
 * Destroy the table releasing any internal objects
 */
OTS_API int32_t
ots_table_destroy(
    ots_table_t table);			/* [in] table handle */

/**
 * Set the table name
 */
OTS_API int32_t
ots_table_set_name(
	ots_table_t table,			/* [in] table handle */
	const char *table_name);	/* [in] table name */

/**
 * Set the table compression algorithm
 */
OTS_API int32_t
ots_table_set_compress_algorithm(
	ots_table_t table,					/* [in] ots table handle */
	OTS_COMPRESS_ALGORITHM algorithm);	/* [in] ots table algorithm */

/**
 * Set the table description
 */
OTS_API int32_t
ots_table_set_desc(
	ots_table_t table,              /* [in] ots table handle */
	const char * desc);				/* [in] Null terminated table description */

/**
 * Set the mob info,if the mob_enable is false,the threshold is ignored
 */
OTS_API int32_t
ots_table_set_mob_support(
	ots_table_t table,				/* [in] ots table handle */
	bool mob_enable,				/* [in] whether enable mob or not */
	uint32_t threshold);			/* [in] mob threshold (unit:kb),this parameter is valid when mob_enable is true */

/**
* Get the table name,the table_name are valid until ots_table_destroy() is called. 
* Don't free the table_name memory manually,it will be auto destroyed by call ots_table_destroy
 */
OTS_API int32_t
ots_table_get_name(
	ots_table_t table,					/* [in] ots table handle */
	const char **table_name);			/* [out] ots table name */	

/**
 * Get the table compression algorithm
 */
OTS_API int32_t
ots_table_get_compress_algorithm(
	ots_table_t table,					/* [in] ots table handle */
	OTS_COMPRESS_ALGORITHM *algorithm);	/* [out] ots table algorithm */			

/**
 * Get the table description,the desc are valid until ots_table_destroy() is called.
 * Don't free the desc memory manually,it will be auto destroyed by call ots_table_destroy
 */
OTS_API int32_t
ots_table_get_desc(
	ots_table_t table,					/* [in] ots table handle */
	const char **desc);					/* [out] ots table description */	

/**
 * Get the mob enable status
 */
OTS_API int32_t
ots_table_get_mob_enabled(
	ots_table_t table,				/* [in] ots table handle */
	bool *enabled                   /* [out] ots table enable status */
	);				

/**
 * Get the mob threshold
 */
OTS_API int32_t
ots_table_get_mob_threshold(
	ots_table_t table,				/* [in] ots table handle */
	uint32_t *threshold);			/* [out] ots table mob threshold */


#ifdef __cplusplus
}  // extern "C"
#endif

#endif /* OTS_TABLE_H_ */
