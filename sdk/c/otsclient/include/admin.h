#ifndef OTS_ADMIN_H_
#define OTS_ADMIN_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"
#include "table.h"

/**
 * Creates an ots table.
 * @returns 0 on success, an error code otherwise.
 */
OTS_API int32_t
ots_admin_create_table(
    const ots_client_t client,        /* [in] ots client handle */
    const ots_table_t table);		  /* [in] ots table */

/**
 * Disable an ots table.
 * @returns 0 on success, an error code otherwise.
 */
OTS_API int32_t
ots_admin_disable_table(
    const ots_client_t client,  /* [in] ots client handle */
    const char *table_name);	/* [in] Null terminated table name */

/**
 * Enable an ots table.
 * @returns 0 on success, an error code otherwise.
 */
OTS_API int32_t
ots_admin_enable_table(
    const ots_client_t client,  /* [in] ots client handle */
    const char *table_name);	/* [in] Null terminated table name */

/**
 * Get an ots table, disables the table if not already disabled.
 * Returns 0 on success, and error code otherwise.
 */
OTS_API int32_t
ots_admin_get_table(
    const ots_client_t client , /* [in] ots client handle */
    const char *table_name,		/* [in] Null terminated table name */
	ots_table_t *table_ptr);	/* [out] ots table handle */

/**
 * Get ots table list
 * Returns 0 on success, and error code otherwise.
 */
OTS_API int32_t
ots_admin_get_table_list(
    const ots_client_t client , /* [in] ots client handle */
 	ots_table_t *table_ptr,		/* [in,out] ots tables */
	uint32_t *num_ptr);			/* [in,out]  ots table count */

/**
 * Update ots table
 * Returns 0 on success, and error code otherwise.
 */
OTS_API int32_t
ots_admin_update_table(
	const ots_client_t client ,		/* [in] ots client handle */
	const ots_table_t *table_ptr);  /* [in] ots_table_handle */

/**
 * Deletes an ots table, disables the table if not already disabled.
 * Returns 0 on success, and error code otherwise.
 */
OTS_API int32_t
ots_admin_delete_table(
    const ots_client_t client , /* [in] ots client handle */
    const char *table_name);	/* [in] Null terminated table name */


/**
 * truncate ots table
 * Returns 0 on success, and error code otherwise.
 */
OTS_API int32_t
ots_admin_truncate_table(
	const ots_client_t client ,		/* [in] ots client handle */
	const char *table_name);		/* [in] Null terminated table name */
#ifdef __cplusplus
}  // extern "C"
#endif

#endif /* OTS_ADMIN_H_ */
