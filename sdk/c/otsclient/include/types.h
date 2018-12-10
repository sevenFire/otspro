#ifndef OTS_TYPES_H_
#define OTS_TYPES_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef byte_t
#undef byte_t
#endif
typedef uint8_t byte_t;

#define IP_LEN			16
#define USER_NAME_LEN	32
#define PASSWORD_LEN	64
#define TABLE_NAME_LEN	32
#define TABLE_DESC_LEN	256
#define KEY_EXP_LEN		256
#define MAX_COLUMN_NUM	16

typedef enum{
	OTS_COMPRESS_NONE = 0,
	OTS_COMPRESS_GZIP,
	OTS_COMPRESS_LZ4,
	OTS_COMPRESS_SNAPPY
}OTS_COMPRESS_ALGORITHM;

typedef enum{
	OTS_QUERY_BY_PRIMARY_KEY = 0,
	OTS_QUERY_BY_RANGE_KEY_INTERVAL,
	OTS_QUERY_BY_RANGE_KEY_PREFIX,
	OTS_QUERY_BY_INDEX,
	OTS_QUERY_BY_INDEX_TABLE
}OTS_QUERY_TYPE;

/**
 * ots cell.
 */
typedef struct _ots_cell {
  /* column */
  char *column;
  size_t  column_len;

  /* column value */
  byte_t *value;
  size_t  value_len;
} ots_cell;

/**
 * ots primary key.
 */
typedef struct _ots_primary_key{
	char *hash_key;
	size_t  hash_key_len;
	char *range_key;
	size_t  range_key_len;
}ots_primary_key;

typedef void *ots_client_t;
typedef void *ots_table_t;
typedef void *ots_query_iterator_t;
typedef void *ots_record_t;
typedef void *ots_query_t;

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* OTS_TYPES_H_ */
