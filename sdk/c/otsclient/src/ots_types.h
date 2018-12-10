#ifndef _OTS_TYPES_H_
#define _OTS_TYPES_H_
#include "types.h"

#define OTS_CLIENT_COLUMN_CACHE_DEF_SIZE	16

typedef struct _ots_record {
	char* hash_key;
	size_t hash_key_len;
	char* range_key;
	size_t range_key_len;
	ots_cell* cells;
	int cell_num;
	int cell_buf_size;
} ots_record;

/*
*ots client
*/
typedef struct _ots_client {
	char server_addr[IP_LEN];
	uint16_t port;
	uint32_t connection_timeout;
	uint32_t read_timeout;
	uint32_t write_timeout;
	char user_name[USER_NAME_LEN];
	char password[PASSWORD_LEN];
} ots_client;

typedef struct _ots_table {
	char name[TABLE_NAME_LEN];
	char desc[TABLE_DESC_LEN];
	OTS_COMPRESS_ALGORITHM compress_algorithm;
	byte_t mob_enabled;
	uint32_t mob_threshold;
	byte_t table_enabled;
	uint32_t max_version;
} ots_table;

typedef struct _ots_query {
	OTS_QUERY_TYPE type;
	char table_name[TABLE_NAME_LEN];
	char index_name[TABLE_NAME_LEN];
	ots_primary_key* row_key;
	char* start_key;
	size_t start_key_len;
	char* end_key;
	size_t end_key_len;
	size_t limits;
	char* key_prefix;
	size_t key_prefix_len;
	char* index_query_cond;
	size_t index_query_cond_len;
	char* columns;
	ots_client* client;
} ots_query;
#endif