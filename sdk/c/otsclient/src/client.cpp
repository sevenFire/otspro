#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "types.h"
#include "client.h"
#include "error_code.h"
#include "string.h"
#include "cJSON.h"
#include "common.h"
#include "record.h"
#include "http_curl.h"
#include "iterator.h"

int32_t ots_client_create
( const char *server_addr, const uint16_t port, const uint32_t connection_timeout, 
  const uint32_t read_timeout, const uint32_t write_timeout, ots_client_t *client_ptr )
{
	if(NULL == server_addr || strlen(server_addr) > IP_LEN - 1|| NULL == client_ptr)
		return EC_OTSCLIENT_INVALID_ARGUMENT;
	ots_client* client = (ots_client*)ots_malloc(sizeof(ots_client));
	if (!client)
		return EC_OTSCLIENT_MALLOC_FAILED;

	client->port = port;
	memcpy(client->server_addr, server_addr, IP_LEN-1);
	client->connection_timeout = connection_timeout;
	client->read_timeout = read_timeout;
	client->write_timeout = write_timeout;

	*client_ptr = (ots_client_t)client;
	return EC_SUCCESS;
}


int32_t ots_client_destroy( ots_client_t client_handle )
{
	if(NULL == client_handle)
		return EC_OTSCLIENT_INVALID_ARGUMENT;

	ots_free(client_handle);
	return EC_SUCCESS;
}

int32_t ots_client_login
( ots_client_t client_handle, const char * user_name, const char * password )
{
	ots_client* client = (ots_client*) client_handle;
	char url[OTS_URL_LEN]={0};
	CHttpCurl c;
	std::string resGet;
	cJSON *json;

	if (NULL == client)
		return EC_OTSCLIENT_CLIENT_NOT_CREATE;
	if(NULL == user_name || strlen(user_name) > USER_NAME_LEN -1)
		return EC_OTSCLIENT_INVALID_ARGUMENT;

	if(NULL ==password || strlen(password) > PASSWORD_LEN - 1 )
		return EC_OTSCLIENT_INVALID_ARGUMENT;

	sprintf(url, "%s%s:%d%s/token", OTS_URL_HEAD, client->server_addr, client->port, OTS_URL_API);

	c.SetUserName(user_name);
	c.SetPassword(password);

	c.Get(url, resGet);

	json = cJSON_Parse(resGet.c_str());
	if (!json) {
		printf("Error before: [%s]\n",cJSON_GetErrorPtr());
		return EC_OTSCLIENT_NO_AUTHORITY;
	} else {
		int error_code = cJSON_GetObjectItem(json, "errcode")->valueint;
		if (EC_SUCCESS != error_code)
		{
			printf("Get authority failed, error code: %d\n", error_code);
			return EC_OTSCLIENT_NO_AUTHORITY;
		}
		std::string token = cJSON_GetObjectItem(json, "token")->valuestring;
		printf("login success with token: %s\n", token.c_str());
		memcpy(client->user_name, user_name, USER_NAME_LEN);
		memcpy(client->password, password, PASSWORD_LEN);
		cJSON_Delete(json);//!!important
	}
	return EC_SUCCESS;
}

int32_t ots_record_query_create( ots_client_t handle, ots_query_t* query_ptr )
{
	ots_client* client = (ots_client*) handle;
	if (NULL == client)
		return EC_OTSCLIENT_CLIENT_NOT_CREATE;
	if(0 == client->user_name[0])
		return EC_OTSCLIENT_CLIENT_NOT_LOGIN;
	ots_query* query = (ots_query*) ots_malloc(sizeof(ots_query));
	if (NULL == query)
		return EC_OTSCLIENT_MALLOC_FAILED;
	query->client = client;
	*query_ptr = (ots_query_t) query;
	return EC_SUCCESS;
}

int32_t ots_record_query_next
( ots_query_t query_handle, ots_record_t **records, size_t *num_records,
  ots_query_iterator_t *iter_ptr )
{
	ots_query* query = (ots_query*) query_handle;
	if (NULL == query || NULL == records || NULL == num_records ||NULL == iter_ptr)
		return EC_OTSCLIENT_NULL_POINTER;

	if (NULL != *iter_ptr)
	{
		QueryIterator* qiter = static_cast<QueryIterator*>(*iter_ptr);
		if (NULL == qiter)
			return EC_OTSCLIENT_NULL_POINTER;
		return qiter->FetchNext((void**)records, num_records);
	}
	else
	{
		QueryIterator* qiter = new QueryIterator(query);
		*iter_ptr = static_cast<ots_query_iterator_t>(qiter);
		if (NULL == qiter)
			return EC_OTSCLIENT_NULL_POINTER;
		return qiter->FetchNext((void**)records, num_records);
	}
	
	return EC_SUCCESS;
}

int32_t ots_free_records( ots_record_t* records_handle, size_t num_records )
{
	if (NULL == records_handle)
		return EC_OTSCLIENT_NULL_POINTER;
	ots_record** records = (ots_record**)records_handle;
	for (size_t i = 0; i < num_records; ++i)
	{
		ots_record_destroy((ots_record_t)records[i]);
	}

	ots_free(records);
	
	return EC_SUCCESS;
}

void ots_record_query_destroy( ots_query_t query_handle, ots_query_iterator_t iter_handle )
{
	if (NULL == query_handle)
		return;
	ots_query* query = (ots_query*) query_handle;
	QueryIterator* iter = (QueryIterator*) iter_handle;

	if(NULL != iter)
	{
		delete iter;
		iter = NULL;
	}

	ots_record_destroy_primary_key(query->row_key);
	ots_free(query->start_key);
	query->start_key = 0;
	ots_free(query->end_key);
	query->end_key = 0;
	ots_free(query->key_prefix);
	query->key_prefix = 0;

	ots_free(query->columns);
	ots_free(query->index_query_cond);
	ots_free(query);
	return;
}

int32_t ots_record_query_set_type( ots_query_t query_handle, OTS_QUERY_TYPE type )
{
	ots_query* query = (ots_query*) query_handle;
	if (NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(OTS_QUERY_BY_PRIMARY_KEY != type && OTS_QUERY_BY_PRIMARY_KEY != type 
		&& OTS_QUERY_BY_RANGE_KEY_INTERVAL != type && OTS_QUERY_BY_INDEX != type
		&&OTS_QUERY_BY_INDEX_TABLE != type)
		return EC_OTSCLIENT_INVALID_ARGUMENT;
	query->type = type;
	return EC_SUCCESS;
}

int32_t ots_record_query_set_table( ots_query_t query_handle, const char *table )
{
	ots_query* query = (ots_query*) query_handle;
	if (NULL == query )
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(NULL == table || strlen(table) > TABLE_NAME_LEN -1)
		return EC_OTSCLIENT_INVALID_ARGUMENT;
	strcpy(query->table_name, table);
	return EC_SUCCESS;
}
int32_t ots_record_query_set_primary_key(
	ots_query_t query,					/* [in] query handle */
	const ots_primary_key *primary_key	/* [in] hash key*/
	)	 
{
	if(NULL == query)
	{
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	}

	if(NULL == primary_key )
		return EC_OTSCLIENT_NULL_POINTER;

	if(0 >= primary_key->hash_key_len || NULL == primary_key->hash_key)
		return EC_OTSCLIENT_INVALID_ARGUMENT;

	ots_query* pQuery = (ots_query*)query;
	ots_record_destroy_primary_key(pQuery->row_key);
	pQuery->row_key = (ots_primary_key*)ots_malloc(sizeof(ots_primary_key));
	pQuery->row_key->hash_key = (char*)ots_malloc(primary_key->hash_key_len);
	pQuery->row_key->hash_key_len = primary_key->hash_key_len;
	memcpy(pQuery->row_key->hash_key, primary_key->hash_key, primary_key->hash_key_len);

	if(NULL != primary_key->range_key && primary_key->range_key_len > 0)
	{
		pQuery->row_key->range_key = (char*)ots_malloc(primary_key->range_key_len);
		pQuery->row_key->range_key_len = primary_key->range_key_len;
		memcpy(pQuery->row_key->range_key, primary_key->range_key, primary_key->range_key_len);
	}

	return EC_SUCCESS;
}

int32_t
	ots_record_query_set_range_key_interval(
	ots_query_t query,						 /* [in] query handle */
	const char *hash_key,					 /* [in] hash key value*/
	const size_t hash_key_length,			 /* [in] hash key value length */
	const char *range_key_start,			 /* [in] range key start value*/
	const size_t range_key_start_length,	 /* [in] range key start value length */
	const char *range_key_end,			     /* [in] range key end value*/
	const size_t range_key_end_length)	     /* [in] range key end value length */
{
	if(NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(NULL == hash_key || hash_key_length <=0)
		return EC_OTSCLIENT_INVALID_ARGUMENT;

	ots_query* pQuery = (ots_query*)query;
	ots_record_destroy_primary_key(pQuery->row_key);
	pQuery->row_key = (ots_primary_key*)ots_malloc(sizeof(ots_primary_key));
	pQuery->row_key->hash_key = (char*)ots_malloc(hash_key_length);
	pQuery->row_key->hash_key_len =hash_key_length;
	memcpy(pQuery->row_key->hash_key, hash_key, hash_key_length);

	ots_free(pQuery->start_key);
	pQuery->start_key_len = 0;
	if(NULL != range_key_start && range_key_start_length > 0)
	{
		pQuery->start_key = (char*)ots_malloc(range_key_start_length);
		pQuery->start_key_len = range_key_start_length;
		memcpy(pQuery->start_key, range_key_start, range_key_start_length);
	}

	ots_free(pQuery->end_key);
	pQuery->end_key_len = 0;
	if(NULL != range_key_end && range_key_end_length > 0)
	{
		pQuery->end_key = (char*)ots_malloc(range_key_end_length);
		pQuery->end_key_len = range_key_end_length;
		memcpy(pQuery->end_key, range_key_end, range_key_end_length);
	}
	return EC_SUCCESS;
}

int32_t
	ots_record_query_set_range_key_prefix(
	ots_query_t query,						/* [in] query handle */
	const char *hash_key,					 /* [in] hash key value*/
	const size_t hash_key_length,			 /* [in] hash key value length */
	const char *range_key_prefix,			/* [in] range key prefix*/
	const size_t range_key_prefix_length)
{
	if(NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(NULL == hash_key || hash_key_length <=0)
		return EC_OTSCLIENT_INVALID_ARGUMENT;

	ots_query* pQuery = (ots_query*)query;
	ots_record_destroy_primary_key(pQuery->row_key);
	pQuery->row_key = (ots_primary_key*)ots_malloc(sizeof(ots_primary_key));
	pQuery->row_key->hash_key = (char*)ots_malloc(hash_key_length);
	pQuery->row_key->hash_key_len =hash_key_length;
	memcpy(pQuery->row_key->hash_key, hash_key, hash_key_length);

	ots_free(pQuery->key_prefix);
	pQuery->key_prefix_len = 0;
	if(NULL != range_key_prefix && range_key_prefix_length > 0)
	{
		pQuery->key_prefix = (char*)ots_malloc(range_key_prefix_length);
		pQuery->key_prefix_len = range_key_prefix_length;
		memcpy(pQuery->key_prefix, range_key_prefix, range_key_prefix_length);
	}

	return EC_SUCCESS;
}

int32_t ots_record_query_set_columns
( ots_query_t query_handle, const char *columns )
{
	ots_query* query = (ots_query*) query_handle;
	if (NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(NULL == columns)
		return EC_OTSCLIENT_INVALID_ARGUMENT;
	query->columns = strdup(columns);
	return EC_SUCCESS;
}

int32_t ots_record_query_set_num_max_rows( ots_query_t query_handle, const size_t cache_size )
{
	ots_query* query = (ots_query*) query_handle;
	if (NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;

	query->limits = cache_size;
	return EC_SUCCESS;
}

int32_t ots_record_query_set_index( ots_query_t query, const char *index_name )
{
	if (NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(NULL == index_name || strlen(index_name) +1 > TABLE_NAME_LEN)
		return EC_OTSCLIENT_INVALID_ARGUMENT;
	strcpy(((ots_query*)query)->index_name, index_name);
	return EC_SUCCESS;
}

int32_t ots_record_query_set_index_condition( ots_query_t query,const char *condition )
{
	if (NULL == query)
		return EC_OTSCLIENT_QUERY_NOT_CREATE;
	if(NULL == condition)
		return EC_OTSCLIENT_INVALID_ARGUMENT;
	if(NULL != ((ots_query*)query)->index_query_cond)
		ots_free(((ots_query*)query)->index_query_cond);
	((ots_query*)query)->index_query_cond = (char*)ots_malloc(strlen(condition) + 1);
	strcpy(((ots_query*)query)->index_query_cond, condition);
	((ots_query*)query)->index_query_cond_len = strlen(condition) + 1;
	return EC_SUCCESS;
}

