#include "CUnit.h"
#include "Automated.h"
#include "client.h"
#include "error_code.h"
#include "types.h"
#include "ots_types.h"
#include "stdio.h"
#include "otsclient_ut.h"
#include "record.h"

void print_records(ots_record_t records, int num)
{
	int i = 0;
	ots_record** pRecords = (ots_record**)records;
	for(i = 0; i < num;++i)
	{
		printf("hash_key:%s, hash_key_len:%d, range_key:%s, range_key_len:%d, cell_num:%d, cell_buf_size:%d\n",
			pRecords[i]->hash_key, pRecords[i]->hash_key_len, pRecords[i]->range_key, 
			pRecords[i]->range_key_len, pRecords[i]->cell_num, pRecords[i]->cell_buf_size);
	}
}

/*
void main()
{
	ots_client_t handle = NULL;
	int ret = ots_client_create("10.25.10.23", 8080, 3,3,3, &handle);
	if (ret)
	{
		printf("ots_client_create failed ,error code : %d\n", ret);
	}
	ret = ots_client_login(handle, "admin@test", "admin");
	if (ret)
	{
		printf("ots_client_login failed ,error code : %d\n", ret);
	}


	while(true)
	{
		ots_query_t query_handle = NULL;
		ots_record* record = NULL;
		size_t num = 0;
		ots_query_iterator_t iter = NULL;

		ret = ots_record_query_create(handle, &query_handle);
		if (ret)
		{
			printf("ots_record_query_create failed ,error code : %d\n", ret);
			return;
		}
		ots_record_query_set_type(query_handle, OTS_QUERY_BY_ROW_KEY_REGULAR_EXPRESSION );
		ots_record_query_set_table(query_handle, "pds_13");
		ots_record_query_set_regular_expression(query_handle, ".*" );
		ots_record_query_set_num_max_rows(query_handle, 3);
		while(true)
		{
			ret = ots_record_query_next(query_handle, (ots_record_t**)&record, &num, &iter);
			if (EC_QUERY_END == ret)
			{
				break;
			}
			else if (ret)
			{
				printf("ots_record_query_next failed ,error code : %d\n", ret);
			}
			else
			{
				for (size_t index = 0; index < num; ++index)
				{
					std::string key = (char*)record[index].row_key.row;
					printf("**************record key: %s\n", key.c_str());
					for (size_t i = 0; i< record[index].cell_num; ++i)
					{
						std::string column_name = (char*)record[index].cell[i].column;
						std::string column_value = (char*)record[index].cell[i].value;
						printf("column name : %s, column value: %s\n", column_name.c_str(), column_value.c_str());
					}
				}
				ots_free_records((void**)record, num);
			}
		}
		ots_record_query_destroy(query_handle, iter);
		Sleep(1000);
	}

	ots_client_destroy(handle);
}
*/

void test_ots_client_query_primary_key(void)
{
	int32_t nRet = EC_SUCCESS;
	ots_client_t client = NULL;
	ots_query_t query = NULL;
	ots_query_iterator_t iter = NULL;
	ots_primary_key key;
	ots_record_t* records = NULL;
	size_t num = 0;
	ots_record** pRecords = NULL;
	printf("test_ots_client_create start!\n");
	nRet = ots_client_create(NULL, 8080, 3, 3, 3, NULL);
	CU_ASSERT_TRUE(EC_SUCCESS != nRet);
	nRet = ots_client_create("168.2.237.96", 8080, 3, 3, 3, NULL);
	CU_ASSERT_TRUE(EC_SUCCESS != nRet);

	nRet = ots_client_create("168.2.237.96", 8080, 3, 3, 3, &client);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_client_login(client, "admin@testgroup", "admin");
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_record_query_create(client, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	ots_record_query_set_type(query, OTS_QUERY_BY_PRIMARY_KEY );
	ots_record_query_set_table(query, "huangnewscheme");
	key.hash_key = "123";
	key.hash_key_len = strlen("123")+1;
	key.range_key = "123";
	key.range_key_len = strlen("123")+1;
	ots_record_query_set_primary_key(query,&key);
	ots_record_query_set_num_max_rows(query, 3);

	nRet = ots_record_query_next(query,&records, &num, &iter);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	pRecords = (ots_record**)records;
	CU_ASSERT_TRUE(0 == strcmp(pRecords[0]->hash_key, "123"));
	nRet = ots_free_records(records, num);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	ots_record_query_destroy(query, iter);
	printf("test_ots_client_create end!\n");
}

void test_ots_client_query_range_key_interval(void)
{
	int32_t nRet = EC_SUCCESS;
	ots_client_t client = NULL;
	ots_query_t query = NULL;
	ots_query_iterator_t iter = NULL;
	ots_record_t* records = NULL;
	size_t num = 0;
	ots_record** pRecords = NULL;

	while(1)
	{
		nRet = ots_client_create("168.2.237.96", 8080, 3, 3, 3, &client);
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);

		nRet = ots_client_login(client, "admin@testgroup", "admin");
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);

		nRet = ots_record_query_create(client, &query);
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);
		ots_record_query_set_type(query, OTS_QUERY_BY_RANGE_KEY_INTERVAL );
		ots_record_query_set_table(query, "huangnewscheme");
		ots_record_query_set_range_key_interval(query, "123", strlen("123")+1, "123", strlen("123")+1, NULL, 0);
		ots_record_query_set_num_max_rows(query, 3);

		nRet = ots_record_query_next(query,&records, &num, &iter);
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);
		print_records(records, num);
		nRet = ots_free_records(records, num);
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);
		ots_record_query_destroy(query, iter);
		iter = NULL;
		query = NULL;
		ots_client_destroy(client);	
	}
		printf("test_ots_client_create end!\n");
}


void test_ots_client_query_index(void)
{
	int32_t nRet = EC_SUCCESS;
	ots_client_t client = NULL;
	ots_query_t query = NULL;
	ots_query_iterator_t iter = NULL;
	ots_record_t* records = NULL;
	size_t num = 0;
	ots_record** pRecords = NULL;
	printf("test_ots_client_query_index start!\n");
	//nRet = ots_client_create(NULL, 8080, 3, 3, 3, NULL);
	//CU_ASSERT_TRUE(EC_SUCCESS != nRet);
	//nRet = ots_client_create("168.2.237.96", 8080, 3, 3, 3, NULL);
	//CU_ASSERT_TRUE(EC_SUCCESS != nRet);
	nRet = ots_client_create("168.2.4.53", 8080, 3, 3, 3, &client);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_client_login(client, "admin@testgroup", "admin");
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_record_query_create(client, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	ots_record_query_set_type(query, OTS_QUERY_BY_INDEX );
	ots_record_query_set_table(query, "test_315");
	ots_record_query_set_index(query, "find_by_name");
	ots_record_query_set_index_condition(query, "*:*");
	ots_record_query_set_num_max_rows(query, 3);

	nRet = ots_record_query_next(query,&records, &num, &iter);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	print_records(records, num);
	nRet = ots_free_records(records, num);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	ots_record_query_destroy(query, iter);
	iter = NULL;
	query = NULL;
	ots_client_destroy(client);	
	printf("test_ots_client_create end!\n");
}

void test_ots_client_create(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	printf("test_ots_client_create start!\n");
	nRet = ots_client_create(NULL, 8080, 3, 3, 3, &pClient);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 3, 3, NULL);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_client_create("168.233.333.333.4", OTS_CLIENT_TEST_PORT, 3, 3, 3, &pClient);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(NULL != pClient);
	potsClient = (ots_client*)pClient;
	CU_ASSERT_TRUE(OTS_CLIENT_TEST_PORT == potsClient->port);
	CU_ASSERT_TRUE(3 == potsClient->connection_timeout);
	CU_ASSERT_TRUE(4 == potsClient->read_timeout);
	CU_ASSERT_TRUE(5 == potsClient->write_timeout);
	CU_ASSERT_TRUE(0 == strcmp(OTS_CLIENT_TEST_IP, potsClient->server_addr));
	printf("test_ots_client_create end!\n");
}

void test_ots_client_destroy(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	printf("test_ots_client_destroy start!\n");
	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	printf("test_ots_client_destroy end!\n");
}

void test_ots_client_login(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	printf("test_ots_client_login start!\n");
	nRet = ots_client_login(pClient, "test", "test");
	CU_ASSERT_TRUE(EC_OTSCLIENT_CLIENT_NOT_CREATE == nRet);
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_client_login(pClient, "dddddddddddddddddddddddddddddddd", OTS_CLIENT_TEST_PASSWD);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_client_login(pClient, OTS_CLIENT_TEST_USER, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_client_login(pClient, OTS_CLIENT_TEST_USER, OTS_CLIENT_TEST_PASSWD);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	potsClient = (ots_client*)pClient;
	CU_ASSERT_TRUE(0 == strcmp(OTS_CLIENT_TEST_USER, potsClient->user_name));
	CU_ASSERT_TRUE(0 == strcmp(OTS_CLIENT_TEST_PASSWD, potsClient->password));
	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	printf("test_ots_client_login end!\n");
}

void test_ots_record_query_create(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	ots_query_t query = NULL;
	ots_query* pQuery = NULL;
	printf("test_ots_record_query_create start!\n");
	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_OTSCLIENT_CLIENT_NOT_CREATE == nRet);
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_OTSCLIENT_CLIENT_NOT_LOGIN == nRet);
	nRet = ots_client_login(pClient, OTS_CLIENT_TEST_USER, OTS_CLIENT_TEST_PASSWD);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	pQuery = (ots_query*)query;
	CU_ASSERT_TRUE(NULL != pQuery->client);

	printf("test_ots_record_query_create end!\n");
}

void test_ots_record_query_destroy(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	ots_query_t query = NULL;
	ots_query* pQuery = NULL;
	ots_query_iterator_t iter = NULL;
	ots_record_t* records = NULL;
	size_t num = 0;
	printf("test_ots_record_query_destroy start!\n");
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_client_login(pClient, OTS_CLIENT_TEST_USER, OTS_CLIENT_TEST_PASSWD);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	ots_record_query_destroy(query, iter);
	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	ots_record_query_destroy(query, iter);
	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_record_query_next(query, &records, &num, &iter);
	ots_record_query_destroy(query, iter);
	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	pQuery = (ots_query*)query;
	CU_ASSERT_TRUE(NULL != pQuery->client);

	printf("test_ots_record_query_destroy end!\n");
}

void test_ots_record_set(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	ots_query_t query = NULL;
	ots_query* pQuery = NULL;
	ots_query_iterator_t iter = NULL;
	ots_primary_key key;
	int i = 0;
	char szTmpName[32];
	printf("test_ots_record_set start!\n");
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_client_login(pClient, OTS_CLIENT_TEST_USER, OTS_CLIENT_TEST_PASSWD);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	// before query create
	nRet = ots_record_query_set_type(query, OTS_QUERY_BY_RANGE_KEY_INTERVAL);
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_table(query, "test");
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_primary_key(query, &key);
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_range_key_interval(query, "test_hash", 
		strlen("test_hash") + 1, "test_range_start", strlen("test_range_start") + 1, 
		"test_range_end", strlen("test_range_end") + 1);
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_range_key_prefix(query, "test_hash", strlen("test_hash") + 1, 
		"test_prefix", strlen("test_prefix") + 1);
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_num_max_rows(query, 3);
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_columns(query, "test_column");
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);
	nRet = ots_record_query_set_index(query, "test_index");
	CU_ASSERT_TRUE(EC_OTSCLIENT_QUERY_NOT_CREATE == nRet);

	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	// set type
	nRet = ots_record_query_set_type(query, OTS_QUERY_BY_RANGE_KEY_INTERVAL);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	pQuery = (ots_query*)query;
	CU_ASSERT_TRUE(OTS_QUERY_BY_RANGE_KEY_INTERVAL == pQuery->type);
	
	// set table
	nRet = ots_record_query_set_table(query, "dddddddddddddddddddddddddddddddd");
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_record_query_set_table(query, "test");
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp("test", pQuery->table_name));

	// set primary key
	nRet = ots_record_query_set_primary_key(query, NULL);
	CU_ASSERT_TRUE(EC_OTSCLIENT_NULL_POINTER == nRet);
	memset(&key, 0, sizeof(ots_primary_key));
	nRet = ots_record_query_set_primary_key(query, &key);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	key.hash_key = "test_hash_key";
	key.hash_key_len = strlen(key.hash_key) + 1;
	nRet = ots_record_query_set_primary_key(query, &key);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(key.hash_key, pQuery->row_key->hash_key));
	CU_ASSERT_TRUE(key.hash_key_len == pQuery->row_key->hash_key_len);
	key.range_key = "test_range_key";
	key.range_key_len = strlen(key.range_key) + 1;
	nRet = ots_record_query_set_primary_key(query, &key);
	CU_ASSERT_TRUE(EC_SUCCESS== nRet);
	CU_ASSERT_TRUE(0 == strcmp(key.hash_key, pQuery->row_key->hash_key));
	CU_ASSERT_TRUE(key.hash_key_len == pQuery->row_key->hash_key_len);
	CU_ASSERT_TRUE(0 == strcmp(key.range_key, pQuery->row_key->range_key));
	CU_ASSERT_TRUE(key.range_key_len == pQuery->row_key->range_key_len);

	// set range key
	nRet = ots_record_query_set_range_key_interval(query, NULL, 
		strlen("test_hash") + 1, "test_range_start", strlen("test_range_start") + 1, 
		"test_range_end", strlen("test_range_end") + 1);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_record_query_set_range_key_interval(query, "test_hash", 
		0, "test_range_start", strlen("test_range_start") + 1, 
		"test_range_end", strlen("test_range_end") + 1);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_record_query_set_range_key_interval(query, "test_hash", 
		strlen("test_hash") + 1, "test_range_start", strlen("test_range_start") + 1, 
		NULL, 0);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->row_key->hash_key, "test_hash"));
	CU_ASSERT_TRUE(pQuery->row_key->hash_key_len == strlen("test_hash") + 1);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->start_key, "test_range_start"));
	CU_ASSERT_TRUE(pQuery->start_key_len == strlen("test_range_start") + 1);
	CU_ASSERT_TRUE(NULL == pQuery->end_key);
	CU_ASSERT_TRUE(0 == pQuery->end_key_len);
	nRet = ots_record_query_set_range_key_interval(query, "test_hash", 
		strlen("test_hash") + 1, NULL, 0, 
		"test_range_end", strlen("test_range_end") + 1);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->row_key->hash_key, "test_hash"));
	CU_ASSERT_TRUE(pQuery->row_key->hash_key_len == strlen("test_hash") + 1);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->end_key, "test_range_end"));
	CU_ASSERT_TRUE(pQuery->end_key_len == strlen("test_range_end") + 1);
	CU_ASSERT_TRUE(NULL == pQuery->start_key);
	CU_ASSERT_TRUE(0 == pQuery->start_key_len);
	nRet = ots_record_query_set_range_key_interval(query, "test_hash", 
		strlen("test_hash") + 1, "test_range_start", strlen("test_range_start") + 1, 
		"test_range_end", strlen("test_range_end") + 1);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->row_key->hash_key, "test_hash"));
	CU_ASSERT_TRUE(pQuery->row_key->hash_key_len == strlen("test_hash") + 1);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->start_key, "test_range_start"));
	CU_ASSERT_TRUE(pQuery->start_key_len == strlen("test_range_start") + 1);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->end_key, "test_range_end"));
	CU_ASSERT_TRUE(pQuery->end_key_len == strlen("test_range_end") + 1);

	nRet = ots_record_query_set_range_key_prefix(query, NULL, strlen("test_hash") + 1, 
		"test_prefix", strlen("test_prefix") + 1);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_record_query_set_range_key_prefix(query, "test_hash", 0, 
		"test_prefix", strlen("test_prefix") + 1);
	CU_ASSERT_TRUE(EC_OTSCLIENT_INVALID_ARGUMENT == nRet);
	nRet = ots_record_query_set_range_key_prefix(query, "test_hash", strlen("test_hash") + 1, 
		"test_prefix", strlen("test_prefix") + 1);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->row_key->hash_key, "test_hash"));
	CU_ASSERT_TRUE(pQuery->row_key->hash_key_len == strlen("test_hash") + 1);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->key_prefix, "test_prefix"));
	CU_ASSERT_TRUE(pQuery->key_prefix_len == strlen("test_prefix") + 1);

	// set num
	nRet = ots_record_query_set_num_max_rows(query, 3);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(3 == pQuery->limits);

	// set column
	nRet = ots_record_query_set_columns(query, "test_column");
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->columns, "test_column"));

	// index
	nRet = ots_record_query_set_index(query, "test_index");
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->index_name, "test_index"))

	ots_record_query_set_index_condition(query, "test_cond");
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(0 == strcmp(pQuery->index_query_cond, "test_cond"));
	CU_ASSERT_TRUE(strlen("test_cond") + 1 == pQuery->index_query_cond_len);

	ots_record_query_destroy(query, iter);

	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	pQuery = (ots_query*)query;
	CU_ASSERT_TRUE(NULL != pQuery->client);

	printf("test_ots_record_set end!\n");
}

void test_ots_record_query_next(void)
{
	int nRet = EC_SUCCESS;
	ots_client_t pClient = NULL;
	ots_client * potsClient = NULL;
	ots_query_t query = NULL;
	ots_query* pQuery = NULL;
	ots_query_iterator_t iter = NULL;
	ots_primary_key key;
	ots_primary_key* pKey = NULL;
	ots_record_t* records = NULL;
	size_t num = 0;
	uint32_t i = 0;

	printf("test_ots_record_query_next start!\n");
	nRet = ots_client_create(OTS_CLIENT_TEST_IP, OTS_CLIENT_TEST_PORT, 3, 4, 5, &pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_client_login(pClient, OTS_CLIENT_TEST_USER, OTS_CLIENT_TEST_PASSWD);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);

	nRet = ots_record_query_create(pClient, &query);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_record_query_set_type(query, OTS_QUERY_BY_PRIMARY_KEY);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	memset(&key, 0, sizeof(ots_primary_key));
	key.hash_key = "test_hash";
	key.hash_key_len = strlen(key.hash_key) + 1;
	nRet = ots_record_query_set_primary_key(query, &key);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	nRet = ots_record_query_next(query, &records, &num, &iter);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	CU_ASSERT_TRUE(UT_DEF_REC_NUM == num);
	for(i = 0; i < num; ++i)
	{
		char szTmp[128];
		ots_cell* pCell = NULL;
		uint32_t cellNum = 0;
		pKey = NULL;
		nRet = ots_record_get_key(records[i], &pKey);
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);
		sprintf(szTmp, "hash_key%d", i);
		CU_ASSERT_TRUE(0 == strcmp(pKey->hash_key, szTmp));
		CU_ASSERT_TRUE(strlen(szTmp) + 1 == pKey->hash_key_len);
		sprintf(szTmp, "range_key%d", i);
		CU_ASSERT_TRUE(0 == strcmp(pKey->range_key, szTmp));
		CU_ASSERT_TRUE(strlen(szTmp) + 1 == pKey->range_key_len);
		ots_record_destroy_primary_key(pKey);

		nRet = ots_record_get_cells(records[i], &pCell, &cellNum);
		CU_ASSERT_TRUE(EC_SUCCESS == nRet);
		CU_ASSERT_TRUE(cellNum == UT_DEF_CELL_NUM);
		for(i = 0; i < cellNum; ++i)
		{
			char szTmp[32];
			sprintf(szTmp, "col%d", i);
			CU_ASSERT_TRUE(0 == strcmp(pCell[i].column,szTmp));
			CU_ASSERT_TRUE(strlen(szTmp) + 1 == pCell[i].column_len);
			sprintf(szTmp, "col_val%d", i);
			CU_ASSERT_TRUE(0 == strcmp((char*)pCell[i].value,szTmp));
			CU_ASSERT_TRUE(strlen(szTmp) + 1 == pCell[i].value_len);
		}
		ots_record_destroy_cells(pCell, cellNum);
	}

	ots_free_records(records, num);
	ots_record_query_destroy(query, iter);

	nRet = ots_client_destroy(pClient);
	CU_ASSERT_TRUE(EC_SUCCESS == nRet);
	pQuery = (ots_query*)query;
	CU_ASSERT_TRUE(NULL != pQuery->client);

	printf("test_ots_record_query_next end!\n");
}

CU_TestInfo case_ots_client[] = {
	//{"function: test_ots_client_query_primary_key:", test_ots_client_query_primary_key },
	//{"function: test_ots_client_query_range_key_interval:", test_ots_client_query_range_key_interval },
	{"function: test_ots_client_create:", test_ots_client_create },
	{"function: test_ots_client_destroy:", test_ots_client_destroy },
	{"function: test_ots_client_login:", test_ots_client_login },
	{"function: test_ots_record_query_create:", test_ots_record_query_create },
	{"function: test_ots_record_query_destroy:", test_ots_record_query_destroy },
	{"function: test_ots_record_set:", test_ots_record_set },
	{"function: test_ots_record_query_next:", test_ots_record_query_next },
	{"function: test_ots_client_query_index:", test_ots_client_query_index },
	CU_TEST_INFO_NULL
};