#include "client.h"
#include "error_code.h"
#include "types.h"
#include "stdio.h"
#include "record.h"
#include "string.h"

int main(int argc, char* argv[])
{
	int32_t nRet = EC_SUCCESS;
	ots_client_t client = NULL;
	ots_query_t query = NULL;
	ots_query_iterator_t iter = NULL;
	ots_record_t* records = NULL;
	size_t num = 0;
	ots_record_t pRecords = NULL;
	//ots_primary_key key;
	int index = 0;

	// create ots_client
	nRet = ots_client_create("10.70.50.36", 8080, 3, 3, 3, &client);
	if(EC_SUCCESS != nRet)
		return nRet;

	// login ots_server
	nRet = ots_client_login(client, "admin@mes", "admin");
	if(EC_SUCCESS != nRet)
	{
		ots_client_destroy(client);
		return nRet;
	}

	// create a query
	nRet = ots_record_query_create(client, &query);
	if(EC_SUCCESS != nRet)
	{
		ots_client_destroy(client);
		return nRet;
	}

	// Set query type, must be one of OTS_QUERY_BY_PRIMARY_KEY,OTS_QUERY_BY_RANGE_KEY_INTERVAL, 
	// OTS_QUERY_BY_RANGE_KEY_PREFIX,OTS_QUERY_BY_INDEX
	ots_record_query_set_type(query, OTS_QUERY_BY_INDEX );

	// Set ots table name
	ots_record_query_set_table(query, "TMMSM96");

	ots_record_query_set_index(query, "index");
	//ots_record_query_set_index_condition(query, "RESUME_TIME:[20151003093000 TO 20151003093900] AND MAT_TRACK_NO:20151003073416517001");
	ots_record_query_set_index_condition(query, "RESUME_TIME:[20151003093000 TO 20151003093900] AND MAT_TRACK_NO:20151003073416517001");

	//key.hash_key = "20151003073416577608";
	//key.hash_key_len = strlen(key.hash_key) + 1;
	//key.range_key = NULL;
	//key.range_key_len = 0;
	//ots_record_query_set_primary_key(query, &key);

	// set hash key and range key interval
	//ots_record_query_set_range_key_interval(query, "123", strlen("123")+1, "123", strlen("123")+1, NULL, 0);

	// set max row buf of query iterator
	ots_record_query_set_num_max_rows(query, 100);
	printf("begin to query table %s\n", "TMMSM96");
	// query records
	nRet = ots_record_query_next(query,&records, &num, &iter);

	while(num > 0 && EC_OTSCLIENT_QUERY_END != nRet)
	{
		printf("query %d records\n", num);
		// view records one by one
		for( index = 0; index < num; ++index)
		{
			ots_primary_key* key = NULL;
			ots_cell* cells = NULL;
			uint32_t cell_num = 0;
			// get key from records
			ots_record_get_key((ots_record_t)records[index], &key);
			// get cells from records
			ots_record_get_cells((ots_record_t)records[index], &cells, &cell_num);
			printf("hash_key:%s, rang_key:%s, cell_num:%d\n", key->hash_key, key->range_key, cell_num);

			// free key
			ots_record_destroy_primary_key(key);
			// free cells
			ots_record_destroy_cells(cells, cell_num);
		}
		// free records
		ots_free_records(records, num);
		records = NULL;
		num = 0;

		// next query
		nRet = ots_record_query_next(query,&records, &num, &iter);
	}

	printf("query end!\n");
	// free query
	ots_record_query_destroy(query, iter);	

	// free client
	ots_client_destroy(client);

	return EC_SUCCESS;
}