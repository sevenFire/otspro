#include "iterator.h"
#include "error_code.h"
#include "http_curl.h"
#include "cJSON.h"
#include "common.h"
#include <sstream>
#include "record.h"
#include <string.h>
#include <stdio.h>
#include "curl.h"
#include "easy.h"
#include "multi.h"

using namespace std;

QueryIterator::QueryIterator(ots_query* query)
{
	mpQuery = query;
	mbEnd = false;
	if (NULL != query)
		mnCacheSize = query->limits;
	else
		mnCacheSize = 0;
	mnSkip = 0;
	mstrNextCursorMark = "*";
}

QueryIterator::~QueryIterator()
{
	ClearRecords();
}

void QueryIterator::UrlEscape(std::string& strUrl)
{
	CURL *curl = NULL;
	char* escUrl = NULL;
	curl = curl_easy_init();
	if(NULL == curl)
		return;
	escUrl = curl_easy_escape(curl, strUrl.c_str(), 0);
	strUrl = escUrl;
	curl_free(escUrl);
	curl_easy_cleanup(curl);
}

int32_t QueryIterator::GenerateQueryUrl(string& strUrl)
{
	stringstream ssUrl(""); 
	if(OTS_QUERY_BY_INDEX != mpQuery->type && OTS_QUERY_BY_INDEX_TABLE != mpQuery->type)
	{
		ssUrl<<OTS_URL_HEAD<<mpQuery->client->server_addr<<":"<<mpQuery->client->port
			<<OTS_URL_API<<"/record/"<<mpQuery->table_name<<"?";
	}
	else
	{
		ssUrl<<OTS_URL_HEAD<<mpQuery->client->server_addr<<":"<<mpQuery->client->port
			<<OTS_URL_API<<"/index/"<<mpQuery->table_name;
	}

	switch (mpQuery->type)
	{
	case OTS_QUERY_BY_PRIMARY_KEY:
		{
			if(NULL == mpQuery->row_key)
				return EC_OTSCLIENT_LACK_HASH_KEY;
			if(NULL == mpQuery->row_key->hash_key || 0 >= mpQuery->row_key->hash_key_len)
				return EC_OTSCLIENT_LACK_HASH_KEY;
			if(NULL == mpQuery->row_key->range_key || 0 >= mpQuery->row_key->range_key_len)
			{
				string strHashKey = mpQuery->row_key->hash_key;
				UrlEscape(strHashKey);
				ssUrl<<"hash_key="<<strHashKey;
			}
			else
			{
				string strHashKey = mpQuery->row_key->hash_key;
				string strRangeKey = mpQuery->row_key->range_key;
				UrlEscape(strHashKey);
				UrlEscape(strRangeKey);
				ssUrl<<"hash_key="<<strHashKey<<"&range_key="<<strRangeKey;
			}
		}
		break;
	case OTS_QUERY_BY_RANGE_KEY_INTERVAL:
		{
			if(NULL == mpQuery->row_key->hash_key || 0 >= mpQuery->row_key->hash_key_len)
				return EC_OTSCLIENT_LACK_HASH_KEY;
			string strHashKey = mpQuery->row_key->hash_key;
			UrlEscape(strHashKey);
			ssUrl<<"hash_key="<<strHashKey;
			if(NULL != mpQuery->start_key)
			{
				string strStartKey = mpQuery->start_key;
				UrlEscape(strStartKey);
				ssUrl<<"&range_key_start="<<strStartKey;
			}
			if(NULL != mpQuery->end_key)
			{
				string strEndKey = mpQuery->end_key;
				UrlEscape(strEndKey);
				ssUrl<<"&range_key_end="<<strEndKey;
			}
		}
		break;
	case OTS_QUERY_BY_RANGE_KEY_PREFIX:
		{
			if(NULL == mpQuery->row_key->hash_key || 0 >= mpQuery->row_key->hash_key_len)
				return EC_OTSCLIENT_LACK_HASH_KEY;
			string strHashKey = mpQuery->row_key->hash_key;
			UrlEscape(strHashKey);
			ssUrl<<"hash_key="<<strHashKey;
			if(NULL != mpQuery->key_prefix)
			{
				string strKeyPrefix = mpQuery->key_prefix;
				UrlEscape(strKeyPrefix);
				ssUrl<<"&rang_key_prefix="<<strKeyPrefix;
			}
		}
		break;
	case OTS_QUERY_BY_INDEX:
		{
			if(NULL == mpQuery->index_name || 0 == strlen(mpQuery->index_name))
				return EC_OTSCLIENT_INVALID_ARGUMENT;
			ssUrl<<"/"<<mpQuery->index_name<<"?"<<"cursor_mark="<<mstrNextCursorMark;
			if(NULL != mpQuery->index_query_cond)
			{
				string strQueryCond = mpQuery->index_query_cond;
				UrlEscape(strQueryCond);
				ssUrl<<"&query="<<strQueryCond;
			}
		}
		break;
	case OTS_QUERY_BY_INDEX_TABLE:
		{
			if(NULL == mpQuery->index_name || 0 == strlen(mpQuery->index_name))
				return EC_OTSCLIENT_INVALID_ARGUMENT;
			ssUrl<<"/"<<mpQuery->index_name<<"?";//<<"cursor_mark="<<mstrNextCursorMark;
			if(NULL != mpQuery->index_query_cond)
			{
				ssUrl<<mpQuery->index_query_cond;
			}
		}
		break;
	default:
		break;
	}

	if (NULL != mpQuery->columns && strlen(mpQuery->columns) > 0)
	{
		ssUrl<<"&columns="<<mpQuery->columns;
	}

	ssUrl<<"&limit="<<mpQuery->limits;
	if(OTS_QUERY_BY_INDEX != mpQuery->type)
	{
		ssUrl<<"&offset="<<mnSkip;
	}
	
	strUrl = ssUrl.str();

	return EC_SUCCESS;
}

void QueryIterator::ClearRecords()
{
	for(uint32_t i = 0; i < mvRecords.size(); ++ i)
	{
		ots_record_destroy(mvRecords[i]);
	}

	mvRecords.clear();
}

int32_t QueryIterator::QueryFromServer()
{
	int32_t ret = EC_SUCCESS;
	string strUrl = "";
	ret = GenerateQueryUrl(strUrl);
	if(EC_SUCCESS != ret)
		return ret;
	CHttpCurl c;
	std::string resGet;
	c.SetUserName(mpQuery->client->user_name);
	c.SetPassword(mpQuery->client->password);
	cJSON *json;
	if(mpQuery->client->connection_timeout > 0)
		c.SetConnTimeout(mpQuery->client->connection_timeout);
	if(mpQuery->client->read_timeout > 0)
		c.SetOptTimeout(mpQuery->client->read_timeout);
	c.Get(strUrl, resGet);
	printf("str url:%s\n", strUrl.c_str());
	json = cJSON_Parse(resGet.c_str());

	ClearRecords();
	ots_record* pRecord = NULL;

	if (!json) {
		printf("Error before: [%s]\n",cJSON_GetErrorPtr());
		c.Get(strUrl, resGet);
		ret = EC_OTSCLIENT_NO_AUTHORITY;
		goto err_goto;
	} else {
		int error_code = cJSON_GetObjectItem(json, "errcode")->valueint;
		if(error_code != EC_SUCCESS)
			return error_code;
		if(OTS_QUERY_BY_INDEX == mpQuery->type)
		{
			mstrNextCursorMark = cJSON_GetObjectItem(json, "next_cursor_mark")->valuestring;
			printf("next cursor:%s\n", mstrNextCursorMark.c_str());
			UrlEscape(mstrNextCursorMark);
		}
		cJSON* rec_array = cJSON_GetObjectItem(json, "records");
		cJSON* rec_list = rec_array->child;

		while(rec_list)
		{
			cJSON* column = rec_list->child;
			std::vector<ots_cell*> vecCell;
			pRecord = (ots_record*)ots_malloc(sizeof(ots_record));
			if (0 == strcmp(column->string, "hash_key")) //key
			{
				ots_free(pRecord->hash_key);
				pRecord->hash_key_len = 0;
				pRecord->hash_key = (char*)ots_malloc(strlen(column->valuestring) + 1);
				strcpy(pRecord->hash_key, column->valuestring);
				pRecord->hash_key_len = strlen(column->valuestring) + 1;
			}
			else if(0 == strcmp(column->string, "range_key"))
			{
				ots_free(pRecord->range_key);
				pRecord->range_key_len = 0;
				pRecord->range_key = (char*)ots_malloc(strlen(column->valuestring) + 1);
				strcpy(pRecord->range_key, column->valuestring);
				pRecord->range_key_len = strlen(column->valuestring) + 1;
			}
			else
			{
				ots_cell* pCell = (ots_cell*)ots_malloc(sizeof(ots_cell));
				pCell->column = (char*)ots_malloc(strlen(column->string) + 1);
				strcpy(pCell->column, column->string);
				pCell->column_len = strlen(column->string) + 1;
				pCell->value = (byte_t *)ots_malloc(strlen(column->valuestring) + 1);
				memcpy(pCell->value, column->valuestring, strlen(column->valuestring) + 1);
				pCell->value_len = strlen(column->valuestring) + 1;
				vecCell.push_back(pCell);
			}

			while (column->next)
			{
				column = column->next;
				if (0 == strcmp(column->string, "hash_key")) //key
				{
					ots_free(pRecord->hash_key);
					pRecord->hash_key_len = 0;
					pRecord->hash_key = (char*)ots_malloc(strlen(column->valuestring) + 1);
					strcpy(pRecord->hash_key, column->valuestring);
					pRecord->hash_key_len = strlen(column->valuestring) + 1;
				}
				else if(0 == strcmp(column->string, "range_key"))
				{
					ots_free(pRecord->range_key);
					pRecord->range_key_len = 0;
					pRecord->range_key = (char*)ots_malloc(strlen(column->valuestring) + 1);
					strcpy(pRecord->range_key, column->valuestring);
					pRecord->range_key_len = strlen(column->valuestring) + 1;
				}
				else
				{
					ots_cell* pCell = (ots_cell*)ots_malloc(sizeof(ots_cell));
					pCell->column = (char*)ots_malloc(strlen(column->string) + 1);
					strcpy(pCell->column, column->string);
					pCell->column_len = strlen(column->string) + 1;
					pCell->value = (byte_t *)ots_malloc(strlen(column->valuestring) + 1);
					memcpy(pCell->value, column->valuestring, strlen(column->valuestring) + 1);
					pCell->value_len = strlen(column->valuestring) + 1;
					vecCell.push_back(pCell);
				}
			}
			pRecord->cells = (ots_cell*)ots_malloc(sizeof(ots_cell)*vecCell.size());
			for(uint32_t i = 0; i < vecCell.size(); ++i)
			{
				memcpy(&pRecord->cells[i], vecCell[i], sizeof(ots_cell));
				ots_free(vecCell[i]);
			}
			pRecord->cell_num = vecCell.size();
			pRecord->cell_buf_size = vecCell.size();
			vecCell.clear();
			mvRecords.push_back(pRecord);
			rec_list = rec_list->next;
		}

		mnSkip += mvRecords.size();
	}
err_goto:
	cJSON_Delete(json);
	return ret;
}

int32_t QueryIterator::FetchNext( void** item, size_t* num_records)
{
	int32_t ret = EC_SUCCESS;
	*item = NULL;
	do 
	{
		if (!mbEnd && 0 == mvRecords.size())
		{
			ret = QueryFromServer();
			if (EC_SUCCESS != ret)
				break;
		}
		if (mvRecords.size() == 0)
		{
			mbEnd = true;
			ret = EC_OTSCLIENT_QUERY_END;
			break;
		}
		
		*num_records = mvRecords.size();
		ots_record** records = (ots_record**)ots_malloc(sizeof(ots_record*) * mvRecords.size());
		for (size_t i = 0; i <mvRecords.size(); ++i)
		{
			records[i] = mvRecords[i];
		}
		*item = static_cast<void*>(records);
		mvRecords.clear();
	} while (false);
	return ret;
}
