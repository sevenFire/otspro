#include "record.h"
#include "types.h"
#include "ots_types.h"
#include "common.h"
#include "error_code.h"
#include "string.h"

#define OTS_CLIENT_CELL_BUF_SIZE	10

int32_t ots_record_create(ots_record_t *record_ptr)
{
	ots_record* pRecord = (ots_record*)ots_malloc(sizeof(ots_record));
	*record_ptr = &pRecord;
	return EC_SUCCESS;
}

int32_t ots_record_destroy(const ots_record_t record)
{
	ots_record* pRecord = (ots_record*)record;
	ots_record_destroy_cells(pRecord->cells, pRecord->cell_num);
	pRecord->cells = NULL;
	pRecord->cell_num = 0;
	pRecord->cell_buf_size = 0;
	ots_free(pRecord->hash_key);
	pRecord->hash_key_len = 0;
	ots_free(pRecord->range_key);
	pRecord->range_key_len = 0;
	ots_free(pRecord)
	return EC_SUCCESS;
}

int32_t ots_record_set_primary_key(
	ots_record_t record,	    /* [in] record object*/
	const byte_t *hash_key,		/* [in] hash key */
	size_t hash_key_len,		/* [in] hash key length*/
	const byte_t *range_key,	/* [in] range key */
	size_t range_key_len)
{
	ots_record* pRecord = NULL;
	if(NULL == record)
		return EC_OTSCLIENT_NULL_POINTER;

	pRecord = (ots_record*)record;
	if(NULL != hash_key && hash_key_len >0)
	{
		ots_free(pRecord->hash_key);
		pRecord->hash_key_len = 0;	

		pRecord->hash_key = (char*)ots_malloc(hash_key_len);
		memcpy(pRecord->hash_key, hash_key, hash_key_len - 1);
		pRecord->hash_key_len = hash_key_len;
	}

	if(NULL != range_key && range_key_len > 0)
	{
		ots_free(pRecord->range_key);
		pRecord->range_key_len = 0;

		pRecord->range_key = (char*)ots_malloc(range_key_len);
		memcpy(pRecord->range_key, range_key, range_key_len - 1);
		pRecord->range_key_len = range_key_len;
	}

	return EC_SUCCESS;
}

int32_t ots_record_add_row_cell(
	ots_record_t record,	/* [in] record object*/
	ots_cell *cell)
{
	ots_record* pRecord = NULL;
	if(NULL == record || NULL == cell)
		return EC_OTSCLIENT_NULL_POINTER;

	pRecord = (ots_record*)record;
 
	if(pRecord->cell_buf_size == pRecord->cell_num)
	{
		ots_cell* pOldCell = pRecord->cells;
		pRecord->cells = (ots_cell*)ots_malloc(sizeof(ots_cell)*(OTS_CLIENT_CELL_BUF_SIZE+pRecord->cell_buf_size));
		if(NULL != pOldCell)
		{
			memcpy(pRecord->cells, pOldCell, sizeof(ots_cell)*pRecord->cell_num);
			ots_free(pOldCell);
		}
	}
	
	pRecord->cells[pRecord->cell_num].column = (char*)ots_malloc(cell->column_len);
	if(NULL ==pRecord->cells[pRecord->cell_num].column)
		return EC_OTSCLIENT_NULL_POINTER;
	memcpy(pRecord->cells[pRecord->cell_num].column, cell->column, cell->column_len);
	pRecord->cells[pRecord->cell_num].column_len = cell->column_len;

	pRecord->cells[pRecord->cell_num].value = (byte_t*)ots_malloc(cell->value_len);
	if(NULL == pRecord->cells[pRecord->cell_num].value)
	{
		ots_free(pRecord->cells[pRecord->cell_num].column);
		return EC_OTSCLIENT_NULL_POINTER;
	}
	memcpy(pRecord->cells[pRecord->cell_num].value, cell->value, cell->value_len);
	pRecord->cells[pRecord->cell_num].value_len = cell->value_len;

	pRecord->cell_num++;

	return EC_SUCCESS;
}

int32_t
	ots_record_get_key(
	const ots_record_t record,	  /* [in] record object which returned by calling ots_record_query_next*/
	ots_primary_key **key_ptr)
{
	ots_record* pRecord = NULL;
	ots_primary_key* pKey = NULL;
	if(NULL == record || NULL == key_ptr)
		return EC_OTSCLIENT_NULL_POINTER;
	pRecord = (ots_record*)record;
	pKey = (ots_primary_key*)ots_malloc(sizeof(ots_primary_key));
	if(NULL != pRecord->hash_key && pRecord->hash_key_len > 0)
	{
		pKey->hash_key = (char*)ots_malloc(pRecord->hash_key_len);
		pKey->hash_key_len = pRecord->hash_key_len;
		memcpy(pKey->hash_key, pRecord->hash_key, pRecord->hash_key_len);
	}

	if(NULL != pRecord->range_key && pRecord->range_key_len > 0)
	{
		pKey->range_key = (char*)ots_malloc(pRecord->range_key_len);
		pKey->range_key_len = pRecord->range_key_len;
		memcpy(pKey->range_key, pRecord->range_key, pRecord->range_key_len);
	}

	*key_ptr = pKey;
	return EC_SUCCESS;
}

int32_t
	ots_record_get_cells(
	const ots_record_t record,     /* [in]  ots record object*/
	ots_cell **cells_ptr,   /* [in, out] cells related with the same record row key */
	uint32_t *num_cells_ptr)
{
	ots_cell* pCells = NULL;
	ots_record* pRecord = NULL;
	int i = 0;
	if(NULL == record || NULL == cells_ptr || NULL == num_cells_ptr)
		return EC_OTSCLIENT_NULL_POINTER;

	pRecord = (ots_record*)record;
	if(pRecord->cell_num > 0)
	{
		pCells = (ots_cell*)ots_malloc(sizeof(ots_cell)*pRecord->cell_num);
		for(i = 0; i < pRecord->cell_num; ++i)
		{
			pCells[i].column = (char*)ots_malloc(pRecord->cells[i].column_len);
			pCells[i].column_len = pRecord->cells[i].column_len;
			memcpy(pCells[i].column, pRecord->cells[i].column, pRecord->cells[i].column_len);

			pCells[i].value = (byte_t *)ots_malloc(pRecord->cells[i].value_len);
			pCells[i].value_len = pRecord->cells[i].value_len;
			memcpy(pCells[i].value, pRecord->cells[i].value, pRecord->cells[i].value_len);
		}
		*cells_ptr = pCells;
		*num_cells_ptr = pRecord->cell_num;
	}

	return EC_SUCCESS;
}

void ots_record_destroy_primary_key(ots_primary_key* key)
{
	if(NULL != key)
	{		
		ots_free(key->hash_key);
		key->hash_key_len = 0;
		ots_free(key->range_key);
		key->range_key_len = 0;
		ots_free(key);
	}
}

void ots_record_destroy_cells(ots_cell* sells_ptr, uint32_t num_cells)
{
	uint32_t i = 0;
	if(NULL != sells_ptr)
	{
		for(i = 0; i < num_cells; ++i)
		{
			ots_free(sells_ptr[i].column);
			sells_ptr[i].column_len = 0;
			ots_free(sells_ptr[i].value);
			sells_ptr[i].value_len = 0;
		}

		ots_free(sells_ptr);
	}
}
