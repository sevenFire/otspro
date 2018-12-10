#include "common.h"
#include "error_code.h"
#include <string.h>

void* ots_malloc(size_t size)
{
	char* ptr = (char*)malloc(size);
	if(NULL != ptr)
	{
		memset(ptr,0, size);
	}

	return ptr;
}
