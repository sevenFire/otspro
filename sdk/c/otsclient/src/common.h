#ifndef _OTS_COMMON_H_
#define _OTS_COMMON_H
#include "types.h"
#include "ots_types.h"
#include <stdlib.h>

#define OTS_URL_LEN		256
#define OTS_URL_HEAD		"http://"
#define OTS_URL_API		"/otsrest/api"

#ifdef __cplusplus
extern "C" {
#endif

#define OTS_CLIENT_DEF_CONN_TIMEOUT	10
#define OTS_CLIENT_DEF_OPT_TIMEOUT	30

void* ots_malloc(size_t size);

#define ots_free(__PTR_MEM__) \
{\
	if(NULL != __PTR_MEM__)\
	{\
		free(__PTR_MEM__);\
		__PTR_MEM__ = NULL;\
	}\
}\

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif