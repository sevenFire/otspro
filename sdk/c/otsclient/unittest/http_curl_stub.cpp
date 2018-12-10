#include "http_curl.h"  
#include "base64.h"
#include "curl.h"  
#include <string>  
#include "otsclient_ut.h"
#include <sstream>
#include <string.h>
#include "common.h"

CHttpCurl::CHttpCurl(void) :m_bDebug(false),m_name(""),m_passwd(""),m_nConnTimeout(OTS_CLIENT_DEF_CONN_TIMEOUT), m_nOptTimeout(OTS_CLIENT_DEF_OPT_TIMEOUT)
{  
}  
  
CHttpCurl::~CHttpCurl(void)  
{  
  
}  

CURL *curl_easy_init(void)
{
	return NULL;
}

void curl_easy_cleanup(CURL *curl)
{
	return ;
}

char *curl_easy_escape(CURL *handle,
	const char *str,
	int length)
{
	return strdup(str);
}

void curl_free(void *p)
{
	if(NULL != p)
		ots_free(p);
}
  
static int OnDebug(CURL *, curl_infotype itype, char * pData, size_t size, void *)  
{  
    if(itype == CURLINFO_TEXT)  
    {  
        //printf("[TEXT]%s\n", pData);  
    }  
    else if(itype == CURLINFO_HEADER_IN)  
    {  
        printf("[HEADER_IN]%s\n", pData);  
    }  
    else if(itype == CURLINFO_HEADER_OUT)  
    {  
        printf("[HEADER_OUT]%s\n", pData);  
    }  
    else if(itype == CURLINFO_DATA_IN)  
    {  
        printf("[DATA_IN]%s\n", pData);  
    }  
    else if(itype == CURLINFO_DATA_OUT)  
    {  
        printf("[DATA_OUT]%s\n", pData);  
    }  
    return 0;  
}  
  
static size_t OnWriteData(void* buffer, size_t size, size_t nmemb, void* lpVoid)  
{  
    std::string* str = dynamic_cast<std::string*>((std::string *)lpVoid);  
    if( NULL == str || NULL == buffer )  
    {  
        return -1;  
    }  
  
    char* pData = (char*)buffer;  
    str->append(pData, size * nmemb);  
    return nmemb;  
}  
  
int CHttpCurl::Get(const std::string & strUrl, std::string & strResponse)  
{  
	return Curl(CURL_GET, strUrl, "", strResponse);
}  

int CHttpCurl::Post(const std::string & strUrl, const std::string & strPost, std::string & strResponse)  
{  
	return Curl(CURL_POST, strUrl, strPost, strResponse);
}  

int CHttpCurl::Put(const std::string & strUrl, const std::string & strPost, std::string & strResponse)  
{  
	return Curl(CURL_PUT, strUrl, strPost, strResponse);
}  
 
int CHttpCurl::Delete(const std::string & strUrl, std::string & strResponse)  
{  
    return Curl(CURL_DELETE, strUrl, "", strResponse);
}  

int CHttpCurl::Curl(const int nMethod, const std::string & strUrl, const std::string & strPost, std::string & strResponse)
{
	switch(nMethod)
	{
	case CURL_GET:
		{
			std::stringstream ss("");
			if(strUrl.compare("http://168.2.4.53:8080/otsrest/api/token") == 0)
				strResponse = "{\"errcode\":\"0\", \"token\":\"test_token\"}";
			else if(0 == strUrl.compare(0, strlen("http://168.2.4.53:8080/otsrest/api/record"), "http://168.2.4.53:8080/otsrest/api/record"))
			{
				ss<<"{\"errcode\":\"0\", \"records\":[";
				for(int i = 0; i < UT_DEF_REC_NUM; ++i)
				{
					ss<<"{";
					for(int j = 0; j < UT_DEF_CELL_NUM; ++j)
					{
						ss<<"\"col"<<j<<"\":\"col_val"<<j<<"\",";
					}
					ss<<"\"hash_key\":\"hash_key"<<i<<"\", \"range_key\":\"range_key"<<i<<"\"}";
					if(i+1 < UT_DEF_REC_NUM)
					{
						ss<<",";
					}

				}
				ss<<"]}";
				strResponse = ss.str();
			}
			else if(0 == strUrl.compare(0, strlen("http://168.2.4.53:8080/otsrest/api/index"), "http://168.2.4.53:8080/otsrest/api/index"))
			{
				ss<<"{\"errcode\":\"0\", \"next_cursor_mark\":\"next_cursor\",\"records\":[";
				for(int i = 0; i < UT_DEF_REC_NUM; ++i)
				{
					ss<<"{";
					for(int j = 0; j < UT_DEF_CELL_NUM; ++j)
					{
						ss<<"\"col"<<j<<"\":\"col_val"<<j<<"\",";
					}
					ss<<"\"hash_key\":\"hash_key"<<i<<"\", \"range_key\":\"range_key"<<i<<"\"}";
					if(i+1 < UT_DEF_REC_NUM)
					{
						ss<<",";
					}

				}
				ss<<"]}";
				strResponse = ss.str();
			}
		}
	}
	return 0; 
}

///////////////////////////////////////////////////////////////////////////////////////////////  
  
void CHttpCurl::SetDebug(bool bDebug)  
{  
    m_bDebug = bDebug;  
} 

void CHttpCurl::SetUserName(const std::string & name)
{
	m_name = name;
}

void CHttpCurl::SetPassword(const std::string & passwd)
{
	m_passwd = passwd;
}

void CHttpCurl::SetConnTimeout(const int timeout)
{
	m_nConnTimeout = timeout;
}

void CHttpCurl::SetOptTimeout(const int timeout)
{
	m_nOptTimeout = timeout;
}
