#include "http_curl.h"  
#include "base64.h"
#include "curl.h"  
#include <string>  
#include "common.h"
  
CHttpCurl::CHttpCurl(void) :m_bDebug(false),m_name(""),m_passwd(""),m_nConnTimeout(OTS_CLIENT_DEF_CONN_TIMEOUT), m_nOptTimeout(OTS_CLIENT_DEF_OPT_TIMEOUT)
{  
	
}  
  
CHttpCurl::~CHttpCurl(void)  
{  
  
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
	CURLcode res;  
	CURL* curl = curl_easy_init();  
	if(NULL == curl) {  
		return CURLE_FAILED_INIT;  
	}  

	if(m_bDebug) {  
		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);  
		curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, OnDebug);  
	}  

	struct curl_slist *headers = NULL;
	headers = curl_slist_append(headers, "Content-Type: Application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	std::string strAuthInfo = m_name+":"+m_passwd;
	curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC); 
	curl_easy_setopt(curl, CURLOPT_USERPWD, strAuthInfo.c_str());

	curl_easy_setopt(curl, CURLOPT_URL, strUrl.c_str());    
	if (nMethod == CURL_GET) {
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET"); /* !!! */
	} else if (nMethod == CURL_POST) {
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST"); /* !!! */
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, strPost.c_str());
	} else if (nMethod == CURL_PUT) {
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT"); /* !!! */
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, strPost.c_str());
	} else if (nMethod == CURL_DELETE) {
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE"); /* !!! */
	}
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, NULL); 
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, OnWriteData);  
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&strResponse);  
	
    /** 
    * 当多个线程都使用超时处理的时候，同时主线程中有sleep或是wait等操作。 
    * 如果不设置这个选项，libcurl将会发信号打断这个wait从而导致程序退出。 
    */  
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);  
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, m_nConnTimeout);  
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, m_nOptTimeout);  
	curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC); 

	res = curl_easy_perform(curl);  
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl); 
	return res; 
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
