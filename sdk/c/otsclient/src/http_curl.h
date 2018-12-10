#ifndef __HTTP_CURL_H__  
#define __HTTP_CURL_H__  
  
#include <string>  
  

#define CURL_GET	0
#define CURL_POST	1
#define CURL_PUT	2
#define CURL_DELETE 3

class CHttpCurl  
{  
public:  
    CHttpCurl(void);  
    ~CHttpCurl(void);  
		
public:  
    /** 
    * @brief HTTP POST请求 
    * @param strUrl 输入参数,请求的Url地址
    * @param strPost 输入参数,json数据包
    * @param strResponse 输出参数,返回的内容 
    * @return 返回是否Post成功 
    */  
    int Post(const std::string & strUrl, const std::string & strPost, std::string & strResponse);  
  
    /** 
    * @brief HTTP GET请求 
    * @param strUrl 输入参数,请求的Url地址
    * @param strResponse 输出参数,返回的内容 
    * @return 返回是否Post成功 
    */  
    int Get(const std::string & strUrl, std::string & strResponse);  
    
    /** 
    * @brief HTTP DELETE请求 
    * @param strUrl 输入参数,请求的Url地址
    * @param strResponse 输出参数,返回的内容 
    * @return 返回是否Post成功 
    */  
	int Delete(const std::string & strUrl, std::string & strResponse); 

	/** 
    * @brief HTTP PUT请求 
    * @param strUrl 输入参数,请求的Url地址
    * @param strResponse 输出参数,返回的内容 
    * @return 返回是否Post成功 
    */  
	int Put(const std::string & strUrl, const std::string & strPost, std::string & strResponse); 


public:  
    void SetDebug(bool bDebug);  

	void SetUserName(const std::string & name);
	void SetPassword(const std::string & passwd);
	void SetConnTimeout(int timeout);
	void SetOptTimeout(int timeout);

private:
	int Curl(const int nMethod, const std::string & strUrl, const std::string & strPost, std::string & strResponse); 

private:  
    bool m_bDebug;
	std::string m_name;
	std::string m_passwd;
	int m_nConnTimeout;
	int m_nOptTimeout;
};  
  
#endif 