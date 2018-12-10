#ifndef _ITERATOR_H
#define _ITERATOR_H

#include <deque>
#include <map>
#include <vector>
#include "types.h"
#include "ots_types.h"
#include <string>

class QueryIterator
{
public:
	QueryIterator(ots_query* query);
	~QueryIterator();
public:
	int32_t QueryFromServer();
	int32_t FetchNext(void** item, size_t* num_records);

private:
	int32_t GenerateQueryUrl(std::string& strUrl);
	void UrlEscape(std::string& strUrl);
	void ClearRecords();
private:
	ots_query* mpQuery;
	int32_t mnSkip;
	bool mbEnd;
	int32_t mnCacheSize;
	std::vector<ots_record*> mvRecords;
	std::string mstrNextCursorMark;
};


#endif