#coding=utf-8
import pycurl
import StringIO
import json
import urllib
import re
from time import sleep


class Record:        
    def __init__(self, hashKey, rangeKey = None):
        self.cells = {}
        self.cells['hash_key'] = hashKey
        if rangeKey != None:    
            self.cells['range_key'] = rangeKey  
        
    def addCell(self, name, value):
        self.cells[name] = value
    
    def addCells(self, cells):
        '''
        ' @param cells: dict, {"name": "value"} 
        '''
        for (k,v) in cells.items():
            self.addCell(k, v)

    def getCells(self):
        return self.cells;
    
    def getHashKey(self):
        return self.cells.get('hash_key')
    
    def getRangeKey(self):
        return self.cells.get('range_key')
    
class RowKey:        
    def __init__(self, hashKey, rangeKey = None):
        self.hash_key = hashKey
        if rangeKey != None:   
            self.range_key = rangeKey
    
    def getHashKey(self):
        return self.hash_key
    
    def getRangeKey(self):
        return self.range_key          

class CursorMarkRecords:        
    def __init__(self, records, next_cousor_mark = None):
        self.records = records
        self.next_cousor_mark = next_cousor_mark
        
    def getRecords(self):
        return self.records
    
    def getNextCursorMark(self):
        return self.next_cousor_mark

class IndexColumn:
    def __init__(self, name, type, maxLen = None):
        self.name = name
        self.type = type
        if maxLen != None:  
            self.maxLen = maxLen
            
    def getColName(self):
        return self.name
    
    def getColType(self):
        return self.type
    
    def getColmaxLen(self):
        return self.maxLen
    

class Index:
    def __init__(self, tablename, name, url, user, passwd):
        self.tablename=tablename
        self.name = name
        self.__url=url
        self.__user=user
        self.__passwd=passwd
        
        self.type = None
        self.pattern = None        
        self.shard_num = None
        self.replication_num = None
        
        self.create_time = None
        self.last_modify = None
        self.columns = None
        
        self.__getInfo()
    
    def getName(self):
        return self.name
    
    def getType(self):
        return self.type
    
    def getPattern(self):
        return self.pattern
    
    def getShardNum(self):
        return self.shard_num
    
    def getReplicationNum(self):
        return self.replication_num
    
    def getCreateTime(self):
        return self.create_time
    
    def getLastModifyTime(self):
        return self.last_modify
    
    def getColumns(self):
        return self.columns
    

    def truncate(self):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST,"PUT") 
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/index/'+self.tablename + '/' +self.name).encode('utf-8'))

        c.setopt(pycurl.POSTFIELDS, json.dumps({'truncate':True}))
        c.perform()
        
        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']

        if (code == 201 and errcode == 0):
            self.__getInfo();
            return 0
        else:
            return (code, str.getvalue())

    def rebuild(self):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, self.__user+':'+self.__passwd)
        c.setopt(pycurl.CUSTOMREQUEST,"PUT") 
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/index/'+self.tablename + '/' +self.name).encode('utf-8'))

        c.setopt(pycurl.POSTFIELDS, json.dumps({'rebuild':True}))
        c.perform()

        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']

        if (code == 201 and errcode == 0):
            self.__getInfo();
            return 0
        else:
            return (code, str.getvalue())

    def update(self, replication_num, pattern, columns):
        '''
        ' update index: hbase index cannot  update
        ' @param replication_num: string
        ' @param pattern: string
        ' @param columns: List<IndexColumn>  
        '''
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST,"PUT") 
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/index/'+self.tablename + '/' +self.name).encode('utf-8'))

        request_data={}
        update_info={}
        if replication_num != None:
            update_info["replication_num"] = replication_num
        if pattern != None:
            update_info["pattern"]=pattern
        if columns != None:
            column_list = []
            for column in columns:
                columnInfo={}
                columnInfo["column"]=column.name
                columnInfo["type"]=column.type
                column_list.append(columnInfo)
            update_info["columns"]=column_list
        request_data["rebuildinfo"]=update_info

        c.setopt(pycurl.POSTFIELDS, json.dumps(request_data))
        c.perform()

        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']

        if (code == 201 and errcode == 0):
            self.__getInfo();
            return 0
        else:
            return (code, str.getvalue())

    def __getInfo(self):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/index/'+self.tablename +'/'+self.name
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result.get('errcode')

        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())                
            self.type = result.get('type') 
            self.shard_num = result.get('shard_num')   
            self.replication_num = result.get('replication_num')
            self.pattern = result.get('pattern')
                
            self.create_time = result.get('create_time')
            self.last_modify = result.get('last_modify')
            columns = result.get('columns')
            self.columns = []
            count = 0
            for column in columns:
                indcol = IndexColumn(column.get('column'), column.get('type'), column.get('maxLen'))
                self.columns.append(indcol)
        else:
            raise RuntimeError(code, str.getvalue()) 

    def getSolrRecords(self, queryinfo):
        '''
        ' @param queryinfo:  dict { "query": "query_exp",  # must
        '                                    "filters":   "filter1,filter2,filter3",
        '                                    "columns":   "column1,column2,column3",
        '                                    "orders":    "column1:desc,column2:asc",
        '                                    "limit":     100,
        '                                    "offset":    0
        '}
        ' @return:  records
        '''
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/index/'+self.tablename + '/' + self.name
        
        if 'query' not in queryinfo:
            url += ('?query=' + urllib.quote('*:*'))
        else:
            query = queryinfo.get('query')
            if query != None:
                despace = re.sub(r'%20', '+', urllib.quote(query)) #去空格
                lparenthese = re.sub(r'%28', '(', despace)         #还原左括号
                rparenthese = re.sub(r'%29', ')', lparenthese)     #还原右括号
                dquotation = re.sub(r'%22', '"', rparenthese)      #还原双引号
                tilde = re.sub(r'%7e', '~', dquotation)            #还原波浪号
                url += '?query=' + tilde
            else:   # 测试特殊场景query = None时用
                url += '?query='
            
        if 'filters' in queryinfo:
            filters = queryinfo.get('filters')
            filters = filters.replace(' ', urllib.quote(' '))
            url += '&filters=' + filters    
                
        if 'columns' in queryinfo:
            columns = queryinfo.get('columns')
            url += '&columns=' + urllib.quote(columns) 
        if 'orders' in queryinfo:
            orders = queryinfo.get('orders')
            url += '&orders='+  re.sub(r'%20', '+', urllib.quote(orders))
        if 'limit' in queryinfo:
            limit = queryinfo.get('limit')
            url += '&limit=%d' %limit
        if 'offset' in queryinfo:   
            offset = queryinfo.get('offset')
            url += '&offset=%d' %offset
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()
        
        print 'url:', url
        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        print 'result:', result
        errcode = result['errcode']
        
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            #match_count = result['match_count']
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col not in ['hash_key', 'range_key']:
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue()) 
        
    def getHbaseRecords(self, queryinfo):
        '''
        ' @param queryinfo:  dict { "query": "query", #must
        '                           "hash_key" : "hash_key", 
        '                           "range_key_start" : "range_key_start", # range query
        '                           "range_key_end" : "range_key_end",                             
        '                           "columns":   "column1,column2,column3",
        '                           "column_ranges":  "col1:[4 TO 8]",
        '                           "limit": 100,
        '                           "cursor_mark": "00000001234"
        '}
        ' @return:  records
	' @return:  next_cursor_mark if using cursor_mark query
       '''
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/index/'+self.tablename + '/' + self.name
        
        query = queryinfo.get('query')
        if query != None:
            despace = re.sub(r'%20', '+', urllib.quote(query)) #去空格
            lparenthese = re.sub(r'%28', '(', despace)         #还原左括号
            rparenthese = re.sub(r'%29', ')', lparenthese)     #还原右括号
            url += '?query=' + rparenthese
        else:
            url += '?query=*:*' 
	    
        if 'hash_key' in queryinfo:
            hash_key = queryinfo.get('hash_key')
            url += '&hash_key=' + urllib.quote(hash_key)
        if 'range_key_start' in queryinfo:
            range_key_start = queryinfo.get('range_key_start')
            url += '&range_key_start=' + urllib.quote(range_key_start)
        if 'range_key_end' in queryinfo:
            range_key_end = queryinfo.get('range_key_end')
            url += '&range_key_end=' + urllib.quote(range_key_end)
        if 'column_ranges' in queryinfo:
            column_ranges = queryinfo.get('column_ranges')
            url += '&column_ranges=' + urllib.quote(column_ranges)
        if 'columns' in queryinfo:
            columns = queryinfo.get('columns')
            url += '&columns=' + columns
        if 'cursor_mark' in queryinfo:
            cursor_mark = queryinfo.get('cursor_mark')
            url += '&cursor_mark=' + cursor_mark                      
        if 'limit' in queryinfo:
            limit = int(queryinfo.get('limit'))
            url += '&limit=%d' %limit
        if 'offset' in queryinfo:
            offset = queryinfo.get('offset')
            url += '&offset=%d' %offset
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()
        
        print 'url:', url
        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        print 'result:', result
        errcode = result['errcode']
        
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            #match_count = result['match_count']
            result_records = result['records']
            records = []
            for row in result_records:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col not in ['hash_key', 'range_key']:
                        rec.addCell(col, row[col])
                records.append(rec)		

            if 'cursor_mark' in queryinfo:
                next_cursor_mark = result['next_cursor_mark']
                return (records, next_cursor_mark)  
            else:		
                return records
        else:
            #raise RuntimeError(code, str.getvalue()) 
            return (code, str.getvalue())


class Table:
    def __init__(self, name, url, user, passwd):
        self.name = name
        self.__url = url
        self.__user = user
        self.__passwd = passwd

        self.__description = None
        self.__compression_type = None
        self.__primary_key_type = None
        self.__hash_key_type = None
        self.__range_key_type = None
        self.__mobenabled = None
        self.__mobthreshold = None
        self.__getInfo()
    
    def getName(self):
        return self.name

    def getDescription(self):
        if self.__description != None:
            return self.__description
        self.__getInfo()
        return self.__description

    def getPrimaryKeyType(self):
        if self.__primary_key_type != None:
            return self.__primary_key_type
        self.__getInfo()
        return self.__primary_key_type
    
    def getHashKeyType(self):
        if self.__hash_key_type != None:
            return self.__hash_key_type
        self.__getInfo()
        return self.__hash_key_type      

    def getRangeKeyType(self):
        if self.__range_key_type != None:
            return self.__range_key_type
        self.__getInfo()
        return self.__range_key_type    

    def getMobenabled(self):
        if self.__mobenabled != None:
            return self.__mobenabled
        self.__getInfo()
        return self.__mobenabled    

    def getCompressionType(self):
        if self.__compression_type != None:
            return self.__compression_type
        self.__getInfo()
        return self.__compression_type

    def getMobthreshold(self):
        if self.__mobthreshold != None:
            return self.__mobthreshold
        self.__getInfo()
        return self.__mobthreshold     

    def truncate(self):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE") 
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/record/'+self.name+'/truncate').encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode ==0:
            self.__getInfo();
            return 0
        else:
            return (code, str.getvalue())        

    def __getInfo(self):        
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.URL, (self.__url+'/table/'+self.name).encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            self.__description = result['description']
            self.__compression_type = result['compression_type']
            self.__primary_key_type = result['primary_key_type']
            self.__hash_key_type = result['hash_key_type']
            if self.__primary_key_type == 1: #0-hash key 1-hash and range key
                self.__range_key_type = result['range_key_type']             
            self.__mobenabled = result['mob_enabled']
            self.__mobthreshold = result['mob_threshold']            
        else:
            raise RuntimeError(code, str.getvalue())

    def putRecords(self, records):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.POST, 1)
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/record/'+self.name).encode('utf-8'))

        request_data={}
        record_list=[]
        for record in records:
            record_list.append(record.cells)
        request_data['records']=record_list

        c.setopt(pycurl.POSTFIELDS, json.dumps(request_data))
        c.perform()

        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']

        if (code == 201 and errcode == 0):
            return 0
        else:
            return (code, str.getvalue())

    def updateRecords(self, records):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/record/'+self.name).encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST,"PUT")  

        request_data={}
        record_list=[]
        for record in records:
            record_list.append(record.cells)
        request_data['records']=record_list

        c.setopt(pycurl.POSTFIELDS, json.dumps(request_data))            
        c.perform()
        
        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 201 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())         
    
    def getRecordsComplex(self, querycondition):
        '''
        ' Query records by query condition:
        ' @param querycondition: query condition, dict
        '    { "hash_key" : "a",               # necessary
        '      "range_key": "1001",
        '      "range_key_start": "0001",
        '      "range_key_end": "1000",
        '      "range_key_prefix": "pre",
        '      "columns": collist,            #List<string>:["col1", "col2"]
        '      "limit': 10,
        '      "range_key_cursor_mark": "FF13DA9370C"    # Hex cursor
        '      "offset": 0}
        ' @return records, List<Record>
        ' @return range_key_next_cursor_mark, str #Hex cursor		
        ' @raise RuntimeError(code, str.getvalue()):
        '''

        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(querycondition.get('hash_key'))
        
        if 'range_key' in querycondition:
            url += '&range_key=' + urllib.quote(querycondition.get('range_key'))
        if 'range_key_start' in querycondition:
            url += '&range_key_start=' + urllib.quote(querycondition.get('range_key_start'))
        if 'range_key_end' in querycondition:
            url += '&range_key_end=' + urllib.quote(querycondition.get('range_key_end'))
        if 'range_key_prefix' in querycondition:
            url += '&range_key_prefix' + urllib.quote(querycondition.get('range_key_prefix'))
        if 'columns' in querycondition:
            url += '&columns='
            for col in querycondition.get('columns'):
                url += urllib.quote(col) + ','
            url = url[:-1]  # delete last ','
        if 'limit' in querycondition:
            url += '&limit=' + urllib.quote(querycondition.get('limit'))
        if 'range_key_cursor_mark' in querycondition:
            url += '&range_key_cursor_mark=' + urllib.quote(querycondition.get('range_key_cursor_mark'))
        if 'offset' in querycondition:
            url += '&offset=' + urllib.quote(querycondition.get('offset'))
        
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()
        
        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode ==0:
            result = json.loads(str.getvalue())
            records = result['records']
            retrecords = []
            for row in records:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                #print 'get record hash_key and range_key:', row.get('hash_key'),row.get('range_key')
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                retrecords.append(rec)
            if 'range_key_cursor_mark' in querycondition:
                next_cursor_mark = result.get('range_key_next_cursor_mark')
                return CursorMarkRecords(records, next_cursor_mark)  
            else:		
                return retrecords
        else:
            raise RuntimeError(code, str.getvalue())
        
    def getRecordsByHashKey(self, hashkey=None, columns=None, limit=100, offset=0):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashkey)

        if columns != None:
            url += '&columns=' + urllib.quote(columns)
        url += '&limit=%d&offset=%d' %(limit,offset)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())

    def getRecords(self, columns=None, limit=100, offset=0):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?limit=%d&offset=%d' %(limit,offset)

        if columns != None:
            url += '&columns=' + urllib.quote(columns)        

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']

        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())

    def getRecordsByKey(self, hashkey=None, rangekey=None, columns=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashkey)
        url += '&range_key=' + urllib.quote(rangekey)

        if columns != None:
            url += '&columns=' + urllib.quote(columns)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())

    def getRecordsByKeys(self, keys=None, columns=None):
        '''
        ' Get records by rowkey:
        ' @param keys: List<RowKey>
        ' @param columns: string
        ' @return:   records
        ' @raise RuntimeError(code, str.getvalue()) 
        '''
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.POST, 1)            
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name+'/keys'

        if columns != None:
            url += '?columns='+ urllib.quote(columns)

        c.setopt(pycurl.URL, url.encode('utf-8'))
   
        request_data={}
        key_list = []
        for key in keys:
            keyInfo={}
            keyInfo = key.__dict__ # 获取RowKey类的变量字典
            key_list.append(keyInfo)
        request_data['key_list']=key_list        
        c.setopt(pycurl.POSTFIELDS, json.dumps(request_data)) 

        c.perform()

        results = json.loads(str.getvalue())
        errcode = results['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            results = json.loads(str.getvalue())
            results = results['records']
            result = []
            [result.append(x) for x in results if x not in result]
            records = []
           
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                #print 'get records: hash_key and range_key:', row.get('hash_key'), row.get('range_key')
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())        

    def getRecordsByRangePrefix(self, hashKey=None, rangeKeyPrefix=None, columns=None, limit=100, offset=0):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashKey)
        url += '&range_key_prefix=' + urllib.quote(rangeKeyPrefix)

        if columns != None:
            url += '&columns=' + urllib.quote(columns)
        url += '&limit=%d&offset=%d' %(limit,offset)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())        

    def getRecordsByRange(self, hashKey=None, rangekeyStart=None, rangekeyEnd=None, columns=None, limit=100, offset=0):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashKey)

        if rangekeyStart!=None:
            url += '&range_key_start=' + urllib.quote(rangekeyStart)
            if rangekeyEnd!=None:
                url+='&range_key_end=' + urllib.quote(rangekeyEnd)
        elif rangekeyEnd!=None:
            url += 'range_key_end=' + urllib.quote(rangekeyEnd)

        if columns != None:
            url += '&columns=' + urllib.quote(columns)
        url += '&limit=%d&offset=%d' %(limit,offset)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())	

    def getRecordsByRangeWithPrefix(self, hashKey=None, rangekeyStart=None, rangekeyEnd=None, rangekeyPrefix=None, columns=None, limit=100, offset=0):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashKey)

        if rangekeyStart!=None:
            url += '&range_key_start=' + urllib.quote(rangekeyStart)
            if rangekeyEnd!=None:
                url+='&range_key_end=' + urllib.quote(rangekeyEnd)
        elif rangekeyEnd!=None:
            url += 'range_key_end=' + urllib.quote(rangekeyEnd)

        if rangekeyPrefix!=None:
            url += '&range_key_prefix=' + urllib.quote(rangekeyPrefix)               


        if columns != None:
            url += '&columns=' + urllib.quote(columns)
        url += '&limit=%d&offset=%d' %(limit,offset)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            result = result['records']
            records = []
            for row in result:
                rec = Record(row.get('hash_key'), row.get('range_key'))
                for col in row:
                    if col != 'hash_key' and col != 'range_key':
                        rec.addCell(col, row[col])
                records.append(rec)
            return records
        else:
            raise RuntimeError(code, str.getvalue())

    def deleteRecordsComplex(self, deletecondition):
        '''
        ' Delete records by delete condition:
        ' @param deletecondition: delete condition, dict
        '    { "hash_key" : "a",               # necessary
        '      "range_key": "1001"
        '      "range_key_start": "0001",
        '      "range_key_end": "1000",
        '      "range_key_prefix": "pre"}
        ' @return 0, (code, str.getvalue())
        '''
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name
        
        url += '?hash_key=' + urllib.quote(deletecondition.get('hash_key'))   
        if 'range_key' in deletecondition:
            url += '&range_key=' + urllib.quote(deletecondition.get('range_key'))
        if 'range_key_start' in deletecondition:
            url += '&range_key_start=' + urllib.quote(deletecondition.get('range_key_start'))
        if 'range_key_end' in deletecondition:
            url += '&range_key_end=' + urllib.quote(deletecondition.get('range_key_end'))
        if 'range_key_prefix' in deletecondition:
            url += '&range_key_prefix' + urllib.quote(deletecondition.get('range_key_prefix'))
        
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())
           
    def deleteRecordsByTime(self, endtime, starttime = None):  
        '''
        ' Delete records by endtime:
        ' @param endtime: "yyyy-MM-dd HH:mm:ss:S"
        ' @param starttime: "yyyy-MM-dd HH:mm:ss:S"
        '''   
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name

        if ' ' in endtime:
            time = endtime.split(' ')
            url += '?end_time=' + time[0] + urllib.quote(' ') + time[1]
        else:
            url += '?end_time=' + urllib.quote(endtime)
                
        if (starttime != None):
            if ' ' in starttime:
                time = starttime.split(' ')
                url += '&start_time=' + time[0] + urllib.quote(' ') + time[1]
            else:
                url += '&start_time=' + urllib.quote(endtime)
                
        print 'url:', url

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()
        code = c.getinfo(pycurl.HTTP_CODE)
        result = json.loads(str.getvalue())
        errcode = result['errcode']
        print 'result:', (code, str.getvalue())
        if (code == 200 and errcode == 0):
            return 0
        else:
            return (code, str.getvalue())
         
    def deleteRecordsByHashKey(self,hashkey=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashkey)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())    

    def deleteRecordsByKey(self, hashkey=None, rangekey=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashkey)
        url += '&range_key=' + urllib.quote(rangekey)

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())     

    def deleteRecordsByKeys(self, keys=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name+'/keys'
        c.setopt(pycurl.URL, url.encode('utf-8'))

        request_data={}
        key_list = []
        for key in keys:
            keyInfo={}
            keyInfo['hash_key']=key.get('hash_key')
            keyInfo['range_key']=key.get('hash_key')
            key_list.append(keyInfo)
        request_data['key_list']=key_list

        c.setopt(pycurl.POSTFIELDS, json.dumps(request_data))         
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())           

    def deleteRecordsByRangePrefix(self, hashKey=None, rangeKeyPrefix=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashKey)
        url += '&range_key_prefix=' + urllib.quote(rangeKeyPrefix)
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())      

    def deleteRecordsByRang(self, hashKey=None, rangekeyStart=None, rangekeyEnd=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashKey)
        url += '&range_key_start='+urllib.quote(rangekeyStart) +'&range_key_end=' +urllib.quote(rangekeyEnd)     
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())        

    def deleteRecordsByRangWithPrefix(self, hashKey=None, rangekeyStart=None, rangekeyEnd=None, rangekeyPrefix=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])
        c.setopt(pycurl.CUSTOMREQUEST,"DELETE")
        url = self.__url+'/record/'+self.name
        url += '?hash_key=' + urllib.quote(hashKey)        
        url += '&range_key_start='+urllib.quote(rangekeyStart) +'&range_key_end=' +urllib.quote(rangekeyEnd)   
        url += '&range_key_prefix='+urllib.quote(rangekeyPrefix)   

        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())                

    def getIndexNames(self):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.URL, (self.__url+'/index/'+self.name+'/_all_indexes').encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            return result['index_names']
        else:
            raise RuntimeError(code, str.getvalue())

    def getIndex(self, index_name):
        return Index(self.name, index_name, self.__url, self.__user, self.__passwd)   

    def createIndex(self, name, indextype, columns, solrattr = None):
        '''
        ' create hbase or solr index
        ' @param name: index name, string
        ' @param indextype: index type, string："0" Solr index，"1" Hbase index
        ' @param columns: columns of index, List<IndexColumn>
        ' @param solrattr: solr attributes, dict:   
        '                    {    "shard_num":3,          # Solr attribute：分片数，建议与集群内Solr服务数量一致
        '                         "replication_num":1,    # Solr attribute：可选项，缺省读取配置文件中该属性值
        '                         "pattern": 0            # Solr attribute：1: online index,  0: offline index
        '                    }
        ' @return: 0 / (errcode, errvalue)                     
        '''
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.POST, 1)
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])       
        url =  self.__url+'/index/'+urllib.quote(self.name)+'/'+urllib.quote(name)
        c.setopt(pycurl.URL, url.encode('utf-8'))

        request_data={}
        request_data['type'] = indextype
        
        if indextype == "0":   # Solr index
            if 'shard_num' in solrattr:
                request_data['shard_num'] = solrattr.get('shard_num')
            if 'replication_num' in solrattr:
                request_data['replication_num'] = solrattr.get('replication_num')
            if 'pattern' in solrattr:
                request_data['pattern'] = solrattr.get('pattern')
        
        column_list = []
        for column in columns:
            columnInfo={}
            columnInfo['column']=column.getColName()
            columnInfo['type']=column.getColType()
            
            if indextype == "1" and column.type == "string" :
                columnInfo['maxLen'] = int(column.getColmaxLen())
            column_list.append(columnInfo)
        request_data['columns']=column_list

        c.setopt(pycurl.POSTFIELDS, json.dumps(request_data))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if (code == 201 and errcode == 0) :   # 要删除or (errcode == 175017)
            return 0
        else:
            return (code, str.getvalue())    

    def deleteIndex(self, index_name):
        #pass
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        url = self.__url+'/index/'+urllib.quote(self.name)+'/'+urllib.quote(index_name)
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
        c.perform()
 
        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())

class OtsAdmin:
    def __init__(self, url, user, passwd):
        self.__url = url
        self.__user = user
        self.__passwd = passwd

    def createTable(self, name, description, primaryKeyType, hashKeyType, rangeKeyType, compressType, mobenabled=False, mobthreshold=100):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, '%s:%s' %(self.__user,self.__passwd))
        c.setopt(pycurl.POST, 1)
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/table/'+name).encode('utf-8'))
        c.setopt(pycurl.POSTFIELDS, json.dumps({"description":description, "primary_key_type":primaryKeyType, "hash_key_type":hashKeyType, "range_key_type":rangeKeyType, "compression_type":compressType, "mob_enabled":mobenabled, "mob_threshold":mobthreshold }))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 201 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())        

    def updateTable(self, name, description, mobenabled, mobthreshold=100):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.HTTPHEADER, ["Content-Type: application/json"])        
        c.setopt(pycurl.URL, (self.__url+'/table/'+name).encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST,"PUT")
        c.setopt(pycurl.POSTFIELDS, json.dumps({"description":description, "mob_enabled":mobenabled, "mob_threshold":mobthreshold}))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 201 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())

    def deleteTable(self, name):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.URL, (self.__url+'/table/'+name).encode('utf-8'))
        c.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            return 0
        else:
            return (code, str.getvalue())    

    def getAllTableNames(self):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        c.setopt(pycurl.URL, (self.__url+'/table/_all_tables').encode('utf-8'))
        c.perform()

        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            return result['table_names']
        else:
            raise RuntimeError(code, str.getvalue())
    
    def queryTable(self, name, limit=None, offset=None):
        c = pycurl.Curl()
        str = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, str.write)
        c.setopt(pycurl.USERPWD, (self.__user+':'+self.__passwd).encode('utf-8'))
        url = self.__url+'/table?name=' + urllib.quote(name)
        if limit != None:
            url += '&limit=' + limit
        if offset != None:
            url += '&offset=' + offset
        
        c.setopt(pycurl.URL, url.encode('utf-8'))
        c.perform()
        
        result = json.loads(str.getvalue())
        errcode = result['errcode']
        code = c.getinfo(pycurl.HTTP_CODE)
        if code == 200 and errcode == 0:
            result = json.loads(str.getvalue())
            return result['table_names']
        else:
            raise RuntimeError(code, str.getvalue())    

    def getTable(self, name):
        table = Table(name, self.__url, self.__user, self.__passwd)
        return table

if __name__ == '__main__':
    ots = OtsAdmin('http://127.0.0.1:8080/otsrest/api', 'admin@testgroup', 'admin')
    print ots.getAllTableNames()
    #table = ots.getTable('t1')
    #columns = []
    #columns.append(IndexColumn.IndexColumn('wyj','string'))
    #table.createIndex('i3', 1, None, None, columns)
    #table.deleteIndex('i2')
    #index = table.getIndex('i3')
    #records = index.getRecords()
    #import pdb; pdb.set_trace()
    #print records
