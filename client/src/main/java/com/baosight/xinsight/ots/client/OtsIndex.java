package com.baosight.xinsight.ots.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.iharder.Base64;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.ots.OtsConfiguration;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.common.index.Column;
import com.baosight.xinsight.ots.common.index.ColumnListConvert;
import com.baosight.xinsight.ots.client.index.IndexConfigurator;
import com.baosight.xinsight.ots.common.index.IndexInfo;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumnListConvert;
import com.baosight.xinsight.ots.client.metacfg.Configurator;
import com.baosight.xinsight.ots.client.metacfg.Index;
import com.baosight.xinsight.ots.client.table.RecordProvider;
import com.baosight.xinsight.ots.client.table.RecordQueryOption;
import com.baosight.xinsight.ots.client.table.RecordResult;
import com.baosight.xinsight.ots.client.util.ConnectionUtil;
import com.baosight.xinsight.ots.exception.OtsException;

/**
 * OtsIndex
 * 
 * @author huangming
 * @created 2015.08.06
 */
public class OtsIndex {
	private static final Logger LOG = Logger.getLogger(OtsIndex.class);

	private Long tenantid;
	private Long userid;
	private String tablename;
	private String indexname;
	private Index info = null;
	private OtsConfiguration conf = null;
	private static CloudSolrServer server = null;

	//for SolrQuery
	private String nextCursorMark = null;
	private Long matchCount = null;

	public OtsIndex(Long tenantid, Long userid, String tablename, Index info, OtsConfiguration conf) {
		this.tenantid = tenantid;
		this.userid = userid;
		this.tablename = tablename;
		this.info = info;
		this.conf = conf;
		if (info != null) {
			this.indexname = info.getName();
		}
	}

	public OtsIndex(Long tenantid, Long userid, String tablename, String indexname, OtsConfiguration conf) {
		this.tenantid = tenantid;
		this.userid = userid;
		this.tablename = tablename;
		this.indexname = indexname;
		this.conf = conf;
	}

	public static void Release() {		
		if (OtsIndex.server != null) {
			OtsIndex.server.shutdown();
		}
	}
	
	private Index getInfo() throws ConfigException {
		if (this.info != null) {
			return this.info;
		} else { //no safe mode
			Configurator configurator = new Configurator();

			try {
				return configurator.queryIndex(this.tenantid, this.tablename, this.indexname);				
			} catch (ConfigException e) {
				e.printStackTrace();
				throw e;
			} finally {
				configurator.release();
			}
		}
	}
	
	public long getId() throws IOException, ConfigException {
		return getInfo().getId();
	}
	
	public long getUid() throws IOException, ConfigException {
		return this.userid;
	}

	public long getTenantid() throws IOException, ConfigException {
		return this.tenantid;
	}
	
	public String getTenantidAsString() throws IOException, ConfigException {
		return String.valueOf(getTenantid());
	}

	public String getName() throws IOException, ConfigException {
		return this.indexname;
	}	

	public String getStartKey() throws IOException, ConfigException {
		return getInfo().getStartKey();
	}

	public String getEndKey() throws IOException, ConfigException {
		return getInfo().getEndKey();
	}

	public char getPattern() throws IOException, ConfigException {
		return getInfo().getPattern();
	}

	public List<Column> getIndexColumns() throws OtsException, IOException, ConfigException {
		List<Column> medaList = ColumnListConvert.toClass(getMedaColumns()).getColumnList();
		return medaList;
	}
	
	public List<SecondaryIndexColumn> getSecIndexColumns() throws OtsException, IOException, ConfigException {
		List<SecondaryIndexColumn> medaList = SecondaryIndexColumnListConvert.toClass(getMedaColumns()).getColumnList();
		return medaList;
	}
	
	private String getMedaColumns() throws IOException, ConfigException {
		return getInfo().getIndexColumns();
	}

	public Date getLastModify() throws IOException, ConfigException {
		return getInfo().getLastModify();
	}

	public Date getCreateTime() throws IOException, ConfigException {
		return getInfo().getCreateTime();
	}

	public int getShardNum() throws IOException, ConfigException {
		return getInfo().getShardNum();
	}

	public int getReplicationNum() throws IOException, ConfigException {
		return getInfo().getReplicationNum();
	}
	
	public String getNextCursorMark() {
		return nextCursorMark;
	}
	
	public Long getMatchCount() {
		return matchCount;
	}
	
	public void rebuild() throws IndexException, IOException, ConfigException, OtsException {		
		
		LOG.info("Rebuild index " + this.tablename + "." + getName());
		
		try {			
			IndexConfigurator indexConfigurator = new IndexConfigurator();
			indexConfigurator.ClearIndex(getTenantidAsString(), this.tablename, getName());
			
			indexConfigurator.BuildIndex(getTenantidAsString(), this.tablename, getName());

		} catch (IOException e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_REBUILD, e.getMessage());
		} catch (IndexException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("rebuild index error. " + e.getMessage());
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_REBUILD, "rebuild index error. " + e.getMessage());
		}
		
		Configurator configurator = new Configurator();
		try {			
			Index index = this.info;
			index.setLastModify(new Date());
			configurator.updateIndex(index);		
			this.info = index;
		} catch (ConfigException e) {
			e.printStackTrace();
			LOG.error("rebuild index error, may column list is invalid!");
			throw e;
		} finally {
			configurator.release();
        }		
	}

	public void truncate() throws IndexException, IOException, ConfigException, OtsException {

		IndexConfigurator indexConfigurator = new IndexConfigurator();
		try {
			LOG.info("Truncate index " + this.tablename + "." + getName());
			indexConfigurator.ClearIndex(getTenantidAsString(), this.tablename, getName());
			
		} catch (IndexException e) {
			e.printStackTrace();			
			throw e;
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_UPDATE, e.getMessage());
		}
		
		Configurator configurator = new Configurator();
		try {
			Index index = this.info;
			index.setLastModify(new Date());
			configurator.updateIndex(index);		
			this.info = index;
		} catch (ConfigException e) {
			e.printStackTrace();
			throw e;
		} finally {
			configurator.release();
        }		
	}
	
	public void update(IndexInfo model) throws OtsException, IndexException, ConfigException {
		IndexConfigurator indexConfigurator = new IndexConfigurator();
		Configurator configurator = new Configurator();
		
		try {	
			LOG.info("Update index " + this.tablename + "." + getName());
			
			if(model.getColumns().size() == 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NUM, "lack index column");
			}
			
			//check duplicate columns and empty name
			if (model.checkColumnsDuplicateAndEmpty()) {
				LOG.warn("create index "+ getName() + "." + model.getName() + " failed for duplicate column or empty column name!");
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_DUPLICATE_COLUMN, "Update index "+ getName() + "." + model.getName() + " failed for duplicate column or empty column name!");
			}
			
			if(model.getShardNum() <= 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_SHARE_NUM, "Invalid shard num:" + model.getShardNum());
			}
			if(model.getReplicationNum() <= 0 ) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_REPLICATION, "Invalid replication:" + model.getReplicationNum());
			}
			if(model.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_ONLINE &&  
					model.getPattern() != (char)OtsConstants.OTS_INDEX_PATTERN_OFFLINE) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_INVALID_PATTERN, "Invalid Pattern:" + model.getPattern());
			}
						
			//indexConfigurator.ClearIndex(getTenantidAsString(), this.tablename, getName());				
			indexConfigurator.DeleteIndex(getTenantidAsString(), this.tablename, getName());							

			indexConfigurator.CreateIndex(getTenantidAsString(), this.tablename, getName(), model);
			
			indexConfigurator.BuildIndex(getTenantidAsString(), this.tablename, getName());
			
			//update rdb index info
			Index index = this.info;
			index.setStartKey(model.getStartKey());
			index.setEndKey(model.getEndKey());	
			ColumnListConvert columnListModel = new ColumnListConvert();
			columnListModel.setColumnList(model.getColumns());
			index.setIndexColumns(columnListModel.toString());
			index.setPattern(model.getPattern());
			index.setShardNum(model.getShardNum());
			index.setReplicationNum(model.getReplicationNum());
			index.setLastModify(new Date());
			
			configurator.delIndexProfile(index.getId());//clear old display columns
			configurator.updateIndex(index);		
			this.info = index;
			
		} catch (ConfigException e) {
			e.printStackTrace();			
			throw e;
		} catch (IndexException e) {
			e.printStackTrace();			
			throw e;
		} catch (IOException e) {
			e.printStackTrace();			
			throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_UPDATE, e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_UPDATE, e.getMessage());
		} finally {
			configurator.release();
        }
	}	

	private String getTableFullname(long tenantid, String tablename) {
		return (String.valueOf(tenantid) + TableName.NAMESPACE_DELIM + tablename);
	}

	public RecordResult getRecords(String strQuery, List<String> filters,
			List<byte[]> columns, List<String> orders, Long limit, Long offset) throws Exception {

		return getRecordsByCursorMark(strQuery, filters, columns, orders, limit, offset, null);
	}
	
	public RecordResult getRecordsByCursorMark(String strQuery, List<String> filters,
			List<byte[]> columns, List<String> orders, Long limit, Long offset, String cursor_mark) throws Exception {

		Table table = null;
		try {
			//init
			this.matchCount = null;
			this.nextCursorMark = null;
			
			if (limit != null && limit < 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_QUERY, "'limit' parameter cannot be negative");
			}			
			if (OtsIndex.server == null) {
				String zkHost = this.conf.getProperty(OtsConstants.ZOOKEEPER_QUORUM);
				OtsIndex.server = new CloudSolrServer(zkHost + "/solr");
			}
			OtsIndex.server.setDefaultCollection(getTenantidAsString() + OtsConstants.COLLECTION_NAME_SEPRATOR + this.tablename
					+ OtsConstants.COLLECTION_NAME_SEPRATOR + getName());
			
			SolrQuery query = new SolrQuery();
			query.setQuery(strQuery);

			if (null != filters) {
				for (String filter : filters) {
					query.addFilterQuery(filter);
				}
			}

			query.setStart(offset==null?OtsConstants.DEFAULT_QUERY_OFFSET:offset.intValue());
			if (limit != null && limit > OtsConstants.DEFAULT_QUERY_MAX_LIMIT) {
				System.out.println("convert limit from " + limit + " to support max:" + OtsConstants.DEFAULT_QUERY_MAX_LIMIT);
				limit = (long) OtsConstants.DEFAULT_QUERY_MAX_LIMIT;				
			}
			query.setRows(limit==null ? OtsConstants.DEFAULT_QUERY_LIMIT : limit.intValue());
			query.setFields(OtsConstants.DEFAULT_SOLR_DOCUMENT_ID);//for safe!!

			boolean hasIdOrder = false;
			if (null == orders) {
				//query.setSort(OtsConstants.DEFAULT_SOLR_DOCUMENT_ID, SolrQuery.ORDER.asc);
			} else {
				for (String order : orders) {
					String orderPair[] = order.split(CommonConstants.DEFAULT_COLON_SPLIT);
					if (orderPair[0].equals(OtsConstants.DEFAULT_SOLR_DOCUMENT_ID)) {
						hasIdOrder = true;
					}
					if (orderPair[1].equals("asc") || orderPair[1].equals("ASC")) {
						query.addSort(orderPair[0], SolrQuery.ORDER.asc);
					} else {
						query.addSort(orderPair[0], SolrQuery.ORDER.desc);
					}
				}
			}
			
			/*
			 * Deep Paging cursorMark implementation notes:
				1. The cursorMark parameter itself contains all the necessary state. There is no server-side state.
				2. The start parameter returned is always 0. It’s up to the client to figure out (or remember) what the position is for display purposes.
				3. There is no need to page to the end of the result set with cursorMark (since there is no server-side state kept). Stop where ever you want.
				4. You know you have reached the end of a result set when you do not get back the full number of rows requested, or when the nextCursorMark 
				   returned is the same as the cursorMark you sent(at which point, no documents will be in the returned list).
				5. Although start must always be 0, you can vary the number of rows for every call to vary the page size.
				6. You can re-use cursorMark values, changing other things like what stored fields are returned or what fields are faceted.
				7. A client can efficiently go back pages by remembering previous cursorMarks and re-submitting them.
				
				8. sort must include a tie-breaker sort on the id field. This prevents tie-breaking by internal lucene document id (which can change).
				9. start must be 0 for all calls including a cursorMark.
				10. pass cursorMark=* for the first request.
				11. Solr will return a nextCursorMark in the response. Simply use this value for cursorMark on the next call to continue paging through the results.
			*/
			if (cursor_mark != null) {
				if (!cursor_mark.equals(OtsConstants.DEFAULT_QUERY_CURSOR_START)) {
					query.add(CursorMarkParams.CURSOR_MARK_PARAM, Base64.encodeBytes(Hex.decodeHex(cursor_mark.toCharArray())));
				} else {
					query.add(CursorMarkParams.CURSOR_MARK_PARAM, cursor_mark);
				}
				query.setStart(0);//!!must, start must always be 0, you can vary the number of rows for every call to vary the page size
				if(!hasIdOrder) { //!!must, Cursor functionality requires a sort containing a uniqueKey field tie breaker
					query.setSort(OtsConstants.DEFAULT_SOLR_DOCUMENT_ID, SolrQuery.ORDER.asc);
				}
			} else {
				if (offset != null && offset < 0) {
					throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_QUERY, "'offset' parameter cannot be negative");
				}
			}

			QueryResponse response = OtsIndex.server.query(query);
			List<byte[]> keys = new ArrayList<byte[]>();
			SolrDocumentList docList = response.getResults();
			for (SolrDocument document : docList) {
				if (document.containsKey(OtsConstants.DEFAULT_SOLR_DOCUMENT_ID)) {
					keys.add(Hex.decodeHex(document.getFieldValue(OtsConstants.DEFAULT_SOLR_DOCUMENT_ID).toString().toCharArray()));
				}
			}
			this.matchCount = docList.getNumFound();
			String nextCursorMark = response.getNextCursorMark();
			if (nextCursorMark != null) {
				if (!nextCursorMark.equals(OtsConstants.DEFAULT_QUERY_CURSOR_START)) {
					nextCursorMark = Hex.encodeHexString(Base64.decode(nextCursorMark));
				}
			}
			this.nextCursorMark = nextCursorMark;
			
			RecordQueryOption option = new RecordQueryOption(columns, limit, 0L, null);
			table = ConnectionUtil.getInstance().getTable(getTableFullname(this.tenantid, this.tablename));
			return RecordProvider.getRecordsByKeys(table, keys, option);
			
		} catch (SolrServerException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}