package com.baosight.xinsight.ots.rest.service;

import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.OtsIndex;
import com.baosight.xinsight.ots.client.OtsSecondaryIndex;
import com.baosight.xinsight.ots.client.OtsTable;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.client.exception.PermissionSqlException;
import com.baosight.xinsight.ots.client.secondaryindex.SecondIndexQueryOption;
import com.baosight.xinsight.ots.client.table.RecordResult;
import com.baosight.xinsight.ots.client.table.RowCell;
import com.baosight.xinsight.ots.client.table.RowRecord;
import com.baosight.xinsight.ots.common.index.Column;
import com.baosight.xinsight.ots.common.index.IndexInfo;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.ots.rest.model.*;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.IndexQueryModel;
import com.baosight.xinsight.ots.rest.model.operate.IndexUpdateModel;
import com.baosight.xinsight.ots.rest.model.operate.SecondIndexQueryModel;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.TableConfigUtil;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IndexService {
    private static final Logger LOG = Logger.getLogger(IndexService.class);

    private static List<Column> ColumnModelListToColumnList(List<ColumnModel> modelList) throws OtsException {
        List<Column> columns = new ArrayList<Column>();
        for (ColumnModel model : modelList) {
            if (model.getType().equals("binary")) {
                LOG.error("index column type now not support binary temporarily!");
                throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "index column type now not support binary temporarily!");
            }

            Column column = new Column(model.getName().trim(), model.getType());
            // column.setIndexed(model.getIndexed());
            // column.setStored(model.getStored());
            columns.add(column);
        }
        return columns;
    }

    private static List<ColumnModel> ColumnListToColumnModelList(List<Column> columnlist) {
        List<ColumnModel> columns = new ArrayList<ColumnModel>();
        for (Column col : columnlist) {
            ColumnModel column = new ColumnModel(col.getName(), col.getType());
            // column.setIndexed(col.getIndexed());
            // column.setStored(col.getStored());
            columns.add(column);
        }
        return columns;
    }

    private static List<ColumnModel> SecondaryColumnListToColumnModelList(List<SecondaryIndexColumn> columnlist) throws OtsException {
        List<ColumnModel> columns = new ArrayList<ColumnModel>();
        for (SecondaryIndexColumn col : columnlist) {
            ColumnModel column = new ColumnModel(col.getName(), col.getType().toString(), col.getMaxLen());
            columns.add(column);
        }
        return columns;
    }

    public static ErrorMode deleteIndex(PermissionCheckUserInfo userInfo, String tableName, String indexName) throws OtsException, IOException, InterruptedException {
        LOG.info("Delete index " + tableName + "." + indexName);
        ErrorMode rMode = new ErrorMode(0L);
        OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
        try {
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkEditPermission(userInfo, table.getId());
            }

            IndexInfoModel indexInfo = getIndexInfo(userInfo, tableName, indexName, null);
            if (RestConstants.OTS_INDEX_TYPE_HBASE == indexInfo.getType()) {
                table.deleteSecordaryIndex(indexName);
            } else {
                table.deleteIndex(indexName);
            }
        } catch (OtsException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw e;
        }
        return rMode;
    }

    public static ErrorMode updateIndex(PermissionCheckUserInfo userInfo, String tableName, String indexName, IndexUpdateModel model) throws OtsException {
        long errorcode = 0;
        ErrorMode rMode = new ErrorMode(errorcode);
        OtsTable tableInfo = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
        try {
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkEditPermission(userInfo, tableInfo.getId());
            }
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            OtsIndex index = table.getIndex(indexName);
            if (null == index) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, String.format("user (id:%d) was not owned index:%s of table:%s!", userInfo.getUserId(), indexName, tableName));
            }

            if (index.getPattern() == (char) OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX) {
                if (null != model.getRebuild() && model.getRebuild()) {
                    LOG.info("Rebuild SecondIndex " + tableName + "." + indexName);
                    table.rebuildSecondIndex(indexName);
                } else if (null != model.getTruncate() && model.getTruncate()) {
                    LOG.info("Truncate SecondIndex " + tableName + "." + indexName);
                    table.truncateSecondIndex(indexName);
                } else {
                    LOG.error("Unknown operation of SecondIndex " + tableName + "." + indexName);
                    throw new OtsException(RestErrorCode.EC_OTS_REST_TABLE_INDEX_INVALID_OP, "Unknown operation of index " + tableName + "." + indexName);
                }
                LOG.debug("RETURN:" + rMode.toString());
                return rMode;
            } else { // solr
                // if exist
                if (null != model.getRebuildmodel()) {
                    LOG.info("Update index " + tableName + "." + indexName);

                    if (model.getRebuildmodel().getIndexPattern() == null) {
                        LOG.error("Index " + tableName + "." + indexName + " pattern unkonwn!");
                        throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "Index " + tableName + "." + indexName + " pattern unkonwn!");
                    }
                    if (model.getRebuildmodel().getIndexPattern() != OtsConstants.OTS_INDEX_PATTERN_ONLINE && model.getRebuildmodel().getIndexPattern() != OtsConstants.OTS_INDEX_PATTERN_OFFLINE) {
                        LOG.error("Index " + tableName + "." + indexName + " pattern invalid!");
                        throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "Index " + tableName + "." + indexName + " pattern invalid!");
                    }
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.setName(indexName);
                    indexInfo.setStartKey(model.getRebuildmodel().getStartKey());
                    indexInfo.setEndKey(model.getRebuildmodel().getEndKey());
                    indexInfo.setPattern((char) model.getRebuildmodel().getIndexPattern().intValue());
                    indexInfo.setShardNum(model.getRebuildmodel().getShard());
                    indexInfo.setReplicationNum(model.getRebuildmodel().getReplication());
                    Integer maxShardNumPerNodeInteger = Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_MAX_SHARDS_PERNODE, "3"));
                    indexInfo.setMaxShardNumPerNode(maxShardNumPerNodeInteger);
                    indexInfo.setColumns(ColumnModelListToColumnList(model.getRebuildmodel().getColumnListForSolr()));
                    index.update(indexInfo);
                } else if (null != model.getRebuild() && model.getRebuild()) {
                    LOG.info("Rebuild index " + tableName + "." + indexName);

                    try {
                        index.rebuild();
                    } catch (OtsException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_REBUILD, "rebuild index error, may column list is invalid!");
                    }
                } else if (null != model.getTruncate() && model.getTruncate()) {
                    LOG.info("Truncate index " + tableName + "." + indexName);
                    index.truncate();
                } else {
                    LOG.error("Unknown operation of index " + tableName + "." + indexName);
                    throw new OtsException(RestErrorCode.EC_OTS_REST_TABLE_INDEX_INVALID_OP, "Unknown operation of index " + tableName + "." + indexName);
                }
            }
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error("updatw index failed:" + tableName + "." + indexName);
            rMode.setError_code(e.getErrorCode());
            rMode.setErrinfo(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("updatw index failed:" + tableName + "." + indexName);
            errorcode = OtsErrorCode.EC_OTS_INDEX_FAILED_UPDATE;
            rMode.setError_code(errorcode);
            rMode.setErrinfo("updatw index failed:" + tableName + "." + indexName);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("updatw index failed:" + tableName + "." + indexName);
            errorcode = OtsErrorCode.EC_OTS_INDEX_FAILED_UPDATE;
            rMode.setError_code(errorcode);
            rMode.setErrinfo("updatw index failed:" + tableName + "." + indexName);
        }
        LOG.debug("RETURN:" + rMode.toString());
        return rMode;
    }

    public static IndexListModel getIndexlist(PermissionCheckUserInfo userInfo, String tableName) throws Exception {
        IndexListModel model = new IndexListModel();
        List<String> indexNames = new ArrayList<String>();
        try {
            OtsTable tableInfo = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkReadPermission(userInfo, tableInfo.getId());
            }

            List<OtsIndex> listIndex = tableInfo.getAllIndexes();
            for (OtsIndex index : listIndex) {
                indexNames.add(index.getName());
            }
            model.setCount(indexNames.size());
            model.setIndexes(indexNames);
            model.setErrcode(0L);

        } catch (OtsException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw new OtsException(RestErrorCode.EC_OTS_REST_QUERY_INDEX, "failed to query table:" + tableName + " indexes");
        }

        LOG.debug("RETURN:" + model.toString());
        return model;
    }

    /**
     * 创建索引
     *
     * @param userInfo
     * @param tableName
     * @param indexName
     * @param model
     * @return
     * @throws IndexException
     * @throws OtsException
     */
    public static ErrorMode createIndex(PermissionCheckUserInfo userInfo, String tableName, String indexName, IndexInfoModel model) throws IndexException, OtsException {
        LOG.info("Create index " + tableName + "." + indexName);
        long errorcode = 0;
        ErrorMode rMode = new ErrorMode(errorcode);
        try {
            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkEditPermission(userInfo, table.getId());
            }

            if (null != table) {
                OtsIndex index = table.getIndex(indexName);
                if (index != null) {
                    LOG.error("Index " + tableName + "." + indexName + " has exist!");
                    throw new OtsException(OtsErrorCode.EC_OTS_INDEX_ALREADY_EXIST, "Index " + tableName + "." + indexName + " has exist!");
                }

                if (RestConstants.OTS_INDEX_TYPE_HBASE == model.getType()) {
                    SecondaryIndexInfo secIndexInfo = new SecondaryIndexInfo(model.getName());
                    try {
                        List<ColumnModel> columns = model.getColumnList();
                        for (ColumnModel column : columns) {
                            secIndexInfo.addColumn(column.getName(), SecondaryIndexColumn.ValueType.valueOf(column.getType()), column.getMaxLen());
                        }
                    } catch (IOException e) {
                        throw new IndexException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_COLUMN_NAME, e.getMessage());
                    }

                    table.createSecordaryIndex(secIndexInfo).rebuild(OtsConstants.OTS_MAPREDUCE_JAR_PATH, false);
                } else {
                    if (model.getIndexPattern() == null) {
                        LOG.error("Index " + tableName + "." + indexName + " pattern unkonwn!");
                        throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "Index " + tableName + "." + indexName + " pattern unkonwn!");
                    }

                    if (model.getIndexPattern() != OtsConstants.OTS_INDEX_PATTERN_ONLINE && model.getIndexPattern() != OtsConstants.OTS_INDEX_PATTERN_OFFLINE) {
                        LOG.error("Index " + tableName + "." + indexName + " pattern unkonwn!");
                        throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "Index " + tableName + "." + indexName + " pattern invalid!");
                    }

                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.setName(indexName);
                    indexInfo.setStartKey(model.getStartKey());
                    indexInfo.setEndKey(model.getEndKey());
                    indexInfo.setPattern((char) model.getIndexPattern().intValue());
                    indexInfo.setShardNum(model.getShard());
                    indexInfo.setReplicationNum(model.getReplication());
                    Integer maxShardNumPerNodeInteger = Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.SOLR_DEFAULT_MAX_SHARDS_PERNODE, "3"));
                    indexInfo.setMaxShardNumPerNode(maxShardNumPerNodeInteger);
                    indexInfo.setColumns(ColumnModelListToColumnList(model.getColumnListForSolr()));

                    table.createIndex(indexInfo);
                }
            } else {
                LOG.error("Table " + tableName + " no exist!");
                errorcode = OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST;
                rMode.setError_code(errorcode);
                rMode.setErrinfo("Table " + tableName + " no exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Create index " + tableName + "." + indexName + " failed!");
            errorcode = OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE;
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Create index " + tableName + "." + indexName + " failed!");
        } catch (OtsException e) {
            e.printStackTrace();
            rMode.setError_code(e.getErrorCode());
            rMode.setErrinfo(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE;
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Create index " + tableName + "." + indexName + " failed!");
        }
        LOG.debug("RETURN:" + rMode.toString());
        return rMode;
    }

    // queryfrom:0-dashboard, other-rest
    public static IndexInfoModel getIndexInfo(PermissionCheckUserInfo userInfo, String tableName, String indexName, String queryfrom) throws PermissionSqlException, OtsException {
        IndexInfoModel model = new IndexInfoModel();
        try {
            OtsTable tableInfo = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkReadPermission(userInfo, tableInfo.getId());
            }
            if (tableInfo == null) {
                LOG.error("Table " + tableName + "no exist!");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Table " + tableName + "no exist!");
            }
            OtsIndex index = tableInfo.getIndex(indexName);
            if (null != index) {
                model.setName(indexName);
                SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 时间格式化的格式
                model.setCreateTime(sDateFormat.format(index.getCreateTime()));
                model.setLastModify(sDateFormat.format(index.getLastModify()));
                if (queryfrom != null && Integer.parseInt(queryfrom) == 0) {
                    model.setTableId(tableInfo.getId());
                    model.setIndexId(index.getId());
                }

                if (index.getPattern() == (char) OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX) {
                    model.setColumnList(SecondaryColumnListToColumnModelList(index.getSecIndexColumns()));
                    model.setType(RestConstants.OTS_INDEX_TYPE_HBASE);
                } else {
                    model.setShard(index.getShardNum());
                    model.setReplication(index.getReplicationNum());
                    model.setStartKey(index.getStartKey());
                    model.setEndKey(index.getEndKey());
                    model.setIndexPattern((int) index.getPattern());
                    model.setColumnListForSolr(ColumnListToColumnModelList(index.getIndexColumns()));
                    model.setType(RestConstants.OTS_INDEX_TYPE_SOLR);
                }
                model.setErrcode(0L);
            } else {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INDEX_NO_EXIST, String.format("user (id:%d) was not owned index:%s of table:%s!", userInfo.getUserId(), indexName, tableName));
            }
        } catch (PermissionSqlException e) {
            e.printStackTrace();
            LOG.error("check permit filed of " + tableName + "failed ! " + "Error Code (" + OtsErrorCode.EC_OTS_QUERY_PERMISSION_SQL_LABEL + ")");
            throw e;
        } catch (OtsException e) {
            throw e;
        } catch (IOException e) {
            LOG.warn("get index list failed!");
            throw new OtsException(OtsErrorCode.EC_RDS_FAILED_QUERY_INDEX, "get index list failed!");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e) + "  Tablename:" + tableName);
        }

        LOG.debug("RETURN:" + model.toString());
        return model;
    }

    public static IndexQueryResultModel getSecondaryIndexQuery(PermissionCheckUserInfo userInfo, String tableName, String indexName, SecondIndexQueryModel model) throws OtsException, Exception {
        TableInfoModel tableInfo = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
        IndexQueryResultModel results = new IndexQueryResultModel(tableName);
        if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
            PermissionUtil.GetInstance().checkReadPermission(userInfo, tableInfo.getId());
        }

        byte[] startKey = null;
        byte[] endKey = null;
        if (null != model.getHashkey()) {
            if (tableInfo.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), tableInfo.getHashKeyType());
            } else {
                if (model.getRange_key_start() != null) {
                    startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), tableInfo.getHashKeyType(), model.getRange_key_start(), tableInfo.getRangeKeyType());
                } else {
                    startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), tableInfo.getHashKeyType());
                    byte[] add = new byte[1];
                    add[0] = 0x00;
                    startKey = Bytes.add(startKey, add);
                }

                if (model.getRange_key_end() != null) {
                    endKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), tableInfo.getHashKeyType(), model.getRange_key_end(), tableInfo.getRangeKeyType());
                } else {
                    endKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), tableInfo.getHashKeyType());
                    byte[] add = new byte[1];
                    add[0] = (byte) 0xff;
                    endKey = Bytes.add(endKey, add);
                }
            }
        }

        SecondIndexQueryOption query = new SecondIndexQueryOption();
        query.setStartUserTableKey(startKey);
        query.setStopUserTableKey(endKey);
        query.setColumnRanges(model.getColumn_ranges());
        query.setColumns(model.getColumns());
        query.setCursor(model.getCursor_mark());
        if (model.hasLimit()) {
            query.setLimit(model.getLimit());
        }
        if (model.hasOffset()) {
            query.setOffset(model.getOffset());
        }

        OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
        OtsSecondaryIndex index = table.getSecondaryIndex(indexName);
        if (null == index) {
            throw new IOException("Index not exist!");
        }
        query.setIndex(index.getIndexInfo());
        RecordResult result = index.getRecords(query);

        results.setNextCursorMark(result.getNext_rowkey());
        results.setMatchCount(result.getCount());// !!match records
        for (RowRecord rowRecord : result.getListRecords()) {
            Map<String, Object> r = new HashMap<String, Object>();
            // rowkey
            byte[] primaryKey = rowRecord.getRowkey();
            byte[] byteHashkey = PrimaryKeyUtil.getHashKey(primaryKey);
            if (tableInfo.getHashKeyType() == PrimaryKeyUtil.TYPE_NUMBER) {
                r.put(RestConstants.OTS_HASHKEY, String.valueOf(Bytes.toLong(byteHashkey)));
            } else if (tableInfo.getHashKeyType() == PrimaryKeyUtil.TYPE_STRING) {
                r.put(RestConstants.OTS_HASHKEY, Bytes.toString(byteHashkey));
            } else if (tableInfo.getHashKeyType() == PrimaryKeyUtil.TYPE_BINARY) {
                r.put(RestConstants.OTS_HASHKEY, Hex.encodeHexString(byteHashkey));
            }
            if (tableInfo.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                byte[] byteRangekey = PrimaryKeyUtil.getRangeKey(primaryKey);
                if (tableInfo.getRangeKeyType() == PrimaryKeyUtil.TYPE_NUMBER) {
                    r.put(RestConstants.OTS_RANGEKEY, String.valueOf(Bytes.toLong(byteRangekey)));
                } else if (tableInfo.getRangeKeyType() == PrimaryKeyUtil.TYPE_STRING) {
                    r.put(RestConstants.OTS_RANGEKEY, Bytes.toString(byteRangekey));
                } else if (tableInfo.getRangeKeyType() == PrimaryKeyUtil.TYPE_BINARY) {
                    r.put(RestConstants.OTS_RANGEKEY, Hex.encodeHexString(byteRangekey));
                }
            }
            // other column
            List<RowCell> cells = rowRecord.getCellList();
            for (RowCell rowCell : cells) {
                //if (0 == Bytes.compareTo(rowCell.getName(), Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
                //	continue;
                //}
                r.put(Bytes.toString(rowCell.getName()), Bytes.toString(rowCell.getValue()));
            }
            results.add(r);
        }
        results.setErrcode(0L);

        if (results.getCount() < 100) {
            String retString = results.toString();
            if (retString.length() > 1000) {
                LOG.debug("RETURN (Part):\n" + retString.substring(0, 999));
            } else {
                LOG.debug("RETURN:" + retString);
            }
        } else {
            LOG.debug("RETURN: records..., count:" + results.getCount() + ", match:" + results.getMatchCount());
        }

        return results;
    }

    public static IndexQueryResultModel getSolrIndexQuery(PermissionCheckUserInfo userInfo, String tableName, String indexName, IndexQueryModel model) throws OtsException {
        IndexQueryResultModel results = new IndexQueryResultModel(tableName);

        try {
            OtsTable tableInfo = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkReadPermission(userInfo, info.getId());
            }

            OtsIndex index = tableInfo.getIndexNoSafe(indexName);
            RecordResult result = index.getRecordsByCursorMark(model.getQuery(), model.getFiltersAsList(), model.getColumnsAsList(), model.getOrdersAsList(),
                    model.getLimit(), model.getOffset(), model.getCursor_mark());

            results.setCount(result.getCount());// !!
            results.setMatchCount(index.getMatchCount());// !!
            results.setNextCursorMark(index.getNextCursorMark());// !!

            for (RowRecord rowRecord : result.getListRecords()) {
                Map<String, Object> r = new HashMap<String, Object>();
                // rowkey
                byte[] primaryKey = rowRecord.getRowkey();
                byte[] byteHashkey = PrimaryKeyUtil.getHashKey(primaryKey);
                if (info.getHashKeyType() == PrimaryKeyUtil.TYPE_NUMBER) {
                    r.put(RestConstants.OTS_HASHKEY, String.valueOf(Bytes.toLong(byteHashkey)));
                } else if (info.getHashKeyType() == PrimaryKeyUtil.TYPE_STRING) {
                    r.put(RestConstants.OTS_HASHKEY, Bytes.toString(byteHashkey));
                } else if (info.getHashKeyType() == PrimaryKeyUtil.TYPE_BINARY) {
                    r.put(RestConstants.OTS_HASHKEY, Hex.encodeHexString(byteHashkey));
                }
                if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                    byte[] byteRangekey = PrimaryKeyUtil.getRangeKey(primaryKey);
                    if (info.getRangeKeyType() == PrimaryKeyUtil.TYPE_NUMBER) {
                        r.put(RestConstants.OTS_RANGEKEY, String.valueOf(Bytes.toLong(byteRangekey)));
                    } else if (info.getRangeKeyType() == PrimaryKeyUtil.TYPE_STRING) {
                        r.put(RestConstants.OTS_RANGEKEY, Bytes.toString(byteRangekey));
                    } else if (info.getRangeKeyType() == PrimaryKeyUtil.TYPE_BINARY) {
                        r.put(RestConstants.OTS_RANGEKEY, Hex.encodeHexString(byteRangekey));
                    }
                }
                // other column
                List<RowCell> cells = rowRecord.getCellList();
                for (RowCell rowCell : cells) {
                    //if (0 == Bytes.compareTo(rowCell.getName(), Bytes.toBytes(OtsConstants.DEFAULT_ROWKEY_NAME))) {
                    //	continue;
                    //}
                    r.put(Bytes.toString(rowCell.getName()), Bytes.toString(rowCell.getValue()));
                }
                results.add(r);
            }

        } catch (SolrServerException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_QUERY, e.getMessage());
        } catch (Exception e) {
            throw new OtsException(OtsErrorCode.EC_OTS_INDEX_FAILED_QUERY, e.getMessage());
        }

        results.setErrcode(0L);
        if (results.getCount() < 100) {
            String retString = results.toString();
            if (retString.length() > 1000) {
                LOG.debug("RETURN (Part):\n" + retString.substring(0, 999));
            } else {
                LOG.debug("RETURN:" + retString);
            }
        } else {
            LOG.debug("RETURN: records..., count:" + results.getCount() + ", match:" + results.getMatchCount());
        }

        return results;
    }

    /**
     * get all index info
     *
     * @param userInfo
     * @param tableName
     * @return
     * @throws Exception
     */
    public static IndexInfoListModel getAllIndexesInfo(PermissionCheckUserInfo userInfo, String tableName) throws OtsException {
        IndexInfoListModel listModel = new IndexInfoListModel();
        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().checkReadPermission(userInfo, info.getId());
            }

            OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (table == null) {
                LOG.error("Table " + tableName + "no exist!");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "Table " + tableName + " no exist!");
            }
            List<OtsIndex> indexList = table.getAllIndexes();
            Map<String, IndexInfoModel> mapHbaseIndex = new HashMap<String, IndexInfoModel>();
            for (OtsIndex index : indexList) {
                if (!(index.getPattern() == (char) OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX)) {
                    IndexInfoModel model = new IndexInfoModel();
                    model.setTableId(table.getId());
                    model.setIndexId(index.getId());
                    model.setType(RestConstants.OTS_INDEX_TYPE_SOLR);
                    model.setName(index.getName());
                    model.setShard(index.getShardNum());
                    model.setReplication(index.getReplicationNum());
                    model.setStartKey(index.getStartKey());
                    model.setEndKey(index.getEndKey());
                    model.setIndexPattern((int) index.getPattern());
                    model.setColumnListForSolr(ColumnListToColumnModelList(index.getIndexColumns()));

                    SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 时间格式化的格式
                    model.setCreateTime(sDateFormat.format(index.getCreateTime()));
                    model.setLastModify(sDateFormat.format(index.getLastModify()));

                    listModel.add(model);
                } else {
                    IndexInfoModel model = new IndexInfoModel();
                    model.setType(RestConstants.OTS_INDEX_TYPE_HBASE);
                    model.setName(index.getName());
                    model.setTableId(table.getId());
                    model.setIndexId(index.getId());

                    SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 时间格式化的格式
                    model.setCreateTime(sDateFormat.format(index.getCreateTime()));
                    model.setLastModify(sDateFormat.format(index.getLastModify()));
                    mapHbaseIndex.put(index.getName(), model);
                }
            }

            List<SecondaryIndexInfo> secIndexList = table.getAllSecondaryIndexeInfos();
            for (SecondaryIndexInfo secIndex : secIndexList) {
                IndexInfoModel model = new IndexInfoModel();
                model.setName(secIndex.getName());
                model.setType(RestConstants.OTS_INDEX_TYPE_HBASE);
                model.setColumnList(SecondaryColumnListToColumnModelList(secIndex.getColumns()));

                if (mapHbaseIndex.containsKey(secIndex.getName())) {
                    model.setCreateTime(mapHbaseIndex.get(secIndex.getName()).getCreateTime());
                    model.setLastModify(mapHbaseIndex.get(secIndex.getName()).getLastModify());
                    model.setTableId(mapHbaseIndex.get(secIndex.getName()).getTableId());
                    model.setIndexId(mapHbaseIndex.get(secIndex.getName()).getIndexId());
                }
                listModel.add(model);
            }
        } catch (OtsException e) {
            throw e;
        } catch (IOException e) {
            LOG.warn("get index list failed!");
            throw new OtsException(OtsErrorCode.EC_RDS_FAILED_QUERY_INDEX, "get index list failed!");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("getAllIndexesInfo failed:" + "Table Name: " + tableName + "UserId: " + userInfo.getUserId());
        }
        listModel.setErrcode(0L);
        listModel.setCount(listModel.getIndexinfolist().size());

        LOG.debug("RETURN:" + listModel.toString());
        return listModel;
    }

    // ///////////////////////

    /**
     * 获取索引操作配置
     *
     * @param userid
     * @param tablename
     * @return
     */
    public static String getConfig(long userid, long tenantid, String tablename, String indexname) throws OtsException {
        return ConfigUtil.getInstance().getOtsAdmin().getIndexConfig(userid, tenantid, tablename, indexname);
    }

    /**
     * 保持索引操作配置
     *
     * @param userid
     * @param tablename
     * @param config
     * @return
     * @throws OtsException
     */
    public static void saveConfig(long userid, long tenantid, String tablename, String indexname, String config) throws OtsException {
        ConfigUtil.getInstance().getOtsAdmin().saveIndexConfig(userid, tenantid, tablename, indexname, config);
    }
}
