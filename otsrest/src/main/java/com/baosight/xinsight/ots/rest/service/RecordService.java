package com.baosight.xinsight.ots.rest.service;

import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.OtsIndex;
import com.baosight.xinsight.ots.client.OTSTable;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.client.table.RecordQueryOption;
import com.baosight.xinsight.ots.client.table.RecordResult;
import com.baosight.xinsight.ots.client.table.RowCell;
import com.baosight.xinsight.ots.client.table.RowRecord;
import com.baosight.xinsight.ots.common.util.PrimaryKeyUtil;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestConstants;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.ots.rest.model.RecordListModel;
import com.baosight.xinsight.ots.rest.model.RecordQueryResultModel;
import com.baosight.xinsight.ots.rest.model.TableInfoModel;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.model.operate.KeyModel;
import com.baosight.xinsight.ots.rest.model.operate.RecordQueryModel;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.TableConfigUtil;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RecordService {
    private static final Logger LOG = Logger.getLogger(RecordService.class);

    private static Map<String, byte[]> getHbaseAttributes(int queryFrom) {
        if (queryFrom == RestConstants.QUERY_FROM_TYPE_BOARD) {
            Map<String, byte[]> hbase_attributes = new HashMap<String, byte[]>();
            hbase_attributes.put(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
            return hbase_attributes;
        }
        return null;
    }

    /**
     * 获取表数据
     *
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     * @throws TableException
     * @throws ConfigException
     * @throws OtsException
     */
    public static RecordQueryResultModel getRecords(PermissionCheckUserInfo userInfo, String tableName, RecordQueryModel model) throws TableException, ConfigException, OtsException {
        RecordResult result = null;

        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, info.getId(), PermissionUtil.PermissionOpesration.READ);
            }

            if (!model.isValidQuery(info.getKeyType())) {
                LOG.error("it is not a valid query.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, "Failed to query record, it is not a valid query.");
            }

            RecordQueryOption query = new RecordQueryOption(model.getColumnsAsList(), model.getLimit(), model.getOffset(), model.getCursor_mark(), model.getDescending(), null,
                    getHbaseAttributes(model.getQuery_from()));

            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            RecordQueryResultModel resultModel = new RecordQueryResultModel(tableName);
            if (model.whichRange(info.getKeyType()) == RestConstants.RANGE_OPERATE) {
                Filter rowFilter = genRowkeyHashFilter(model.getHashkey(), info.getHashKeyType());
                query.setFilter(rowFilter);

                byte[] startKey = null;
                if (model.getRange_key_start() != null) {
                    startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_start(), info.getRangeKeyType());
                }
                byte[] endKey = null;
                if (model.getRange_key_end() != null) {
                    endKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_end(), info.getRangeKeyType());
                }

                result = table.getRecords(startKey, endKey, query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.RANGE_PREFIX_OPERATE) {
                Filter rowFilter = genRowkeyPrefixFilter(model.getHashkey(), info.getHashKeyType(), model.getRangekey_prefix());
                query.setFilter(rowFilter);

                byte[] startKey = null;
                if (model.getRange_key_start() != null) {
                    startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_start(), info.getRangeKeyType());
                }
                byte[] endKey = null;
                if (model.getRange_key_end() != null) {
                    endKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_end(), info.getRangeKeyType());
                }
                result = table.getRecords(startKey, endKey, query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.PREFIX_OPERATE) {
                if (info.getKeyType() != PrimaryKeyUtil.ROWKEY_TYPE_RANGE || info.getRangeKeyType() != PrimaryKeyUtil.TYPE_STRING) {
                    LOG.error("Failed to query record, " + RestConstants.Query_rangekey_prefix + " query only surport range key with string type!");
                    throw new OtsException(RestErrorCode.EC_OTS_REST_INVALID_QUERY_TYPE, "Failed to query record, " + RestConstants.Query_rangekey_prefix
                            + " query only surport range key with string type!");
                }
                Filter rowFilter = genRowkeyPrefixFilter(model.getHashkey(), info.getHashKeyType(), model.getRangekey_prefix());
                query.setFilter(rowFilter);
                result = table.getRecords(query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.KEY_OPERATE) {
                byte[] rowKey = null;
                if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                    rowKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType());
                } else {
                    rowKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRangekey(), info.getRangeKeyType());
                }

                result = table.getRecord(rowKey, model.getColumnsAsList(), getHbaseAttributes(model.getQuery_from()));
            } else if (model.whichRange(info.getKeyType()) == RestConstants.MULTIKEY_OPERATE) {
                List<byte[]> keys = trans2Keys(model.get_full_keys(), info);
                result = table.getRecords(keys, query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.HASH_OPERATE) {
                if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                    byte[] rowKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType());
                    result = table.getRecord(rowKey, model.getColumnsAsList(), getHbaseAttributes(model.getQuery_from()));
                } else {
                    Filter rowFilter = genRowkeyHashFilter(model.getHashkey(), info.getHashKeyType());
                    query.setFilter(rowFilter);
                    result = table.getRecords(query);
                }
            } else if (model.whichRange(info.getKeyType()) == RestConstants.ANYKEY_OPERATE) {
                result = table.getRecords(query);
            } else {
                LOG.error("Failed to query record because invalid query type!");
                throw new OtsException(RestErrorCode.EC_OTS_REST_INVALID_QUERY_TYPE, "Failed to query record because invalid query type!");
            }

            resultModel.setCount(result.getCount());// !!match records
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
                resultModel.add(r);
            }

            resultModel.setErrcode(0L);
            if (model.getCursor_mark() != null) { // important
                resultModel.setRangekey_next_cursormark(result.getNext_rowkey());
            }

            if (result.size() < 100) {
                String retString = resultModel.toString();
                if (retString.length() > 1000) {
                    LOG.debug("RETURN (Part):\n" + retString.substring(0, 999));
                } else {
                    LOG.debug("RETURN:" + retString);
                }
            } else {
                LOG.debug("RETURN: records..., count:" + resultModel.getCount());
            }

            return resultModel;
        } catch (OtsException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_QUERY, e.getMessage());
        }
    }

    /**
     * 获取文件列（mob）
     *
     * @throws TableException
     * @throws IOException
     * @throws OtsException
     */
    public static byte[] getRecordFile(PermissionCheckUserInfo userInfo, String tableName, String hashkey, String rangekey, String column) throws Exception {
        try {
            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, table.getId(), PermissionUtil.PermissionOpesration.READ);
            }
            byte[] rowKey = null;
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                rowKey = PrimaryKeyUtil.buildPrimaryKey(hashkey, info.getHashKeyType());
            } else if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                rowKey = PrimaryKeyUtil.buildPrimaryKey(hashkey, info.getHashKeyType(), rangekey, info.getRangeKeyType());
            } else {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_GET, "unknown rowkey type, Failed to get record file!");
            }
            return table.getRecordFile(rowKey, Bytes.toBytes(column));
        } catch (IOException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_GET, "Failed to get record file!");
        } catch (TableException e) {
            throw e;
        } catch (OtsException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_GET, "Failed to get record file!");
        }
    }

    /**
     * 上传文件列（mob）
     *
     * @throws TableException
     * @throws IOException
     */
    public static ErrorMode putRecordFile(PermissionCheckUserInfo userInfo, String tableName, String hashkey, String rangekey, String column, byte[] data) throws Exception {
        long errorcode = 0;
        ErrorMode rMode = new ErrorMode(errorcode);
        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, info.getId(), PermissionUtil.PermissionOpesration.EDIT);
            }
            byte[] rowKey = null;
            if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                rowKey = PrimaryKeyUtil.buildPrimaryKey(hashkey, info.getHashKeyType());
            } else if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                rowKey = PrimaryKeyUtil.buildPrimaryKey(hashkey, info.getHashKeyType(), rangekey, info.getRangeKeyType());
            } else {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_INSERT, "unknown rowkey type, Failed to insert record file!");
            }
            table.putRecordFile(rowKey, Bytes.toBytes(column), data);
        } catch (TableException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            rMode.setErrinfo(e.getMessage());
            rMode.setError_code(e.getErrorCode());
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            rMode.setErrinfo(e.getMessage());
            rMode.setError_code(e.getErrorCode());
        } catch (Exception e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_FILE_INSERT, "Failed to insert record file!");
        }


        LOG.debug("RETURN:" + rMode.toString());
        return rMode;
    }

    /**
     * 删除表数据
     *
     * @return 0-all successful, other-failed
     * @throws Exception
     */
    public static ErrorMode deleteRecords(PermissionCheckUserInfo userInfo, String tableName, RecordQueryModel model) throws Exception {
        long errorcode = 0;
        ErrorMode rMode = new ErrorMode(errorcode);
        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, info.getId(), PermissionUtil.PermissionOpesration.EDIT);
            }
            if (!model.isValidQuery(info.getKeyType())) {
                LOG.error("it is not a valid delete.");
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE, "Failed to delete record, it is not a valid delete.");
            }

            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            RecordQueryOption query = new RecordQueryOption(model.getColumnsAsList(), model.getLimit(), model.getOffset(), model.getCursor_mark(), model.getDescending(), null,
                    getHbaseAttributes(model.getQuery_from()));

            if (model.whichRange(info.getKeyType()) == RestConstants.RANGE_OPERATE) {
                Filter rowFilter = genRowkeyHashFilter(model.getHashkey(), info.getHashKeyType());
                query.setFilter(rowFilter);

                byte[] startKey = null;
                if (model.getRange_key_start() != null) {
                    startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_start(), info.getRangeKeyType());
                }
                byte[] endKey = null;
                if (model.getRange_key_end() != null) {
                    endKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_end(), info.getRangeKeyType());
                }

                table.deleteRecords(startKey, endKey, query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.RANGE_PREFIX_OPERATE) {
                Filter rowFilter = genRowkeyPrefixFilter(model.getHashkey(), info.getHashKeyType(), model.getRangekey_prefix());
                query.setFilter(rowFilter);

                byte[] startKey = null;
                if (model.getRange_key_start() != null) {
                    startKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_start(), info.getRangeKeyType());
                }
                byte[] endKey = null;
                if (model.getRange_key_end() != null) {
                    endKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRange_key_end(), info.getRangeKeyType());
                }
                table.deleteRecords(startKey, endKey, query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.PREFIX_OPERATE) {
                if (info.getKeyType() != PrimaryKeyUtil.ROWKEY_TYPE_RANGE || info.getRangeKeyType() != PrimaryKeyUtil.TYPE_STRING) {
                    LOG.error("Failed to delete record, " + RestConstants.Query_rangekey_prefix + " delete only surport range key with string type!");
                    throw new OtsException(RestErrorCode.EC_OTS_REST_INVALID_DELETE_TYPE, "Failed to delete record, " + RestConstants.Query_rangekey_prefix
                            + " delete only surport range key with string type!");
                }
                Filter rowFilter = genRowkeyPrefixFilter(model.getHashkey(), info.getHashKeyType(), model.getRangekey_prefix());
                query.setFilter(rowFilter);
                table.deleteRecords(query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.KEY_OPERATE) {
                byte[] rowKey = null;
                if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                    rowKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType());
                } else {
                    rowKey = PrimaryKeyUtil.buildPrimaryKey(model.getHashkey(), info.getHashKeyType(), model.getRangekey(), info.getRangeKeyType());
                }
                table.deleteRecord(rowKey);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.MULTIKEY_OPERATE) {
                List<byte[]> keys = trans2Keys(model.get_full_keys(), info);
                table.deleteRecords(keys);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.HASH_OPERATE) {
                Filter rowFilter = genRowkeyHashFilter(model.getHashkey(), info.getHashKeyType());
                query.setFilter(rowFilter);
                table.deleteRecords(query);
            } else if (model.whichRange(info.getKeyType()) == RestConstants.TIMERANGE_OPERATE) {
                table.deleteRecords(model.getStart_time(), model.getEnd_time());
            } else {
                errorcode = RestErrorCode.EC_OTS_REST_INVALID_DELETE_TYPE;
                LOG.error("Failed to delete record because invalid delete type!");

                rMode.setError_code(errorcode);
                rMode.setErrinfo("Failed to delete record because invalid delete type!");
            }

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER;
            LOG.error("Failed to delete records because hbase master no running!");
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to delete records because hbase master no running!");
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK;
            LOG.error("Failed to delete because can not connecto to zookeeper!");

            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to delete because can not connecto to zookeeper!");
        } catch (IOException e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE;
            LOG.error("Failed to delete records!" + e.getMessage());
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to delete records!" + e.getMessage());
        } catch (TableException e) {
            e.printStackTrace();
            errorcode = e.getErrorCode();
            LOG.error(e.getMessage());
            rMode.setError_code(errorcode);
            rMode.setErrinfo(e.getMessage());
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error("Failed to truncate table for!\n" + e.getMessage());
            rMode.setError_code(e.getErrorCode());
            rMode.setErrinfo(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_RECORD_DELETE;
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to delete records!" + e.getMessage());
        }

        LOG.debug("RETURN:" + rMode.toString());
        return rMode;
    }

    private static Filter genRowkeyPrefixFilter(Object hashkey, int hashKeyType, String rangekey_prefix) throws Exception {
        byte[] hashKey = PrimaryKeyUtil.buildPrimaryKey(hashkey, hashKeyType);
        byte[] rangekeyPrefix = Bytes.toBytes(rangekey_prefix);
        byte[] prefixKey = new byte[hashKey.length + rangekeyPrefix.length];
        Bytes.putBytes(prefixKey, 0, hashKey, 0, hashKey.length);
        Bytes.putBytes(prefixKey, hashKey.length, rangekeyPrefix, 0, rangekeyPrefix.length);
        Filter rowFilter = new PrefixFilter(prefixKey);
        return rowFilter;
    }

    private static Filter genRowkeyHashFilter(Object hashkey, int hashKeyType) throws Exception {
        byte[] hashKey = PrimaryKeyUtil.buildPrimaryKey(hashkey, hashKeyType);
        Filter rowFilter = new PrefixFilter(hashKey);
        return rowFilter;
    }

    private static List<byte[]> trans2Keys(List<KeyModel> querykeys, TableInfoModel info) throws Exception {
        List<byte[]> keysList = new ArrayList<byte[]>();
        for (KeyModel m : querykeys) {
            byte[] key = null;
            if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                key = PrimaryKeyUtil.buildPrimaryKey(m.getHashkey(), info.getHashKeyType());
            } else if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                key = PrimaryKeyUtil.buildPrimaryKey(m.getHashkey(), info.getHashKeyType(), m.getRangekey(), info.getRangeKeyType());
            }
            keysList.add(key);
        }

        return keysList;
    }

    /**
     * 插入表数据
     *
     * @throws IOException
     * @throws TableException
     * @throws OtsException
     */
    public static ErrorMode insertRecords(PermissionCheckUserInfo userInfo, String tableName, RecordListModel model) throws Exception {
        List<RowRecord> records = new ArrayList<RowRecord>();
        long errorcode = 0;
        ErrorMode rMode = new ErrorMode(errorcode);
        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, info.getId(), PermissionUtil.PermissionOpesration.EDIT);
            }

            for (int i = 1; i <= model.size(); ++i) {
                RowRecord rec = new RowRecord();
                Map<String, Object> record = model.getRec(i - 1);

                if (!record.containsKey(RestConstants.OTS_HASHKEY)) {
                    continue;
                }
                String hash_key = record.get(RestConstants.OTS_HASHKEY).toString().trim();
                if (hash_key.isEmpty()) {
                    continue;
                }

                byte[] rowkey = null;
                if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                    rowkey = PrimaryKeyUtil.buildPrimaryKey(hash_key, info.getHashKeyType());
                } else if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                    if (!record.containsKey(RestConstants.OTS_RANGEKEY)) {
                        continue;
                    }
                    String range_key = record.get(RestConstants.OTS_RANGEKEY).toString().trim();
                    if (range_key.isEmpty()) {
                        continue;
                    }
                    rowkey = PrimaryKeyUtil.buildPrimaryKey(hash_key, info.getHashKeyType(), range_key, info.getRangeKeyType());
                } else {
                    continue;
                }

                rec.setRowkey(rowkey);
                for (String cell : record.keySet()) {
                    String cellkey = cell.trim();
                    String cellvalue = record.get(cell).toString().trim();
                    // not hash_key and range_key, also column name and value not empty
                    if (!cellkey.isEmpty() && !cellkey.equals(RestConstants.OTS_HASHKEY) && !cellkey.equals(RestConstants.OTS_RANGEKEY)) {
                        RowCell c = new RowCell(Bytes.toBytes(cellkey), Bytes.toBytes(cellvalue));
                        rec.addCell(c);
                    }
                }
                records.add(rec);
            }
            if (records.size() != model.size()) {
                if (records.size() <= 0) {
                    errorcode = RestErrorCode.EC_OTS_REST_INVALID_RECORDS;
                    LOG.error("Failed to insert records, because all invalid records without primarykey!");

                    rMode.setError_code(errorcode);
                    rMode.setErrinfo("Failed to insert records, because all invalid records without primarykey!");
                    LOG.debug("RETURN:" + rMode.toString());
                    return rMode;
                }

                errorcode = RestErrorCode.EC_OTS_REST_PARTIAL_INVALID_RECORDS;
                LOG.error("partial records invalid to insert without primarykey, just ignored!");

                rMode.setError_code(errorcode);
                rMode.setErrinfo("partial records invalid to insert without primarykey, just ignored!");
                LOG.debug("RETURN:" + rMode.toString());
                return rMode;
            }

            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            table.insertRecords(records);

        } catch (TableException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            rMode.setError_code(e.getErrorCode());
            rMode.setErrinfo(e.getMessage());
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error("Failed to truncate table for!\n" + e.getMessage());
            rMode.setError_code(e.getErrorCode());
            rMode.setErrinfo(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_RECORD_INSERT;
            LOG.error("Failed to insert records!" + e.getMessage());
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to insert records!" + e.getMessage());
        }

        LOG.debug("RETURN:" + rMode.toString());
        return rMode;
    }

    /**
     * 更新表数据
     *
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     * @throws TableException
     */
    public static ErrorMode updateRecords(PermissionCheckUserInfo userInfo, String tableName, RecordListModel model) throws Exception {
        List<RowRecord> records = new ArrayList<RowRecord>();
        long errorcode = 0;
        ErrorMode rMode = new ErrorMode(errorcode);
        try {
            TableInfoModel info = TableConfigUtil.getTableConfig(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, info.getId(), PermissionUtil.PermissionOpesration.EDIT);
            }

            for (int i = 1; i <= model.size(); ++i) {
                RowRecord rec = new RowRecord();
                Map<String, Object> record = model.getRec(i - 1);

                if (!record.containsKey(RestConstants.OTS_HASHKEY)) {
                    continue;
                }
                String hash_key = record.get(RestConstants.OTS_HASHKEY).toString().trim();
                if (hash_key.isEmpty()) {
                    continue;
                }

                byte[] rowkey = null;
                if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_HASH) {
                    rowkey = PrimaryKeyUtil.buildPrimaryKey(hash_key, info.getHashKeyType());
                } else if (info.getKeyType() == PrimaryKeyUtil.ROWKEY_TYPE_RANGE) {
                    if (!record.containsKey(RestConstants.OTS_RANGEKEY)) {
                        continue;
                    }
                    String range_key = record.get(RestConstants.OTS_RANGEKEY).toString().trim();
                    if (range_key.isEmpty()) {
                        continue;
                    }
                    rowkey = PrimaryKeyUtil.buildPrimaryKey(hash_key, info.getHashKeyType(), range_key, info.getRangeKeyType());
                } else {
                    continue;
                }

                rec.setRowkey(rowkey);
                for (String cell : record.keySet()) {
                    String cellkey = cell.trim();
                    String cellvalue = record.get(cell).toString().trim();
                    // not hash_key and range_key, also column name and value not empty
                    if (!cellkey.isEmpty() && !cellkey.equals(RestConstants.OTS_HASHKEY) && !cellkey.equals(RestConstants.OTS_RANGEKEY)) {
                        RowCell c = new RowCell(Bytes.toBytes(cellkey), Bytes.toBytes(cellvalue));
                        rec.addCell(c);
                    }
                }
                records.add(rec);
            }
            if (records.size() != model.size()) {
                if (records.size() <= 0) {
                    errorcode = RestErrorCode.EC_OTS_REST_INVALID_RECORDS;
                    LOG.error("Failed to update records, because all invalid records without primarykey!");

                    rMode.setError_code(errorcode);
                    rMode.setErrinfo("Failed to update records, because all invalid records without primarykey!");
                    LOG.debug("RETURN:" + rMode.toString());
                    return rMode;
                }

                errorcode = RestErrorCode.EC_OTS_REST_PARTIAL_INVALID_RECORDS;
                LOG.error("partial records invalid to update without primarykey, just ignored!");

                rMode.setError_code(errorcode);
                rMode.setErrinfo("partial records invalid to update without primarykey, just ignored!");
                LOG.debug("RETURN:" + rMode.toString());
                return rMode;
            }

            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            table.updateRecords(records);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER;
            LOG.error("Failed to update records because hbase master no running!");
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to update records because hbase master no running!");
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            errorcode = OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK;
            LOG.error("Failed to update because can not connecto to zookeeper!");
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to update because can not connecto to zookeeper!");
        } catch (IOException e) {
            e.printStackTrace();
            errorcode = RestErrorCode.EC_OTS_REST_RECORD_UPDATE;
            LOG.error("Failed to update records!" + e.getMessage());
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to update records!" + e.getMessage());
        } catch (TableException e) {
            e.printStackTrace();
            errorcode = e.getErrorCode();
            LOG.error(e.getMessage());
            rMode.setError_code(errorcode);
            rMode.setErrinfo(e.getMessage());
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error("Failed to truncate table for!\n" + e.getMessage());
            rMode.setError_code(e.getErrorCode());
            rMode.setErrinfo(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            errorcode = RestErrorCode.EC_OTS_REST_RECORD_UPDATE;
            LOG.error("Failed to update records!" + e.getMessage());
            rMode.setError_code(errorcode);
            rMode.setErrinfo("Failed to update records!" + e.getMessage());
        }

        LOG.debug("RETURN:" + rMode.toString());
        return rMode;
    }

    public static Object deleteAllRecords(PermissionCheckUserInfo userInfo, String tableName) throws Exception {
        ErrorMode rmodel = new ErrorMode(0L);

        try {
            OTSTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userInfo.getUserId(), userInfo.getTenantId(), tableName);
            if (userInfo.getTenantId() != null && userInfo.getUserId() != null) {
                PermissionUtil.GetInstance().otsPermissionHandler(userInfo, table.getId(), PermissionUtil.PermissionOpesration.EDIT);
            }
            if (table == null) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, "table not exist!");
            }
            table.truncate();

            // truncate the online index data
            List<OtsIndex> listIndex = table.getAllIndexes();
            for (OtsIndex index : listIndex) {
                if (OtsConstants.OTS_INDEX_PATTERN_ONLINE == (int) index.getPattern()) {
                    index.truncate();
                }
            }
        } catch (TableException e) {
            e.printStackTrace();
            LOG.error("Failed to truncate table for!\n" + e.getMessage());
            rmodel.setError_code(e.getErrorCode());
            rmodel.setErrinfo(e.getMessage());
        } catch (OtsException e) {
            e.printStackTrace();
            LOG.error("Failed to truncate table for!\n" + e.getMessage());
            rmodel.setError_code(e.getErrorCode());
            rmodel.setErrinfo(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            long errorcode = OtsErrorCode.EC_OTS_STORAGE_TABLE_UPDATE;
            LOG.error("Failed to truncate table!" + e.getMessage());
            rmodel.setError_code(errorcode);
            rmodel.setErrinfo(e.getMessage());
        }
        LOG.debug("RETURN:" + rmodel.toString());
        return rmodel;
    }
}
