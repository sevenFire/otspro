package com.baosight.xinsight.ots.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.config.ConfigReader;
import com.baosight.xinsight.ots.client.table.*;
import com.baosight.xinsight.utils.BytesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.filter.Filter;

import com.baosight.xinsight.ots.OtsConfiguration;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.client.exception.PermissionSqlException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.client.index.IndexConfigurator;
import com.baosight.xinsight.ots.client.metacfg.Configurator;
import com.baosight.xinsight.ots.client.metacfg.Index;
import com.baosight.xinsight.ots.client.metacfg.IndexProfile;
import com.baosight.xinsight.ots.client.metacfg.Table;
import com.baosight.xinsight.ots.client.metacfg.TableProfile;
import com.baosight.xinsight.ots.client.util.ConnectionUtil;
import com.baosight.xinsight.ots.common.table.TableMetrics;
import com.baosight.xinsight.ots.common.util.SecondaryIndexUtil;
import com.baosight.xinsight.ots.exception.OtsException;


/**
 * OtsAdmin
 *
 * @author huangming
 * @created 2015.08.06
 */
public class OtsAdmin {
    private static final Logger LOG = Logger.getLogger(OtsAdmin.class);

    private OtsConfiguration conf = null;

    public OtsAdmin(OtsConfiguration conf) throws IOException, NumberFormatException, ConfigException, OtsException, Exception {
        this.conf = conf;

        try {
            String ZOOKEEPER_QUORUM = conf.getProperty(OtsConstants.ZOOKEEPER_QUORUM);
            String ZOOKEEPER_TIMEOUT = conf.getProperty(OtsConstants.ZOOKEEPER_TIMEOUT);
            if (ZOOKEEPER_QUORUM == null || ZOOKEEPER_TIMEOUT == null) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INVALID_INITPARAM_HBASE,
                        "invalid input parameter for zookeeper:" + ZOOKEEPER_QUORUM + ZOOKEEPER_TIMEOUT);
            }
            String POSTGRES_QUORUM = conf.getProperty(OtsConstants.POSTGRES_QUORUM);
            String POSTGRES_PORT = conf.getProperty(OtsConstants.POSTGRES_PORT);
            String POSTGRES_DBNAME = conf.getProperty(OtsConstants.POSTGRES_DBNAME);
            String POSTGRES_USERNAME = conf.getProperty(OtsConstants.POSTGRES_USERNAME);
            String POSTGRES_PASSWORD = conf.getProperty(OtsConstants.POSTGRES_PASSWORD);
            if (POSTGRES_QUORUM == null || POSTGRES_DBNAME == null
                    || POSTGRES_USERNAME == null || POSTGRES_PASSWORD == null) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INVALID_INITPARAM_RDS,
                        "invalid input parameter for postgres:"
                                + POSTGRES_QUORUM + "," + POSTGRES_PORT
                                + "," + POSTGRES_DBNAME + "," + POSTGRES_USERNAME);
            }
            if (POSTGRES_PORT == null) {
                LOG.warn(OtsConstants.POSTGRES_PORT + "not set postgres port, try to use 5432 default port");
                POSTGRES_PORT = "5432";
            }

            // hbase
            Configuration hbase_cfg = HBaseConfiguration.create();
            hbase_cfg.setStrings(HConstants.ZOOKEEPER_QUORUM, ZOOKEEPER_QUORUM);
            hbase_cfg.setStrings(HConstants.ZK_SESSION_TIMEOUT, ZOOKEEPER_TIMEOUT);
            ConnectionUtil.init(hbase_cfg);

            // rdb
            Configurator.init(POSTGRES_QUORUM, Integer.valueOf(POSTGRES_PORT),
                    POSTGRES_DBNAME, POSTGRES_USERNAME, POSTGRES_PASSWORD);

            //hbase-indexer
            String INDEX_CONFIG_HOME = conf.getProperty(OtsConstants.INDEX_CONFIG_HOME);
            String HBASE_INDEXER_QUORUM = conf.getProperty(OtsConstants.HBASE_INDEXER_QUORUM);
            if (INDEX_CONFIG_HOME == null || HBASE_INDEXER_QUORUM == null) {
                LOG.warn("no input parameter for indexer:" + INDEX_CONFIG_HOME + "," + HBASE_INDEXER_QUORUM);
            } else {
                IndexConfigurator.Init(ZOOKEEPER_QUORUM, Integer.valueOf(ZOOKEEPER_TIMEOUT),
                        INDEX_CONFIG_HOME, HBASE_INDEXER_QUORUM);
            }

            //redis


        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw e;
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void finalize() {
        ConnectionUtil.getInstance().stop();
        IndexConfigurator.Release();
        OtsIndex.Release();
    }

    public OtsTable createTable(long userid, long tenantid, String tablename, Integer keyType, Integer hashkeyType,
                                Integer rangekeyType, String description, String compressType, Boolean bReplication) throws Exception {
        return createTable(userid, tenantid, tablename, keyType, hashkeyType, rangekeyType, description,
                compressType, 1, false, 0, bReplication);
    }

    public OtsTable createTable(long userid, long tenantid, String tablename, Integer keytype, Integer hashkeyType,
                                Integer rangekeyType, String description, String compressType,
                                Integer maxVersion, Boolean mobEnable, Integer mobTheshold, Boolean bReplication) throws OtsException, ConfigException {

        Configurator configurator = new Configurator();
        boolean hbaseFailed2DelPost = false;

        try {
            if (null != configurator.queryTable(tenantid, tablename)) {
                throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_EXIST, String.format("tenant (id:%d) already owned table:%s!", tenantid, tablename));
            }

            Table table = new Table();
            table.setUid(userid);
            table.setTid(tenantid);
            table.setName(tablename);
            table.setCompression(compressType != null ? compressType : Algorithm.NONE.getName());
            table.setDesp(description);
            table.setEnable(1);
            table.setMobEnabled((mobEnable != null && mobEnable.booleanValue()) ? 1 : 0);
            table.setMobThreshold(mobTheshold != null ? mobTheshold : 100);//!!kb
            table.setMaxversion(maxVersion != null ? maxVersion : 1);
            table.setKeytype(keytype);
            table.setHashkeyType(hashkeyType);
            if (null != rangekeyType) {
                table.setRangekeyType(rangekeyType);
            } else {
                table.setRangekeyType(-1);
            }
            table.setCreateTime(new Date());
            table.setModifyTime(table.getCreateTime());
            table.setModifyUid(userid);
            long id = configurator.addTable(table);
            table.setId(id);

            //after rdb success, add hbase info; if hbase operate error, clean rdb info
            try {
                //todo 加代码，验证是否存在大表
                if(!isHbaseTableExist(tenantid)){
                    createHbaseTable(tenantid,  compressType, maxVersion, mobEnable, mobTheshold, bReplication);
                }

            } catch (OtsException ex) {
                hbaseFailed2DelPost = true;
                throw ex;
            } catch (Exception e) {
                e.printStackTrace();
            }

            return new OtsTable(table, tenantid, this.conf);

        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } catch (OtsException e) {
            throw e;
        } finally {
            try {
                if (hbaseFailed2DelPost) {
                    configurator.delTable(tenantid, tablename);
                }
            } catch (ConfigException e) {
                e.printStackTrace();
            }

            configurator.release();
        }
    }

    //待删除
    public void createHbaseTable(long tenantid,  String tablename, String compressType, Integer maxVersion,
                                 Boolean mobEnable, Integer mobTheshold, Boolean bReplication) throws OtsException {
        Admin admin = null;

        try {
            admin = ConnectionUtil.getInstance().getAdmin();
            TableProvider.createTable(admin, String.valueOf(tenantid), compressType, maxVersion, mobEnable, mobTheshold, bReplication);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER,
                    "Failed to create table because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,
                    "Failed to create table because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (TableException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_CREATE,
                    "Failed to create table!\n" + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_CREATE,
                    "Failed to create table!\n" + e.getMessage());
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void createHbaseTable(long tenantid,  String compressType, Integer maxVersion,
                                 Boolean mobEnable, Integer mobTheshold, Boolean bReplication) throws OtsException {
        Admin admin = null;

        try {
            admin = ConnectionUtil.getInstance().getAdmin();
            TableProvider.createTable(admin, String.valueOf(tenantid), compressType, maxVersion, mobEnable, mobTheshold, bReplication);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER,
                    "Failed to create hbasetable because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,
                    "Failed to create hbasetable because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (TableException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_CREATE,
                    "Failed to create hbasetable!\n" + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_CREATE,
                    "Failed to create hbasetable!\n" + e.getMessage());
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String getTableFullname(long tenantid, String tablename) {
        return (String.valueOf(tenantid) + TableName.NAMESPACE_DELIM + tablename);
    }

    public boolean isTableEnabled(long tenantid, String tablename) throws TableException {
        Admin admin = null;
        try {
            String fulltablename = getTableFullname(tenantid, tablename);
            admin = ConnectionUtil.getInstance().getAdmin();
            return admin.isTableEnabled(TableName.valueOf(fulltablename));
        } catch (Exception e) {
            throw new TableException(OtsErrorCode.EC_OTS_STORAGE_INVALID_CHECK_TABLESTATUS, "error when checking table enable or not. " + e.getMessage());
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isTableExist(long userid, long tenantid, String tablename) throws Exception {
        Configurator configurator = new Configurator();

        try {
            if (null != configurator.queryTable(tenantid, tablename)) {
                return true;
            }
            return false;

        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    //todo 待删除
	public boolean isHbaseTableExist(long userid, long tenantid, String tablename) throws Exception {

		Admin admin = null;
		try {
			admin = ConnectionUtil.getInstance().getAdmin();
			return TableProvider.isTableExist(admin, String.valueOf(tenantid), tablename);
		} catch (MasterNotRunningException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER,
					"Failed to check table exist or not because hbase master no running!\n" + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,
					"Failed to check table exist or not because can not connecto to zookeeper!\n" + e.getMessage());
		} catch (IOException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST,
					"Failed to check table exist or not!\n" + e.getMessage());
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

    public boolean isHbaseTableExist(long tenantid) throws Exception {

        Admin admin = null;
        try {
            admin = ConnectionUtil.getInstance().getAdmin();
            return TableProvider.isTableExist(admin, String.valueOf(tenantid));
        } catch (MasterNotRunningException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER,
                    "Failed to check table exist or not because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,
                    "Failed to check table exist or not because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (IOException e) {
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST,
                    "Failed to check table exist or not!\n" + e.getMessage());
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteTable(long userid, long tenantid, String tablename) throws IOException, ConfigException, InterruptedException, IndexException, OtsException {
        LOG.debug("deleteTable begin: uid=" + userid + ", tenantid=" + tenantid + ", tablename=" + tablename);
        Configurator configurator = new Configurator();

        Admin admin = null;
        try {
            admin = ConnectionUtil.getInstance().getAdmin();

            List<Index> indexList = configurator.queryTableIndex(tenantid, tablename);
            String tableFullName = getTableFullname(tenantid, tablename);
            for (Index index : indexList) {
                String indexname = index.getName();
                LOG.info("Delete index " + tablename + "." + indexname);

                if ((int) index.getPattern() != OtsConstants.OTS_INDEX_PATTERN_SECONDARY_INDEX) {
                    IndexConfigurator indexConfigurator = new IndexConfigurator();
                    indexConfigurator.DeleteIndex(String.valueOf(tenantid), tablename, indexname);
                } else {
                    TableName indexTableName = TableName.valueOf(SecondaryIndexUtil.getIndexTableName(tableFullName, indexname));
                    TableProvider.deleteTable(admin, indexTableName);
                }
                configurator.delIndexProfile(index.getTid());
                configurator.delIndex(index.getTid(), indexname);
            }

            // delete table in hbase
            TableProvider.deleteTable(admin, String.valueOf(tenantid), tablename);

            // delete table in rdb
            Table tb = configurator.queryTable(tenantid, tablename);
            if (tb != null) {
                configurator.delTableProfile(tb.getId());
                configurator.delTable(tb.getTid(), tb.getName());
            }

            LOG.debug("deleteTable successful: uid=" + userid + ", tenantid=" + tenantid + ", tablename=" + tablename);

        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER,
                    "Failed to delete table because hbase master no running!\n" + e.getMessage());
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK,
                    "Failed to delete table because can not connecto to zookeeper!\n" + e.getMessage());
        } catch (TableException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_DELETE,
                    "Failed to delete table!\n" + e.getMessage());
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            configurator.release();
        }
    }

    public List<OtsTable> getPermisstionTables(long userid, long tenantid) throws ConfigException {

        List<OtsTable> lstTable = new ArrayList<OtsTable>();

        Configurator configurator = new Configurator();
        try {
            List<Table> lstTables = configurator.queryPermisstionTables(userid, tenantid);
            for (Table table : lstTables) {
                lstTable.add(new OtsTable(table, tenantid, this.conf));
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }

        return lstTable;
    }

    public List<OtsTable> getAllTables(long userid, long tenantid, List<Long> noGetPermissionList) throws ConfigException {

        List<OtsTable> lstTable = new ArrayList<OtsTable>();

        Configurator configurator = new Configurator();
        try {
            List<Table> lstTables = configurator.queryAllTable(userid, tenantid, noGetPermissionList);
            for (Table table : lstTables) {
                lstTable.add(new OtsTable(table, tenantid, this.conf));
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }

        return lstTable;
    }

    public List<OtsTable> getAllTables(long userid, long tenantid, String name, Long limit, Long offset, List<Long> noGetPermissionList) throws ConfigException {

        List<OtsTable> lstTable = new ArrayList<OtsTable>();

        Configurator configurator = new Configurator();
        try {
            List<Table> lstTables = configurator.queryAllTable(userid, tenantid, name, limit, offset, noGetPermissionList);
            for (Table table : lstTables) {
                lstTable.add(new OtsTable(table, tenantid, this.conf));
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }

        return lstTable;
    }

    public long countAllTables(long userid, long tenantid, String name, List<Long> noGetPermissionList) throws ConfigException {

        Configurator configurator = new Configurator();
        try {
            return configurator.countAllTable(userid, tenantid, name, noGetPermissionList);

        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    public List<OtsTable> getAllTablesByTid(long tenantid, List<Long> noGetPermissionList) throws ConfigException {

        List<OtsTable> lstTable = new ArrayList<OtsTable>();

        Configurator configurator = new Configurator();
        try {
            List<Table> lstTables = configurator.queryAllTableByTid(tenantid, noGetPermissionList);
            for (Table table : lstTables) {
                lstTable.add(new OtsTable(table, tenantid, this.conf));
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }

        return lstTable;
    }

    public List<Long> getAllTenants() throws Exception {
        Configurator configurator = new Configurator();
        try {
            return configurator.queryAllTenantId();
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    public OtsTable getTable(long userid, long tenantid, String tablename) throws ConfigException {
        Configurator configurator = new Configurator();

        try {
            Table table = configurator.queryTable(tenantid, tablename);
            if (table != null) {
                return new OtsTable(table, tenantid, this.conf);
            }
            return null;

        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    /**
     * !!important note:
     * no safe mode, table's info is null until getXXX() property function called.
     * main using for getRecords()
     */
    public OtsTable getTableNoSafe(long userid, long tenantid, String tablename) {
        return new OtsTable(userid, tenantid, tablename, this.conf);
    }

    public Map<String, TableMetrics> getTableMetrics(long tenantid) throws OtsException, TableException, ConfigException {
        Admin admin = null;
        try {
            admin = ConnectionUtil.getInstance().getAdmin();
            return MetricsProvider.getTableMetricsByNamespace(admin, String.valueOf(tenantid));
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to query metrics because hbase master no running!");
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to query metrics because can not connecto to zookeeper!");
        } catch (IOException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "Failed to query metrics!");
        } catch (TableException e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isNamespaceExist(long tenantid) throws OtsException {
        Admin admin = null;
        try {
            admin = ConnectionUtil.getInstance().getAdmin();
            return MetricsProvider.isTableNamespaceExist(admin, String.valueOf(tenantid));
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_NO_RUNNING_HBASE_MASTER, "Failed to check if namespace exist because hbase master no running!");
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_FAILED_CONN_ZK, "Failed to check if namespace exist because can not connecto to zookeeper!");
        } catch (IOException e) {
            e.printStackTrace();
            throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_INVALID_TENANT, "Failed to check if namespace exist!");
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getTableConfig(long userid, long tenantid, String tablename) throws ConfigException {
        Configurator configurator = new Configurator();
        String columns = null;

        try {
            TableProfile profile = configurator.queryTableProfile(tenantid, tablename);
            if (null != profile) {
                columns = profile.getDisplayCol();
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }

        return columns;
    }

    public void saveTableConfig(long userid, long tenantid, String tablename, String config) throws ConfigException {
        String display_columns = config;
        Configurator configurator = new Configurator();

        try {
            TableProfile profile = configurator.queryTableProfile(tenantid, tablename);
            if (null != profile) {
                profile.setDisplayCol(display_columns);
                configurator.updateTableProfile(profile);
            } else {
                Table table = configurator.queryTable(tenantid, tablename);
                if (null == table) {
                    throw new ConfigException(OtsErrorCode.EC_OTS_STORAGE_SAVE_SQLTABLE_CONFIG, "table=" + tablename + " config save failed.");
                }

                profile = new TableProfile();
                profile.setTid(table.getId());
                profile.setDisplayCol(display_columns);
                configurator.addTableProfile(profile);
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    public String getIndexConfig(long userid, long tenantid, String tablename, String indexname) throws ConfigException {
        Configurator configurator = new Configurator();
        String columns = null;

        try {
            IndexProfile profile = configurator.queryIndexProfile(tenantid, tablename, indexname);
            if (null != profile) {
                columns = profile.getDisplayCol();
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }

        return columns;
    }

    public void saveIndexConfig(long userid, long tenantid, String tablename, String indexname, String config) throws ConfigException {
        String display_columns = config;
        Configurator configurator = new Configurator();

        try {
            IndexProfile profile = configurator.queryIndexProfile(tenantid, tablename, indexname);
            if (null != profile) {
                profile.setDisplayCol(display_columns);
                configurator.updateIndexProfile(profile);
            } else {
                Index index = configurator.queryIndex(tenantid, tablename, indexname);
                if (null == index) {
                    throw new ConfigException(OtsErrorCode.EC_OTS_STORAGE_SAVE_SQLTABLE_CONFIG, "table=" + tablename + ", index=" + indexname + " config save failed.");
                }

                profile = new IndexProfile();
                profile.setIndexid(index.getId());
                profile.setDisplayCol(display_columns);
                configurator.addIndexProfile(profile);
            }
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    public boolean checkTablePermitted(long tableId) throws ConfigException, PermissionSqlException {
        Configurator configurator = new Configurator();
        try {
            return configurator.checkPermitted(tableId);
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } catch (PermissionSqlException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    public void setTablePermittion(long tableId) throws ConfigException, PermissionSqlException {
        Configurator configurator = new Configurator();
        try {
            configurator.setTablePermittion(tableId);
        } catch (ConfigException e) {
            e.printStackTrace();
            throw e;
        } catch (PermissionSqlException e) {
            e.printStackTrace();
            throw e;
        } finally {
            configurator.release();
        }
    }

    public OtsConfiguration getConf() {
        return conf;
    }

    //将小表行键包装成大表的行键
    public static byte[] getBigRowkey(long tableId, byte[] rRowKey) {

        byte[] tableIdKey = BytesUtil.toBytes(tableId);
//        byte[] tableIdKey = longToBytes(tableId);

        byte[] BigRowkey = new byte[tableIdKey.length + rRowKey.length];
        BytesUtil.putBytes(BigRowkey, 0, tableIdKey, 0, tableIdKey.length);
        BytesUtil.putBytes(BigRowkey, tableIdKey.length, rRowKey, 0, rRowKey.length);

        return BigRowkey;
    }

//    /**
//     * byte和long转化
//     */
//    private static ByteBuffer buffer = ByteBuffer.allocate(8);
//    //byte 数组与 long 的相互转换
//    public static byte[] longToBytes(long x) {
//        buffer.putLong(0, x);
//        return buffer.array();
//    }
//
//    public static long bytesToLong(byte[] bytes) {
//        buffer.put(bytes, 0, bytes.length);
//        buffer.flip();//need flip
//        return buffer.getLong();
//    }

//	public static void main(String[] args) throws Exception {
//		// hbase
////		Instantiating configuration class
//		Configuration hbase_cfg = HBaseConfiguration.create();
//		hbase_cfg.setStrings(HConstants.ZOOKEEPER_QUORUM, "168.2.8.77:2181,168.2.8.78:2181,168.2.8.79:2181");
//		hbase_cfg.setStrings(HConstants.ZK_SESSION_TIMEOUT, "3000");
//		ConnectionUtil.init(hbase_cfg);
//
////		实例化HBaseAdmin,这个类需要配置对象作为参数，因此初始实例配置类传递此实例给HBaseAdmin。
//		Admin admin = ConnectionUtil.getInstance().getAdmin();
//		TableName tableName = TableName.valueOf("1:wls_test");
//
////		creating table descriptor,构造一个表描述符指定TableName对象
//		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
//
////		creating column family descriptor
//		HColumnDescriptor coulmnDescriptor = new HColumnDescriptor("f");
//
////		将列家族给定的描述符,adding coloumn family to HTable
//		tableDescriptor.addFamily(coulmnDescriptor);
//
//		if (admin.tableExists(tableName)) {
//			long start = System.currentTimeMillis();
//			admin.disableTableAsync(tableName);
//			admin.deleteTable(tableName);
//			System.out.println("original delete:" + (System.currentTimeMillis() - start));
//
//			admin.createTable(tableDescriptor);
//
//			start = System.currentTimeMillis();
//			admin.disableTable(tableName);
//			//admin.deleteTable(tableName);
//			System.out.println("original disable:" + (System.currentTimeMillis() - start));
//
//			//admin.createTable(tableDescriptor);
//
//			start = System.currentTimeMillis();
//			admin.enableTable(tableName);
//			System.out.println("original enable:" + (System.currentTimeMillis() - start));
//
//			////////////////////
//			start = System.currentTimeMillis();
//			TableProvider.setTableEnableStatus(admin, tableName, false);
//			System.out.println("like 5.5.1 disable:" + (System.currentTimeMillis() - start));
//
//			start = System.currentTimeMillis();
//			TableProvider.setTableEnableStatus(admin, tableName, true);
//			System.out.println("like 5.5.1 enable:" + (System.currentTimeMillis() - start));
//
//			HTableDescriptor tableDescriptorNew = admin.getTableDescriptor(tableName);
//			HColumnDescriptor coulmnDescriptorNew = tableDescriptorNew.getFamily(Bytes.toBytes("f"));
//
//			coulmnDescriptorNew.setScope(1);
//			start = System.currentTimeMillis();
//			admin.modifyTable(tableName, tableDescriptorNew);
//			System.out.println("mm:" + (System.currentTimeMillis() - start));
//
//			coulmnDescriptorNew.setScope(0);
//			start = System.currentTimeMillis();
//			admin.modifyTable(tableName, tableDescriptorNew);
//			System.out.println("mm:" + (System.currentTimeMillis() - start));
//		}
//
//		admin.close();
//	}

    public static void main(String[] args) throws Exception {

        //init otsclient

        ConfigReader configReader = new ConfigReader("ots", ConfigReader.class);
        OtsConfiguration conf = OtsConfiguration.create();
        //如果默认ots的conf.properties存在,这样都可以不配置
        conf.setProperty(OtsConstants.ZOOKEEPER_QUORUM, configReader.getValue(ConfigConstants.ZOOKEEPER_QUORUM, "127.0.0.1:2181"));
        conf.setProperty(OtsConstants.ZOOKEEPER_TIMEOUT, configReader.getValue(ConfigConstants.ZOOKEEPER_TIMEOUT, "3000"));
//		conf.setProperty(OtsConstants.CLIENT_HBASE_RETRIES_NUMBER, ConfigUtil.getValue("hbase_client_retries_number", "1"));
        conf.setProperty(OtsConstants.CLIENT_HBASE_RETRIES_NUMBER, "2");
        conf.setProperty(OtsConstants.POSTGRES_QUORUM, "168.2.8.221");
        conf.setProperty(OtsConstants.POSTGRES_PORT, "5432");
        conf.setProperty(OtsConstants.POSTGRES_DBNAME, configReader.getValue(ConfigConstants.OTS_DBNAME, "ots"));
        conf.setProperty(OtsConstants.POSTGRES_USERNAME, configReader.getValue(ConfigConstants.POSTGRESQL_USER, "postgres"));
        conf.setProperty(OtsConstants.POSTGRES_PASSWORD, configReader.getValue(ConfigConstants.POSTGRESQL_PASSWORD, "q1w2e3"));
//        conf.setProperty(OtsConstants.INDEX_CONFIG_HOME, getValue("ots_indexer_cfghome");
        conf.setProperty(OtsConstants.HBASE_INDEXER_QUORUM, configReader.getValue(ConfigConstants.OTS_INDEX_HOST, "127.0.0.1"));
        conf.setProperty(ConfigConstants.YARN_RM_HOST, configReader.getValue(ConfigConstants.YARN_RM_HOST, "127.0.0.1:8088"));
        conf.setProperty(ConfigConstants.REDIS_QUORUM, configReader.getValue(ConfigConstants.REDIS_QUORUM, "127.0.0.1:6379"));

        OtsAdmin otsadmin = new OtsAdmin(conf);
//        if(!otsadmin.isTableExist(12345, 101, "test_wls"))
//        {
//        //createTable在创建数据库中的小表，在这个函数里就包含了先判断hbase大表是否存在
//            otsadmin.createTable(12345, 101, "test_wls", 2, 1, 2, "test_wls", "snappy", 1, true, 0, true);
//        }
//        else {
//        OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTableNoSafe(userInfo.getUserId(), userInfo.getTenantId(), tableName);
//        table.insertRecords(records);

            OtsTable otstable = otsadmin.getTable(12345, 101, "test_wls1");
        ////todo 验证postgres中小表存在与否,判断是否为null
		if(otstable != null)
		{
            //插入记录
            RowRecord rec1 = new RowRecord();
            rec1.setRowkey("10000".getBytes());
            RowCell cell1 = new RowCell("col1".getBytes(), "test1".getBytes());
            rec1.addCell(cell1);
            otstable.insertRecord(rec1);

            RowRecord rec2 = new RowRecord();
            rec2.setRowkey("20000".getBytes());
            RowCell cell2 = new RowCell("col2".getBytes(), "test2".getBytes());
            rec2.addCell(cell2);
            otstable.insertRecord(rec2);

            RowRecord rec3 = new RowRecord();
            rec3.setRowkey("30000".getBytes());
            RowCell cell3 = new RowCell("col3".getBytes(), "test3".getBytes());
            rec3.addCell(cell3);
            otstable.insertRecord(rec3);

            RowRecord rec4 = new RowRecord();
            rec4.setRowkey("40000".getBytes());
            RowCell cell4 = new RowCell("col4".getBytes(), "test4".getBytes());
            rec4.addCell(cell4);
            otstable.insertRecord(rec4);


            //更改记录
            cell4.setValue("test4_1".getBytes());
            otstable.updateRecord(rec4);

            //用RecordQueryOption查询，要设Cursor_mark和Limit
            RecordQueryOption recordQuery = new RecordQueryOption();
            recordQuery.setCursor_mark("*");
            recordQuery.setLimit(100L);
            RecordResult r1 = otstable.getRecords(recordQuery);

            List<RowRecord> ff = r1.getListRecords();
            for(int i = 0;i < ff.size();i++){
                System.out.println("1_lists:" + ff.get(i));
            }
//            //delete记录，全删
//            otstable.deleteRecords(recordQuery);
//            System.out.println("delete records");

            //用行键（range）和RecordQueryOption查询
            RecordQueryOption recordQuery_rangeRK = new RecordQueryOption();
            recordQuery_rangeRK.setLimit(100L);
            //startKey
//            Long tableId = otstable.getId();
//            byte[] rRowKey = rec.getRowkey();
//            byte[] tableIdKey = BytesUtil.toBytes(tableId);
//            byte[] startKey = new byte[tableIdKey.length + rRowKey.length];
//            BytesUtil.putBytes(startKey, 0, tableIdKey, 0, tableIdKey.length);
//            BytesUtil.putBytes(startKey, tableIdKey.length, rRowKey, 0, rRowKey.length);
            //endKey
//            byte[] rRowKey2 = rec2.getRowkey();
//            byte[] endKey = new byte[tableIdKey.length + rRowKey2.length];
//            BytesUtil.putBytes(endKey, 0, tableIdKey, 0, tableIdKey.length);
//            BytesUtil.putBytes(endKey, tableIdKey.length, rRowKey2, 0, rRowKey2.length);
            byte[] startKey = otsadmin.getBigRowkey(otstable.getId(),rec1.getRowkey());
            byte[] endKey = otsadmin.getBigRowkey(otstable.getId(),rec2.getRowkey());
            RecordResult r_rangeRK = otstable.getRecords(startKey, endKey, recordQuery_rangeRK);

            List<RowRecord> ff2 = r_rangeRK.getListRecords();
            for(int i = 0;i < ff2.size();i++){
                System.out.println("2_lists:" + ff2.get(i));
            }
            //delete记录（行键range）,需要设置filter
            // 这里filter用正则表达式来匹配rowkey
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*10000"));
            recordQuery_rangeRK.setFilter(filter);

            otstable.deleteRecords(startKey, endKey, recordQuery_rangeRK);
            System.out.println("delete records from startKey to endKey.");

            //用行键（list）和RecordQueryOption查询
            RecordQueryOption recordQuery_listRK = new RecordQueryOption();
            recordQuery_listRK.setLimit(100L);
            List<byte[]> keys = new ArrayList<byte[]>();
            keys.add(0, rec3.getRowkey());
            keys.add(1, rec4.getRowkey());
            List<byte[]> bigkeys = new ArrayList<byte[]>();
            if (keys.size() > 0) {
                int count = 0;
                for (byte[] row : keys) {
                    bigkeys.add(count, otsadmin.getBigRowkey(otstable.getId(), row));
                    count++;
                }
            }
            RecordResult r_listRK = otstable.getRecords(bigkeys,recordQuery_listRK);

            List<RowRecord> ff3 = r_listRK.getListRecords();
            for(int i = 0;i < ff3.size();i++){
                System.out.println("3_lists:" + ff3.get(i));
            }

//            //delete记录（listKeys）
//            otstable.deleteRecords(bigkeys);
//            System.out.println("delete records in the listkeys.");

            System.out.println("Records");
        }
		else {
            System.out.println("Put the record failed because the table doesn't exist.");

    }
        otsadmin.finalize();
     }
}