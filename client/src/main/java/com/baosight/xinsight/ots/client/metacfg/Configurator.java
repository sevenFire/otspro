package com.baosight.xinsight.ots.client.metacfg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.PermissionSqlException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class Configurator {
	private static final Logger LOG = Logger.getLogger(Configurator.class);

	private Connection conn;	// connection to configure database
	private static String []urls;
	private static String user;
	private static String passwd;
	public static final int TIMEOUT = 3;//second

	public static void init(String quorum, int port, String dbname, String strUser, String strPasswd) throws ConfigException {
		user = strUser;
		passwd = strPasswd;
		String[] hosts = quorum.split(",");
		if(hosts.length <= 0) {
			throw new ConfigException(OtsErrorCode.EC_RDS_ZERO_QUROM, "Invalid postgresql quorum format, too less option!");
		}
		urls = new String[hosts.length];
		for (int i = 0; i < hosts.length; i++) {
			urls[i] = String.format("jdbc:postgresql://%s:%d/%s", hosts[i], port, dbname);
		}
		
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_LOAD_JDBC_DRIVER, e.getMessage());
		}
	}	
		
	public void release() throws ConfigException {
		try {
			if(null != conn)
				conn.close();
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_CLOSE_CONN, e.getMessage());
		}
	}

	/**
	 * @throws ConfigException 
	 */
	public void connect() throws ConfigException {
		for (int i = 0; i < urls.length; i++) {
			try {
				if(null == conn || conn.isClosed()) {
					conn = DriverManager.getConnection(urls[i], user, passwd);
				}
				
				if(!conn.isValid(TIMEOUT)) {
					conn.close();
					conn = DriverManager.getConnection(urls[i], user, passwd);
				} else {
					break;
				}
			} catch (SQLException e) {
				if (i < urls.length - 1) {
					continue;
				} else {
					throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_CONN, "Failed to connect to database!");
				}
			}
		}	
	}
	
	/**
	 * @throws ConfigException 
	 */
	public void disConnect() throws ConfigException {
		try {
			conn.close();
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_CLOSE_CONN, e.getMessage());
		}
	}
	
	public long addTable(Table table) throws ConfigException	{
		long id = 0;
		
		try { 
			connect();
			conn.setAutoCommit(false);
			String sql = String.format("insert into ots_user_table (\"uid\", \"name\", \"compression\", \"mob_enabled\", \"mob_threshold\", \"desc\", \"enable\", \"maxversion\", \"tid\", \"keytype\", \"hashkey_type\", \"rangekey_type\", \"create_time\", \"modify_time\", \"modify_uid\") values ('%d', '%s', '%s', '%d', '%d', '%s', '%d', '%d', '%d', ? , ? , ? , ? , ? , ?) returning id;",
					table.getUid(), table.getName(), table.getCompression(), table.getMobEnabled(), table.getMobThreshold(),  table.getDesp(), table.getEnable(), table.getMaxversion(), table.getTid());
			
			PreparedStatement pstmt = conn.prepareStatement(sql);			
			pstmt.setInt(1, table.getKeytype());
			pstmt.setInt(2, table.getHashkeyType());
			pstmt.setInt(3, table.getRangekeyType());
			pstmt.setTimestamp(4, new Timestamp(table.getCreateTime().getTime()));
			pstmt.setTimestamp(5, new Timestamp(table.getModifyTime().getTime()));
			pstmt.setLong(6, table.getModifyUid());				
			//System.out.println(pstmt.toString());
			LOG.debug(pstmt.toString());
			
			pstmt.execute();
			ResultSet rs = pstmt.getResultSet();			
			if(rs.next()){
				id = rs.getLong("id");
			}
			conn.commit();
			conn.setAutoCommit(true);
			pstmt.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ROLLBACK, "Failed to add new table and failed to rollback db!" + e.getMessage());
			}
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_CREATE_TABLE, "Failed to add new table " + table.getName() +"!\n" + e.getMessage());
		}

		return id;
	}
	
	public void delTable(long tenantid, String name) throws ConfigException {
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("delete from ots_user_table where ots_user_table.name = '%s' and ots_user_table.tid = '%d';", name, tenantid);
			//System.out.println(sql);
			LOG.debug(sql);
			
			st.execute(sql);
			st.close();	
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_DEL_TABLE, "Failed to delete table " + name + "!\n" + e.getMessage());
		}

	}

	public void updateTable(Table table) throws ConfigException	{	
		
		try { 
			connect();
			
			conn.setAutoCommit(false);
			String sql = String.format("update ots_user_table set \"compression\" = '%s', \"mob_enabled\" = '%d', \"mob_threshold\" = '%d', \"desc\" = '%s', \"enable\" = '%d', \"maxversion\" = '%d', \"tid\" = '%d', \"modify_time\" = ?, \"modify_uid\" = '%d' where (\"name\" = '%s' and \"uid\" = '%d');",
					table.getCompression(), table.getMobEnabled(), table.getMobThreshold(), table.getDesp(), table.getEnable(), table.getMaxversion(), table.getTid(), table.getModifyUid(), table.getName(), table.getUid());

			PreparedStatement pstmt = conn.prepareStatement(sql);			
			pstmt.setTimestamp(1, new Timestamp(table.getModifyTime().getTime()));
			//System.out.println(pstmt.toString());
			LOG.debug(pstmt.toString());
						
			pstmt.execute();
			
			conn.commit();
			conn.setAutoCommit(true);
			pstmt.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ROLLBACK, "Failed to update table " + table.getName() + " and rollback!\n" + e.getMessage() + e1.getMessage());
			}
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_UPDATE_TABLE, "Failed to update table " + table.getName() + "!\n" + e.getMessage());
		}

		return ;	
	}
	
	public Table queryTable(long tenantid, String tableName) throws ConfigException	{
		Table table = null;
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("select * from ots_user_table where ots_user_table.name = '%s' and ots_user_table.tid = '%d';", tableName, tenantid);
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()) {
				table = new Table();
				table.setUid(rs.getLong("uid"));
				table.setName(tableName);
				table.setId(rs.getLong("id"));
				table.setDesp(rs.getString("desc"));
				table.setCompression(rs.getString("compression"));
				table.setEnable(rs.getShort("enable"));
				table.setMaxversion(rs.getInt("maxversion"));
				table.setTid(tenantid);
				table.setMobEnabled(rs.getShort("mob_enabled"));
				table.setMobThreshold(rs.getLong("mob_threshold"));
				
				table.setKeytype(rs.getInt("keytype"));
				table.setHashkeyType(rs.getInt("hashkey_type"));
				table.setRangekeyType(rs.getInt("rangekey_type"));
				table.setCreateTime(rs.getTimestamp("create_time"));
				table.setModifyTime(rs.getTimestamp("modify_time"));
				table.setModifyUid(rs.getLong("modify_uid"));
			}
			st.close();	
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query table " + tableName + "!\n" + e.getMessage());
		}
		
		return table;
	}
	
	public List<Table> queryAllTable(long userid, long tenantid, List<Long> noGetPermissionList) throws ConfigException {
		List<Table> lstTables = new ArrayList<Table>();
		
		try {
			connect();			
			Statement st = conn.createStatement();			
			String sql = "";
			if (noGetPermissionList != null && !noGetPermissionList.isEmpty()) {
				String list2String = StringUtils.join(noGetPermissionList.toArray(), ",");
				StringBuilder noGetPermissionObj = new StringBuilder().append("(").append(list2String).append(")");
				sql = String.format("select * from ots_user_table where ots_user_table.tid = '%d' and ots_user_table.id not in %s order by ots_user_table.id;", tenantid, noGetPermissionObj);
			} else {
				sql = String.format("select * from ots_user_table where ots_user_table.tid = '%d' order by ots_user_table.id;", tenantid);
			}

			//System.out.println(sql);
			LOG.debug(sql);			
			ResultSet rs = st.executeQuery(sql);
			while (rs.next())	{
				Table table = new Table();
				table.setUid(rs.getLong("uid"));
				table.setName(rs.getString("name"));
				table.setId(rs.getLong("id"));
				table.setDesp(rs.getString("desc"));
				table.setCompression(rs.getString("compression"));
				table.setEnable(rs.getShort("enable"));
				table.setMaxversion(rs.getInt("maxversion"));
				table.setTid(rs.getLong("tid"));
				table.setMobEnabled(rs.getShort("mob_enabled"));
				table.setMobThreshold(rs.getLong("mob_threshold"));
				
				table.setKeytype(rs.getInt("keytype"));
				table.setHashkeyType(rs.getInt("hashkey_type"));
				table.setRangekeyType(rs.getInt("rangekey_type"));
				table.setCreateTime(rs.getTimestamp("create_time"));
				table.setModifyTime(rs.getTimestamp("modify_time"));
				table.setModifyUid(rs.getLong("modify_uid"));
				
				lstTables.add(table);
			}
			st.close();				
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query all table!\n" + e.getMessage());
		}

		return lstTables;
	}
	
	public List<Table> queryAllTable(long userid, long tenantid, String name, Long limit, Long offset, List<Long> noGetPermissionList) throws ConfigException {
		List<Table> lstTables = new ArrayList<Table>();
		
		try {
			connect();			
			Statement st = conn.createStatement();
			String sql ="";
			if (noGetPermissionList != null && !noGetPermissionList.isEmpty()) {
				String list2String = StringUtils.join(noGetPermissionList.toArray(), ",");
				StringBuilder noGetPermissionObj = new StringBuilder().append("(").append(list2String).append(")");
				if (limit != null) {
					sql = String.format("select * from ots_user_table where ots_user_table.name ~* '%s' and ots_user_table.tid = '%d' and ots_user_table.id not in %s order by ots_user_table.id limit '%d' offset '%d';", name, tenantid, noGetPermissionObj, limit, offset);
				} else {
					sql = String.format("select * from ots_user_table where ots_user_table.name ~* '%s'  and ots_user_table.tid = '%d' and ots_user_table.id not in %s order by ots_user_table.id;", name, tenantid, noGetPermissionObj);
				}
			}else{
				if (limit != null) {
					sql = String.format("select * from ots_user_table where ots_user_table.name ~* '%s' and ots_user_table.tid = '%d' order by ots_user_table.id limit '%d' offset '%d';", name, tenantid, limit, offset);
				} else {
					sql = String.format("select * from ots_user_table where ots_user_table.name ~* '%s'  and ots_user_table.tid = '%d' order by ots_user_table.id;", name, tenantid);
				}
			}					

			//System.out.println(sql);
			LOG.debug(sql);
			
			ResultSet rs = st.executeQuery(sql);
			while (rs.next())	{
				Table table = new Table();
				table.setUid(rs.getLong("uid"));
				table.setName(rs.getString("name"));
				table.setId(rs.getLong("id"));
				table.setDesp(rs.getString("desc"));
				table.setCompression(rs.getString("compression"));
				table.setEnable(rs.getShort("enable"));
				table.setMaxversion(rs.getInt("maxversion"));
				table.setTid(rs.getLong("tid"));
				table.setMobEnabled(rs.getShort("mob_enabled"));
				table.setMobThreshold(rs.getLong("mob_threshold"));
				
				table.setKeytype(rs.getInt("keytype"));
				table.setHashkeyType(rs.getInt("hashkey_type"));
				table.setRangekeyType(rs.getInt("rangekey_type"));
				table.setCreateTime(rs.getTimestamp("create_time"));
				table.setModifyTime(rs.getTimestamp("modify_time"));
				table.setModifyUid(rs.getLong("modify_uid"));
				
				lstTables.add(table);
			}
			st.close();				
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query all table!\n" + e.getMessage());
		}

		return lstTables;
	}
	
	public long countAllTable(long userid, long tenantid, String name, List<Long> noGetPermissionList) throws ConfigException {
	
		try {
			connect();			
			Statement st = conn.createStatement();
			String sql = "";
			if (noGetPermissionList != null && !noGetPermissionList.isEmpty()) {
				String list2String = StringUtils.join(noGetPermissionList.toArray(), ",");
				StringBuilder noGetPermissionObj = new StringBuilder().append("(").append(list2String).append(")");
				sql = String.format("select count(*) from ots_user_table where ots_user_table.name ~* '%s' and ots_user_table.tid = '%d'and ots_user_table.tid not in %s ;", name, tenantid, noGetPermissionObj);
			}else{
				sql = String.format("select count(*) from ots_user_table where ots_user_table.name ~* '%s' and ots_user_table.tid = '%d';", name, tenantid);
			}
			//System.out.println(sql);
			LOG.debug(sql);			
			ResultSet rs = st.executeQuery(sql);
			while (rs.next())	{
				return rs.getLong("count");
			}
			st.close();				
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query all table!\n" + e.getMessage());
		}
		
		return 0;
	}
	
	public List<Table> queryAllTableByTid(long tenantid, List<Long> noGetPermissionList) throws ConfigException {
		List<Table> lstTables = new ArrayList<Table>();		
		try {
			connect();			
			Statement st = conn.createStatement();
			String sql = "";
			if(noGetPermissionList != null && !noGetPermissionList.isEmpty()){
				String list2String = StringUtils.join(noGetPermissionList.toArray(), ",");
				StringBuilder noGetPermissionObj = new StringBuilder().append("(").append(list2String).append(")");
				sql = String.format("select * from ots_user_table where ots_user_table.tid = '%d' order by ots_user_table.id and ots_user_table.tid not in '%s';", tenantid, noGetPermissionObj );
			}else{
				sql = String.format("select * from ots_user_table where ots_user_table.tid = '%d' order by ots_user_table.id;", tenantid);
			}
			//System.out.println(sql);
			LOG.debug(sql);
			
			ResultSet rs = st.executeQuery(sql);
			while (rs.next())	{
				Table table = new Table();
				table.setUid(rs.getLong("uid"));
				table.setName(rs.getString("name"));
				table.setId(rs.getLong("id"));
				table.setDesp(rs.getString("desc"));
				table.setCompression(rs.getString("compression"));
				table.setEnable(rs.getShort("enable"));
				table.setMaxversion(rs.getInt("maxversion"));
				table.setTid(tenantid);
				table.setMobEnabled(rs.getShort("mob_enabled"));
				table.setMobThreshold(rs.getLong("mob_threshold"));
				
				table.setKeytype(rs.getInt("keytype"));
				table.setHashkeyType(rs.getInt("hashkey_type"));
				table.setRangekeyType(rs.getInt("rangekey_type"));
				table.setCreateTime(rs.getTimestamp("create_time"));
				table.setModifyTime(rs.getTimestamp("modify_time"));
				table.setModifyUid(rs.getLong("modify_uid"));
				
				lstTables.add(table);
			}
			st.close();				
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query all table!\n" + e.getMessage());
		}

		return lstTables;
	}
	
	public List<Long> queryAllTenantId() throws ConfigException {
		List<Long> lstTids = new ArrayList<Long>();
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("select ots_user_table.tid from ots_user_table group by ots_user_table.tid;");
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);
			while (rs.next()) {			
				lstTids.add(rs.getLong("tid"));
			}
			st.close();	
			
		} catch (Exception e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_ALL_TID, "Failed to query all tids!\n" + e.getMessage());
		}
		
		return lstTids;
	}
	
	public long addIndex(Index index) throws ConfigException {
		long id = 0;
		
		try { 
			connect();
			conn.setAutoCommit(false);

			String sql = String.format("insert into ots_table_index (\"tid\", \"name\", \"shard\", \"replication\", \"start_key\", \"end_key\", \"pattern\", \"index_columns\", \"create_time\", \"last_modify\") values ('%d', '%s', '%d', '%d', ?, ?, ?, ?, ?, ?) returning id;",
					index.getTid(),index.getName(), index.getShardNum(), index.getReplicationNum());

			PreparedStatement pstmt = conn.prepareStatement(sql);			
			pstmt.setString(1, index.getStartKey());
			pstmt.setString(2, index.getEndKey());
			pstmt.setInt(3, index.getPattern());
			pstmt.setString(4, index.getIndexColumns());
			pstmt.setTimestamp(5, new Timestamp(index.getCreateTime().getTime()));
			pstmt.setTimestamp(6, new Timestamp(index.getLastModify().getTime()));						
			//System.out.println(pstmt.toString());
			LOG.debug(pstmt.toString());
			
			pstmt.execute();
			ResultSet rs = pstmt.getResultSet();
			if(rs.next()){
				id = rs.getLong("id");
			}
			conn.commit();
			conn.setAutoCommit(true);
			pstmt.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ADD_INDEX, "Failed to add index " + index.getName() + " and rollback!\n" + e.getMessage() + e1.getMessage());
			}
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ADD_INDEX, "Failed to add index " + index.getName() + "!\n" + e.getMessage());
		}

		return id;
	}
	
	public void delIndex(long tableid, String name) throws ConfigException	{
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("delete from ots_table_index where ots_table_index.name = '%s' and ots_table_index.tid = '%d';", name, tableid);
			//System.out.println(sql);
			LOG.debug(sql);
			
			st.execute(sql);
			st.close();		
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_DEL_INDEX, "Failed to delete index " + name + "!\n" + e.getMessage());
		}	
	}
	
	public void delIndex(long idxid) throws ConfigException	{
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("delete from ots_table_index where ots_table_index.id = '%d';", idxid);
			//System.out.println(sql);
			LOG.debug(sql);
			
			st.execute(sql);
			st.close();		
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_DEL_INDEX, "Failed to delete index id=" + idxid + "!\n" + e.getMessage());
		}	
	}

	public void updateIndex(Index index) throws ConfigException	{	
		
		try { 
			connect();
			conn.setAutoCommit(false);
			
			String sql = String.format("update ots_table_index set \"shard\" = %d, \"replication\" = %d, \"start_key\" = ?, \"end_key\" = ?, \"pattern\" = ?, \"index_columns\" = ?, \"last_modify\" = ? where (\"name\" = '%s' and \"tid\" = '%d');",
					index.getShardNum(), index.getReplicationNum(), index.getName(), index.getTid());			
			
			PreparedStatement pstmt = conn.prepareStatement(sql);			
			pstmt.setString(1, index.getStartKey());
			pstmt.setString(2, index.getEndKey());
			pstmt.setInt(3, index.getPattern());
			pstmt.setString(4, index.getIndexColumns());
			pstmt.setTimestamp(5, new Timestamp(index.getLastModify().getTime()));
			//System.out.println(pstmt.toString());
			LOG.debug(pstmt.toString());

			pstmt.execute();
			conn.commit();
			conn.setAutoCommit(true);
			pstmt.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ROLLBACK, "Failed to update index " + index.getName() + " and rollback!\n" + e.getMessage());
			}
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_UPDATE_INDEX, "Failed to update index " + index.getName() + "!\n" + e.getMessage());
		}

		return;	
	}


	public void updateIndexTime(Index index) throws ConfigException	{	
		
		try { 
			connect();
			conn.setAutoCommit(false);
			
			String sql = String.format("update ots_table_index set \"last_modify\" = ? where (\"name\" = '%s' and \"tid\" = '%d');", index.getName(), index.getTid());			
			
			PreparedStatement pstmt = conn.prepareStatement(sql);			
			pstmt.setTimestamp(1, new Timestamp(index.getLastModify().getTime()));
			//System.out.println(pstmt.toString());
			LOG.debug(pstmt.toString());

			pstmt.execute();
			conn.commit();
			conn.setAutoCommit(true);
			pstmt.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ROLLBACK, "Failed to update index " + index.getName() + " and rollback!\n" + e.getMessage());
			}
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_UPDATE_INDEX, "Failed to update index " + index.getName() + "!\n" + e.getMessage());
		}

		return;	
	}
		
	public List<Index> queryTableIndex(long tenantid, String tableName) throws ConfigException {
		List<Index> indexList = new ArrayList<Index>();		
		
		try {
			connect();			
			Statement st = conn.createStatement();
			
			String sql = String.format("select ots_table_index.id, ots_table_index.tid, ots_table_index.name, ots_table_index.shard, ots_table_index.replication, ots_table_index.start_key, ots_table_index.end_key, ots_table_index.pattern, ots_table_index.index_columns, ots_table_index.create_time, ots_table_index.last_modify from ots_table_index , ots_user_table where (ots_user_table.id = ots_table_index.tid and ots_user_table.name = '%s' and ots_user_table.tid='%d') order by ots_table_index.id;",
					tableName, tenantid);
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);			
			while(rs.next()) {
				Index index = new Index();
				index.setId(rs.getLong("id"));
				index.setName(rs.getString("name"));
				index.setShardNum(rs.getInt("shard"));
				index.setReplicationNum(rs.getInt("replication"));
				index.setPattern((char)rs.getByte("pattern"));
				index.setStartKey(rs.getString("start_key"));				
				index.setEndKey(rs.getString("end_key"));
								
				index.setTid(rs.getLong("tid"));
				index.setIndexColumns(rs.getString("index_columns"));
				index.setCreateTime(rs.getTimestamp("create_time"));
				index.setLastModify(rs.getTimestamp("last_modify"));
				
				indexList.add(index);
			}
			st.close();			
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_INDEX, "Failed to query table index, tablename=" + tableName +"!\n" + e.getMessage());
		}
	
		return indexList;
	}
	
	public Index queryIndex(long tenantid, String tableName, String indexName) throws ConfigException {
		Index index = null;
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("select ots_table_index.id, ots_table_index.tid, ots_table_index.name, ots_table_index.shard, ots_table_index.replication, ots_table_index.start_key, ots_table_index.end_key, ots_table_index.pattern, ots_table_index.index_columns, ots_table_index.create_time, ots_table_index.last_modify from ots_table_index, ots_user_table where ots_user_table.id = ots_table_index.tid and ots_user_table.name = '%s' and ots_user_table.tid='%d' and ots_table_index.name = '%s';",
					tableName, tenantid, indexName);
			//System.out.println(sql);
			LOG.debug(sql);
			
			ResultSet rs = st.executeQuery(sql);			
			if(rs.next()) {
				index = new Index();
				index.setId(rs.getLong("id"));
				index.setName(rs.getString("name"));
				index.setShardNum(rs.getInt("shard"));
				index.setReplicationNum(rs.getInt("replication"));
				index.setPattern((char)rs.getByte("pattern"));
				index.setStartKey(rs.getString("start_key"));
				index.setEndKey(rs.getString("end_key"));
				
				index.setTid(rs.getLong("tid"));
				index.setIndexColumns(rs.getString("index_columns"));
				index.setCreateTime(rs.getTimestamp("create_time"));
				index.setLastModify(rs.getTimestamp("last_modify"));
			}
			st.close();	
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_INDEX, "Failed to query index, tablename=" 
			+ tableName + " indexName=" + indexName +"!\n" + e.getMessage());
		}		
		
		return index;
	}
	
	public long addTableProfile(TableProfile profile) throws ConfigException	{
		long id = 0;
		
		try { 
			connect();
			conn.setAutoCommit(false);
			Statement st = conn.createStatement();
			String sql = String.format("insert into ots_table_profile (\"tid\", \"display_columns\") values ('%d', '%s') returning id;",
					profile.getTid(), profile.getDisplayCol());
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				id = rs.getLong("id");
			}
			conn.commit();
			conn.setAutoCommit(true);
			st.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ADD_TABLE_PROFILE, "Failed to add table profile and rollback, tid=" 
							+ profile.getTid() + "!\n" + e.getMessage());
			}
			
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ADD_TABLE_PROFILE, "Failed to add table profile, tid=" 
						+ profile.getTid() + "!\n" + e.getMessage());
		}

		return id;	
	}
	
	public void delTableProfile(long tableid) throws ConfigException {
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("delete from ots_table_profile where ots_table_profile.tid = '%d';", tableid);
			//System.out.println(sql);
			LOG.debug(sql);
						
			st.execute(sql);
			st.close();			
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_DEL_TABLE_PROFILE, "Failed to delete table profile, tid=" + tableid + "!\n" + e.getMessage());
		}
	}
	
	public TableProfile queryTableProfile(long tenantid, String tableName) throws ConfigException {
		TableProfile profile = null;

		try {
			connect();
			Statement st = conn.createStatement();			
			String sql = String.format("select ots_table_profile.id, ots_table_profile.tid, ots_table_profile.display_columns from ots_user_table, ots_table_profile where (ots_user_table.tid='%d' and ots_user_table.name = '%s' and ots_user_table.id = ots_table_profile.tid);",
					tenantid, tableName);			
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);
			if(rs.next())
			{
				profile = new TableProfile();
				profile.setDisplayCol(rs.getString("display_columns"));
				profile.setTid(rs.getLong("tid"));
				profile.setId(rs.getLong("id"));
			}
			
			st.close();			
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE_PROFILE, "Failed to query table profile, tablename=" + tableName + "!\n" + e.getMessage());
		}

		return profile;
	}
	
	public void updateTableProfile(TableProfile profile) throws ConfigException	{	
		try { 
			connect();
			conn.setAutoCommit(false);
			Statement st = conn.createStatement();
			
			String sql = String.format("update ots_table_profile set \"display_columns\"='%s' where (\"tid\" = '%d');", profile.getDisplayCol(), profile.getTid());
			//System.out.println(sql);
			LOG.debug(sql);
						
			st.execute(sql);
			conn.commit();
			conn.setAutoCommit(true);
			st.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_UPDATE_TABLE_PROFILE, "Failed to update table profile!\n" + e.getMessage());
		}	
	}
	
	
	public long addIndexProfile(IndexProfile profile) throws ConfigException	{
		long id = 0;
		
		try { 
			connect();
			conn.setAutoCommit(false);
			Statement st = conn.createStatement();
			String sql = String.format("insert into ots_index_profile (\"iid\", \"display_columns\") values ('%d', '%s') returning id;",
					profile.getIndexid(), profile.getDisplayCol());
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				id = rs.getLong("id");
			}
			conn.commit();
			conn.setAutoCommit(true);
			st.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ADD_INDEX_PROFILE, "Failed to add index profile and rollback, iid=" 
							+ profile.getIndexid() + "!\n" + e.getMessage());				
			}
			
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_ADD_INDEX_PROFILE, "Failed to add index profile, iid=" 
						+ profile.getIndexid() + "!\n" + e.getMessage());			
		}

		return id;	
	}
	
	public void delIndexProfile(long indexid) throws ConfigException {
		
		try {
			connect();
			
			Statement st = conn.createStatement();
			String sql = String.format("delete from ots_index_profile where ots_index_profile.iid = '%d';", indexid);
			//System.out.println(sql);
			LOG.debug(sql);
						
			st.execute(sql);
			st.close();			
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_DEL_INDEX_PROFILE, "Failed to delete index profile, iid=" + indexid + "!\n" + e.getMessage());
		}
	}
	
	public IndexProfile queryIndexProfile(long tenantid, String tableName, String indexName) throws ConfigException {
		IndexProfile profile = null;

		try {
			connect();
			Statement st = conn.createStatement();	
			assert(indexName != null);

			String sql = String.format("select ots_index_profile.id, ots_index_profile.iid, ots_index_profile.display_columns from ots_user_table, ots_index_profile, ots_table_index where (ots_user_table.tid='%d' and ots_user_table.name = '%s' and ots_table_index.name = '%s' and ots_table_index.tid = ots_user_table.id and ots_table_index.id = ots_index_profile.iid);",
					tenantid, tableName, indexName);			
			
			//System.out.println(sql);
			LOG.debug(sql);
						
			ResultSet rs = st.executeQuery(sql);
			if(rs.next())
			{
				profile = new IndexProfile();
				profile.setDisplayCol(rs.getString("display_columns"));
				profile.setIndexid(rs.getLong("iid"));
				profile.setId(rs.getLong("id"));
			}
			
			st.close();			
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_INDEX_PROFILE, "Failed to query index profile, tablename=" + tableName + ", indexname=" + indexName + "!\n" + e.getMessage());
		}

		return profile;
	}
	
	public void updateIndexProfile(IndexProfile profile) throws ConfigException	{	
		try { 
			connect();
			conn.setAutoCommit(false);
			Statement st = conn.createStatement();
			
			String sql = String.format("update ots_index_profile set \"display_columns\"='%s' where (\"iid\" = '%d');", profile.getDisplayCol(), profile.getIndexid());
			//System.out.println(sql);
			LOG.debug(sql);
						
			st.execute(sql);
			conn.commit();
			conn.setAutoCommit(true);
			st.close();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_UPDATE_INDEX_PROFILE, "Failed to update index profile!\n" + e.getMessage());
		}	
	}
	
	public List<Table> queryPermisstionTables(long userid, long tenantid) throws ConfigException {	
		List<Table> lstTables = new ArrayList<Table>();		
		try {
			connect();			
			Statement st = conn.createStatement();			
			String sqlByPermittedList = String.format("select * from ots_user_table where permission=true and ots_user_table.tid = '%d'", tenantid);
			ResultSet rs = st.executeQuery(sqlByPermittedList);
			
			while (rs.next())	{
				Table table = new Table();
				table.setUid(rs.getLong("uid"));
				table.setName(rs.getString("name"));
				table.setId(rs.getLong("id"));
				table.setDesp(rs.getString("desc"));
				table.setCompression(rs.getString("compression"));
				table.setEnable(rs.getShort("enable"));
				table.setMaxversion(rs.getInt("maxversion"));
				table.setTid(rs.getLong("tid"));
				table.setMobEnabled(rs.getShort("mob_enabled"));
				table.setMobThreshold(rs.getLong("mob_threshold"));
				
				table.setKeytype(rs.getInt("keytype"));
				table.setHashkeyType(rs.getInt("hashkey_type"));
				table.setRangekeyType(rs.getInt("rangekey_type"));
				table.setCreateTime(rs.getTimestamp("create_time"));
				table.setModifyTime(rs.getTimestamp("modify_time"));
				table.setModifyUid(rs.getLong("modify_uid"));
				
				lstTables.add(table);
			}
			st.close();
		} catch (SQLException e) {
			throw new ConfigException(OtsErrorCode.EC_RDS_FAILED_QUERY_TABLE, "Failed to query all table!\n" + e.getMessage());
		}
		return lstTables;
    }
	
	public void setTablePermittion(long tableId) throws PermissionSqlException, ConfigException {
		try {
			connect();			
			Statement st = conn.createStatement();
			String sql = "update public.ots_user_table set permission = true where public.ots_user_table.id =" + tableId;
		    st.execute(sql);		
		    st.close();
		}  catch(ConfigException e){
			e.getStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
			throw e;
		}	catch (SQLException e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
			throw new PermissionSqlException(OtsErrorCode.EC_OTS_ADD_PERMISSION_SQL_LABEL, (new StringBuilder()).append("Failed to add the permitted field to the relevant table").toString());			
		}	
	}
	
	public boolean checkPermitted(long insId) throws PermissionSqlException, ConfigException {
		boolean permitted = false;
		try {			
			connect();			
			Statement st = conn.createStatement();
			String sql = "select permission from public.ots_user_table where public.ots_user_table.id =" + insId;
		    ResultSet rs = st.executeQuery(sql);		
		    if(rs.next()){
		    	permitted = rs.getBoolean("permission");	    	
		    }		  		    
		    st.close();
		} catch(ConfigException e){
			e.getStackTrace();
			LOG.error(ExceptionUtils.getFullStackTrace(e));
			throw e;
		} catch (SQLException e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
			throw new PermissionSqlException(OtsErrorCode.EC_OTS_QUERY_PERMISSION_SQL_LABEL, (new StringBuilder()).append("Failed to query the specified value of permission label fields ").toString());			
		}	
		return permitted;
	}
		
	////////////////////////
	
	public static void main(String[] args) throws ConfigException {  
		 Configurator configurator = new Configurator();
		 Configurator.init("168.2.6.154,168.2.6.155",5432, "ots", "postgres", "q1w2e3");
		 Index index = new Index();
		 index.setName("test-bys");
		 index.setStartKey(null);
		 index.setEndKey(null);
		 index.setIndexColumns("ab, bc, cd");
		 index.setPattern("1".charAt(0));
		 index.setTid(1);
		 
		 configurator.delIndex(1, "test-bys");
		 configurator.release();
	 }
}
