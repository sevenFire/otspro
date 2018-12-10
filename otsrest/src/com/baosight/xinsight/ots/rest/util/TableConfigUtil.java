package com.baosight.xinsight.ots.rest.util;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.OtsTable;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.model.TableInfoListModel;
import com.baosight.xinsight.ots.rest.model.TableInfoModel;
import com.baosight.xinsight.ots.rest.service.TableService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableConfigUtil {
    private static Map<String, Map<String, TableInfoModel> > tbConfig = new HashMap<String, Map<String, TableInfoModel> >();

	public static void cacheTableConfig(long userid, long tenantid, String tenant, String username) throws ConfigException, IOException {
		try {
			String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
			if (tbConfig.containsKey(key)) {
				return; 
			}
			PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
			userInfo.setUserId(userid);
			userInfo.setUserName(username);
			userInfo.setTenantId(tenantid);
			userInfo.setTenantName(tenant);
			userInfo.setServiceName(OtsConstants.OTS_SERVICE_NAME);
			TableInfoListModel infolist = TableService.getAllTablesInfo(userInfo);
			Map<String, TableInfoModel> tbConfigMap = new HashMap<String, TableInfoModel>();
			for (TableInfoModel t : infolist.getTableinfolist()) {
				tbConfigMap.put(t.getName(), t);
			}
			tbConfig.put(key, tbConfigMap);
		}
		catch (ConfigException e) {
			throw e;
		}		
	}
	
	public static void cacheTableConfig(long userid, long tenantid,	List<OtsTable> lstTables) throws IOException, ConfigException  {
		try {
			String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
				
			Map<String, TableInfoModel> tbConfigMap = new HashMap<String, TableInfoModel>();
			for (OtsTable table : lstTables) {
				
				TableInfoModel model = new TableInfoModel(table.getName());
				model.setId(table.getId());
				model.setKeyType(table.getKeyType());
				model.setHashKeyType(table.getHashKeyType());
				model.setRangeKeyType(table.getRangeKeyType());
				
				tbConfigMap.put(model.getName(),model);
			}
			tbConfig.put(key, tbConfigMap);
		}
		catch (ConfigException e) {
			throw e;
		}		
	}
	
	public static void addTableConfig(long userid, long tenantid, TableInfoModel info) throws ConfigException {
		String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
		if (tbConfig.containsKey(key)) {
			tbConfig.get(key).put(info.getName(), info);
			return; 
		}			
		
		Map<String, TableInfoModel> tbConfigMap = new HashMap<String, TableInfoModel>();
		tbConfigMap.put(info.getName(), info);			
		tbConfig.put(key, tbConfigMap);		
	}
	
	public static void deleteTableConfig(long userid, long tenantid, String tablename) throws ConfigException {		
		String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
		if (tbConfig.containsKey(key)) {
			if (tbConfig.get(key).containsKey(tablename)) {
				tbConfig.get(key).remove(tablename);
			}			
		}	
	}
	
	public static TableInfoModel getTableConfig(long userid, long tenantid, String tablename) throws ConfigException, IOException, Exception   {
		String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
		if (tbConfig.containsKey(key)) {			
			if (tbConfig.get(key).containsKey(tablename)) {
				return tbConfig.get(key).get(tablename);
			}
		}			
		
		try {
			OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userid, tenantid, tablename);
			if (table == null) {
				throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, String.format("user (id:%d) was not owned table:%s!", userid, tablename));
			}			
			
			TableInfoModel info = new TableInfoModel(tablename);
			info.setId(table.getId());
			info.setKeyType(table.getKeyType());
			info.setHashKeyType(table.getHashKeyType());
			info.setRangeKeyType(table.getRangeKeyType());
			addTableConfig(userid, tenantid, info);
			return info;
		} catch (ConfigException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}

	public static synchronized void syncAdd(long tenantid, long userid, String tablename) throws ConfigException, Exception {
		try {
			OtsTable table = ConfigUtil.getInstance().getOtsAdmin().getTable(userid, tenantid, tablename);
			if (table == null) {
				//throw new OtsException(OtsErrorCode.EC_OTS_STORAGE_TABLE_NOTEXIST, String.format("user (id:%d) was not owned table:%s!", userid, tablename));
				return;
			}			
			
			TableInfoModel info = new TableInfoModel(tablename);
			info.setId(table.getId());
			info.setKeyType(table.getKeyType());
			info.setHashKeyType(table.getHashKeyType());
			info.setRangeKeyType(table.getRangeKeyType());
			addTableConfig(userid, tenantid, info);		
		} catch (ConfigException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}

	public static synchronized void syncDel(long tenantid, long userid, String tablename) {		
		String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
		if (tbConfig.containsKey(key)) {
			if (tbConfig.get(key).containsKey(tablename)) {
				tbConfig.get(key).remove(tablename);
			}			
		}			
	}

	public static synchronized void syncUpdate(long tenantid, long userid, String tablename) throws ConfigException, Exception {
		String key = String.valueOf(tenantid) + CommonConstants.DEFAULT_DOMAIN_SPLIT + String.valueOf(userid);
		if (tbConfig.containsKey(key)) {
			if (tbConfig.get(key).containsKey(tablename)) {				
				tbConfig.get(key).remove(tablename);
				
				syncAdd(tenantid, userid, tablename);
			}			
		}		
	}
}
