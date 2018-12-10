package com.baosight.xinsight.ots.cfgsvr.common;


public class RestConstants extends com.baosight.xinsight.ots.rest.common.RestConstants {
	
	public static final int DEFAULT_CONNECT_TYPE_SFTP = 1;
	public static final int DEFAULT_CONNECT_TYPE_FTP = 0;
		
	public static final String DEFAULT_METRICS_PREFIX = "ots_metrics_";
	public static final String DEFAULT_METRICS_LOCK_PREFIX = "/ots_metrics_lock";
	public static final String DEFAULT_ROWCOUNT_LOCK_PREFIX = "/ots_rowcount_lock";
	
	public static final String DEFAULT_BACKUP_PREFIX = "ots_backup_";
	public static final String DEFAULT_BACKUP_TYPE = "ots_backup_type";
	public static final int DEFAULT_BACKUP_TYPE_BACKUP = 0;
	public static final int DEFAULT_BACKUP_TYPE_RESTORE = 1;
	public static final String DEFAULT_BACKUP_STATE = "ots_backup_state";
	public static final String DEFAULT_BACKUP_STATE_WAITING = "0";
	public static final String DEFAULT_BACKUP_STATE_RUNNING = "1";
	public static final String DEFAULT_BACKUP_STATE_FINISH = "2";
	public static final String DEFAULT_BACKUP_RESULT = "ots_backup_result";
	public static final String DEFAULT_BACKUP_RESULT_SUCCESS = "0";
	public static final String DEFAULT_BACKUP_RESULT_FAIL_HDFS = "1";
	public static final String DEFAULT_BACKUP_RESULT_FAIL_FTP = "2";
	public static final String DEFAULT_BACKUP_PROGRESS = "ots_backup_progress";
			
	public static final String DEFAULT_API_PATH_LOCALLOGIN = "/login";
	public static final String DEFAULT_API_PATH_LOCALLOGOUT = "/logout";
	
	public static final String ERROR_INFO_GROUPNAME_EMPTY = "the group name list is empty !";
	public static final String ERROR_INFO_OBJECTSID_EMPTY = "the permitted objects Id list is empty !";
	public static final String ERROR_INFO_DISNAME_OR_SERNAME_EMPTY = "the dispaly or service name is empty !";
	
	public static final String AAS_GROUPE_NAME = "group_name";
	public static final String AAS_TABLE_ID = "tableId";
	public static final String AAS_DISPLAY_RESOURCE_NAME = "display_resourceName";
	public static final String AAS_NAME = "name";
	public static final String AAS_PERMISSION_ACTION = "action";
	public static final String AAS_RELEVANT_SERVICE = "relevantService";
	public static final String AAS_RESOURCES = "resources";
	public static final String AAS_SERVICE = "service";
	public static final String AAS_RELEVANT_AUTHORITY ="relevant_authority";
	public static final String AAS_ADMAIN_GROUP = "admingroup";
	public static final String AAS_GROUPS = "groups";
	

}
