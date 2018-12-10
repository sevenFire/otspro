package com.baosight.xinsight.ots.cfgsvr.common;

public class RestErrorCode extends com.baosight.xinsight.ots.rest.common.RestErrorCode {
	
	public static final long EC_OTS_TABLE_BACKUP_RUNNING = EC_OTS_REST_BASE_NO + 21;
	public static final long EC_OTS_TABLE_BACKUP_FTP_CONNECT_FAILURE = EC_OTS_REST_BASE_NO + 22;
	public static final long EC_OTS_TABLE_BACKUP_FTP_FOLDER_NOT_EXIST = EC_OTS_REST_BASE_NO + 23;
	public static final long EC_OTS_TABLE_BACKUP_HDFS_JOB_ERROR = EC_OTS_REST_BASE_NO + 24;
	
	public static final long EC_OTS_TABLE_BACKUP_BACKUP_FAILED = EC_OTS_REST_BASE_NO + 25;
	public static final long EC_OTS_TABLE_BACKUP_RESTORE_FAILED = EC_OTS_REST_BASE_NO + 26;	
	
//	//permission errcode
//	public static final long EC_OTS_MANAGE_INSTANCE_ADD_FAILED = EC_OTS_REST_BASE_NO + 27;
//	public static final long EC_OTS_NO_PERMISSION_FAULT = EC_OTS_REST_BASE_NO + 28;
//	public static final long EC_GROUPNAME_EMPTY = EC_OTS_REST_BASE_NO + 29;
//	public static final long EC_OTS_GET_GROUP_LIST_FAILED = EC_OTS_REST_BASE_NO + 30;
//	public static final long EC_OTS_OBJECTID_EMPTY = EC_OTS_REST_BASE_NO + 31;
//	public static final long EC_PARAMERTERS_NOT_COMPLETE = EC_OTS_REST_BASE_NO + 32;
}
