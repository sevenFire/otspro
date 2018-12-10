package com.baosight.xinsight.ots.rest.common;

public class RestErrorCode {
	public static final long EC_OTS_REST_BASE_NO = 173000;

	public static final long EC_OTS_REST_TABLE_INDEX_INVALID_OP = EC_OTS_REST_BASE_NO + 1;
	public static final long EC_OTS_REST_RECORD_UPDATE = EC_OTS_REST_BASE_NO + 2;
	public static final long EC_OTS_REST_METRICS_TABLE = EC_OTS_REST_BASE_NO + 3;
	public static final long EC_OTS_REST_METRICS_NAMESPACE = EC_OTS_REST_BASE_NO + 4;
	public static final long EC_OTS_REST_METRICS_LOCK = EC_OTS_REST_BASE_NO + 5;	
	public static final long EC_OTS_REST_INVALID_QUERY_TYPE = EC_OTS_REST_BASE_NO + 6;
	public static final long EC_OTS_REST_INVALID_DELETE_TYPE = EC_OTS_REST_BASE_NO + 7;
	public static final long EC_OTS_REST_INVALID_RECORDS = EC_OTS_REST_BASE_NO + 8;
	public static final long EC_OTS_REST_QUERY_INDEX = EC_OTS_REST_BASE_NO + 9;
	
	public static final long EC_OTS_REST_QUERY_TOKEN = EC_OTS_REST_BASE_NO + 10;

	public static final long EC_OTS_REST_UNKNOWNHOST = EC_OTS_REST_BASE_NO + 11;
	public static final long EC_OTS_REST_PARTIAL_INVALID_RECORDS = EC_OTS_REST_BASE_NO + 12;
	
	public static final long EC_OTS_REST_GET_CACEH_PERMISSION_EXCEPTION = EC_OTS_REST_BASE_NO + 13;
	public static final long EC_OTS_REST_GET_TABLE_PERMISSION_EXCEPTION = EC_OTS_REST_BASE_NO + 14;
	public static final long EC_OTS_AAS_REGISTER_RESSOURCE_FAILED = EC_OTS_REST_BASE_NO + 15;
	public static final long EC_OTS_REST_INIT_INNERTABLE = EC_OTS_REST_BASE_NO + 16;
}
