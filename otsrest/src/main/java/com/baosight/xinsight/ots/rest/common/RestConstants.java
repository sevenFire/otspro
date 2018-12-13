package com.baosight.xinsight.ots.rest.common;

public class RestConstants {
	
	public static final int UNKNOWN_OPERATE = 8;
	public static final int TIMERANGE_OPERATE = 7;
	public static final int ANYKEY_OPERATE = 6;
	public static final int HASH_OPERATE = 5;
	public static final int MULTIKEY_OPERATE = 4;
	public static final int PREFIX_OPERATE = 3;
	public static final int RANGE_PREFIX_OPERATE = 2;
	public static final int RANGE_OPERATE = 1;
	public static final int KEY_OPERATE = 0;
		
	public static final int ITERATE_CURSOR_MARK = 1;
	public static final int ITERATE_OFFSET = 0;
		
	public static final int QUERY_FROM_TYPE_REST = 1;
	public static final int QUERY_FROM_TYPE_BOARD = 0;
	
	public static final long MOB_THRESHOLD_MAX_LIMIT = 10*1024;//unit KB, max was 10M
	public static final long MOB_THRESHOLD_MIN_LIMIT = 100;//unit KB, min was 100k

	public static final long DEFAULT_QUERY_LIMIT = 100;
	public static final long DEFAULT_QUERY_OFFSET = 0;
	public static final long DEFAULT_QUERY_MAX_LIMIT = 10000;
	
	public static final long DEFAULT_MAX_MILLISECOND = 31536000000000L;	//2970-01-01

	public static final String OTS_RANGEKEY = "range_key";
	public static final String OTS_HASHKEY = "hash_key";
	
	public static final String DEFAULT_METRICS_PREFIX = "ots_metrics_";
	public static final String METRICS_TABLE_READ_COUNT = "read_count";
	public static final String METRICS_TABLE_WRITE_COUNT = "write_count";
	public static final String METRICS_TABLE_DISK_SIZE = "disk_size";
	public static final String METRICS_TABLE_ROW_COUNT = "record_count";
	public static final String METRICS_REGION_COUNT = "region_count";
		
	public static final String Query_name = "name";
	public static final String Query_all_tables = "_all_tables";
	public static final String Query_column = "column";
	public static final String Query_columns = "columns";
	public static final String Query_limit = "limit";
	public static final String Query_offset = "offset";
	public static final String Query_descending = "descending";

	public static final String Query_hashkey = "hash_key";
	public static final String Query_rangekey = "range_key";
	public static final String Query_rangekey_start = "range_key_start";
	public static final String Query_rangekey_end = "range_key_end";
	public static final String Query_rangekey_prefix = "range_key_prefix";
	public static final String Query_rangekey_cursormark = "range_key_cursor_mark";
	
	public static final String Query_start_time = "start_time";
	public static final String Query_end_time = "end_time";
	
	public static final String Query_all_indexes = "_all_indexes";
	public static final String Query_query = "query";
	public static final String Query_secondary_index_query = "secondary_index_query";
	public static final String Query_filters = "filters";	
	public static final String Query_orders = "orders";
	public static final String Query_indexcursor = "cursor_mark";
	public static final String Query_queryfrom = "query_from";
	public static final String Query_column_ranges = "column_ranges";
	
	public static final int OTS_INDEX_TYPE_SOLR = 0;
	public static final int OTS_INDEX_TYPE_HBASE = 1;
	public static final String Query_index_type = "index_type";
	
	public static final String DEFAULT_BACKUP_PREFIX = "ots_backup_";

	public static final String DEFAULT_KEYWORD_PROGRESS = "progress";
	public static final String DEFAULT_PROGRESS_FAILED = "-1";
	public static final String DEFAULT_PROGRESS_INIT = "0";
	public static final String DEFAULT_PROGRESS_COMPLETE = "100";
	
	public static final String Query_table_id = "table_id";
	public static final String Query_index_id = "index_id";
	
	public static final long OTS_MANAGE_PERMISSION_OPERATION = -1L;
	public static final String OTS_PERMISSION_KAFKA_TOPIC = "ots_kafka_permission_topic";
	public final static String OTS_PERMISSION_CACHE_KEY = "ots:";
	
	public static final Long OTS_WEBOP_ADD = 0L;
	public static final Long OTS_WEBOP_DELETE = 1L;
	public static final Long OTS_WEBOP_UPDATE = 4L;
}
