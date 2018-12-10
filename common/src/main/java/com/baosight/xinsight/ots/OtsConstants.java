package com.baosight.xinsight.ots;

public class OtsConstants {

    public static final String DEFAULT_FAMILY_NAME = "f";
    public static final int DEFAULT_REPLICATED_SCOPE = 0;
    public static final int ENABLE_REPLICATED_SCOPE = 1;
    public static final String DEFAULT_SOLR_DOCUMENT_ID = "id";
    //public static final String DEFAULT_ROWKEY_NAME = "id";

    public static final long DEFAULT_MEM_STORE_FLUSH_SIZE = 4096 * 2000;
    public static final int DEFAULT_META_SCANNER_CACHING = 100;
    public static final int DEFAULT_CLIENT_WRITE_BUFFER = 2097152;//2*1024*1024

    public static final int OTS_INDEX_PATTERN_ONLINE = 1;
    public static final int OTS_INDEX_PATTERN_OFFLINE = 0;
    public static final int OTS_INDEX_PATTERN_SECONDARY_INDEX = 9;

    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final int DEFAULT_QUERY_LIMIT = 100;
    public static final int DEFAULT_QUERY_OFFSET = 0;
    public static final int DEFAULT_QUERY_MAX_LIMIT = 10000;
    public static final String DEFAULT_QUERY_CURSOR_START = "*";

    //0--none,1--snappy,2--lz4,3--gz,4--lzo
    public static final int TABLE_ALG_NONE = 0;
    public static final int TABLE_ALG_SNAPPY = 1;
    public static final int TABLE_ALG_LZ4 = 2;
    public static final int TABLE_ALG_GZ = 3;
    public static final int TABLE_ALG_LZO = 4;

    public static final String METRICS_DISK_SIZE = "disk_size";
    public static final String METRICS_READ_COUNT = "read_count";
    public static final String METRICS_WRITE_COUNT = "write_count";
    public static final String METRICS_REGION_COUNT = "region_count";

    //////////////////////////input param
    public static final String ZOOKEEPER_TIMEOUT = "ots.zookeeper.timeout";
    public static final String CLIENT_HBASE_RETRIES_NUMBER = "ots.client.hbase.retries.number";
    public static final String ZOOKEEPER_QUORUM = "ots.zookeeper.quorum";

    public static final String POSTGRES_QUORUM = "ots.postgres.quorum";
    public static final String POSTGRES_USERNAME = "ots.postgres.username";
    public static final String POSTGRES_PASSWORD = "ots.postgres.password";
    public static final String POSTGRES_PORT = "ots.postgres.port";
    public static final String POSTGRES_DBNAME = "ots.postgres.dbname";

    public static final String HBASE_INDEXER_QUORUM = "ots.hbase.indexer.quorum";
    public static final String INDEX_CONFIG_HOME = "ots.index.config.home";

    /////////////////////////////index
    public static final String CONFIG_FILE_MORPHLINES_HBASE_MAPPER = "morphline-hbase-mapper.xml";
    public static final String CONFIG_FILE_MORPHLINES = "morphlines.conf";
    public static final String ZK_COLLECTION_PATH = "/solr/collections/";
    public static final String ZK_COLLECTION_PATH_WITHOUT_SLASH = "/solr/collections";
    public static final String ZK_SOLR_CONFIG_PATH = "/solr/configs/";
    public static final String CONFIG_FILE_SCHEMA = "schema.xml";
    public static final String COLLECTION_NAME_SEPRATOR = "__";
    public static final char ZKHOST_SEPRATOR = ':';

    public static final String BAOSIGHT_OTS = "/baosight_ots";
    public static final String BAOSIGHT_OTS_INDEX = BAOSIGHT_OTS + "/indexer";

    public static final int DEF_MAX_INDEX_NAME_LENGTH = 32;

    public static final String OTS_INDEXES = "ots.indexs";
    public static final String OTS_INDEX_TABLE_SUFFIX = "__ots__idx";
    public static final String OTS_INDEX_TABLE_SPLIT = "__";

    public static final int OTS_SEC_INDEX_TYPE_INT_LEN = 4;
    public static final int OTS_SEC_INDEX_TYPE_FLOAT_LEN = 4;
    public static final int OTS_SEC_INDEX_TYPE_LONG_LEN = 8;
    public static final int OTS_SEC_INDEX_TYPE_DOUBLE_LEN = 8;
    public static final int OTS_SEC_INDEX_TYPE_SHORT_LEN = 2;
    public static final int OTS_SEC_INDEX_TYPE_CHAR_LEN = 1;

    public static final int OTS_SEC_INDEX_DEF_COL_MAX_LEN = 64;
    public static final String OTS_RESOURCE_MANAGE = "ots_manage";
    public static final String OTS_MAPREDUCE_JAR_PATH = "/apps/xinsight/yarn/lib/xinsight-ots-mapreduce-*.jar";

    public static final String OTS_ERROR_CODE = "errcode";
    public static final String OTS_ERROR_INFO = "errinfo";
    public static final String OTS_TOTAL_COUNT = "total_count";
    public static final String OTS_SERVICE_NAME = "OTS";
    public static final String OTS_SERVICE_OTSCFGSVR_NAME = "OTSCFGSVR";
    public static final String OTS_SERVICE_OTSREST_NAME = "OTSREST";
    //ots ehcache config
    public static final String OTS_PERMISSION_EHC_CACHE_NAME = "ots_permission_cache";
    public static final String OTS_EHC_MAX_MEMO_ELEMENT_NUMB = "ehc_max_memory_numb";
    public static final String OTS_EHC_MAX_INTERVAL_TIME = "ehc_max_interval_time"; //unit seconds
    public static final String OTS_EHC_MAX_ALIVED_TIME = "ehc_max_alived_time"; //unit seconds; 0 is infinity
}
