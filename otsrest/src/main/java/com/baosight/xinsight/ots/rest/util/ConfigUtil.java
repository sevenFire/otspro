package com.baosight.xinsight.ots.rest.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import redis.clients.jedis.JedisPoolConfig;

import com.baosight.xinsight.license.LicenseManager;
import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.config.ConfigReader;
import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.ots.OtsConfiguration;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.OtsAdmin;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.redis.RedisUtil;
import com.baosight.xinsight.utils.SystemUtil;

public class ConfigUtil {
	private static final Logger LOG = Logger.getLogger(ConfigUtil.class);

	private static ConfigUtil INSTANCE;
	private ConfigReader configReader;
	private LicenseManager licMgr = null;
	private static final String MODULE_NAME = "ots";
	private Pattern PATTERN = null;
	private OtsAdmin admin = null;
	private RedisUtil redisutil = null;
	
	private String hostName = null;	
	private static String PROJECT_NAME = "otsrest";
	private static String systemIdentifier = null;

	//public String system_version = "1.0";	

	//jedis
    private  JedisPoolConfig jedisPoolConfig = null; 
	private  int jedis_pool_maxTotal;
	private  int jedis_pool_maxIdle;
	private  int jedis_pool_numTestsPerEvictionRun;
	private  int jedis_pool_timeBetweenEvictionRunsMillis;
	private  int jedis_pool_minEvictableIdleTimeMillis;
	private  int jedis_pool_softMinEvictableIdleTimeMillis;
	private  int jedis_pool_maxWaitMillis;
	private  boolean jedis_pool_testOnBorrow;
	private  boolean jedis_pool_testWhileIdle;
	private  boolean jedis_pool_testOnReturn;
	private  boolean jedis_pool_jmxEnabled;
	private  String jedis_pool_jmxNamePrefix;
	private  boolean jedis_pool_blockWhenExhausted;
	
	public synchronized static ConfigUtil getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new ConfigUtil();
		}
		return INSTANCE;
	}
	
	private ConfigUtil() {
		try {	            
        	initEvn();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}  
	}

	public synchronized void stop() {
		if (INSTANCE != null) {		
			admin.finalize();
			licMgr.stop();
			LogManager.shutdown();
			INSTANCE = null;
		}
	}
	
	public static String getSystemIdentifier() {
		if (systemIdentifier == null) {
			systemIdentifier = SystemUtil.getSystemIdentifier(PROJECT_NAME);
		}
		return systemIdentifier;
	}
	
	public static void setSystemIdentifier(String systemIdentifier) {
		ConfigUtil.systemIdentifier = systemIdentifier;
	}

	public static String getProjectName() {
		return PROJECT_NAME;
	}
	
	public static void setProjectName(String projectName) {
		PROJECT_NAME = projectName;
	}
	
	private void initEvn() throws NumberFormatException, ConfigException, OtsException, Exception {
		String classPath = ConfigUtil.class.getClassLoader().getResource("/").getPath();
		String webRoot = classPath.substring(0, classPath.lastIndexOf("/WEB-INF/"));
		System.out.println("---webRoot path:" + webRoot); 
		//check the log directory 		
		String osType = System.getProperty("os.name");
		if (osType.toLowerCase().equals("sunos") || osType.toLowerCase().equals("linux")) {
			webRoot = CommonConstants.DEFAULT_WEBLOG_PATH;
			webRoot += PROJECT_NAME + "/" + PROJECT_NAME;
		} else {
			webRoot += "/logs/" + PROJECT_NAME;
		}
		System.out.println("---Log path:" + webRoot); 
		System.setProperty("log_path", webRoot); //log4j.properties
		
		configReader = new ConfigReader(MODULE_NAME, ConfigUtil.class);
		if (configReader.getModuleConfigFile() != null) {
			PropertyConfigurator.configureAndWatch(configReader.getModuleConfigFile(), 30*1000);			
		}
		
	 	PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}");	
		
		String postgres_quorum = configReader.getValue(ConfigConstants.POSTGRESQL_QUORUM, "127.0.0.1:5432");
		String  postgres_server = "127.0.0.1";
		String  postgres_port = "5432";
		if (null != postgres_quorum){
			String []postgres_hosts = postgres_quorum.split(",");
			if (postgres_hosts.length > 0){
				String []postgres_host = postgres_hosts[0].split(":");
				if (postgres_host.length > 0){
					postgres_server = postgres_host[0];
					if (postgres_host.length > 1)
						postgres_port = postgres_host[1];
				}
			}
		}	
				
		InetAddress inetAddr = InetAddress.getLocalHost();
		hostName = inetAddr.getHostName();
		
		//init otsclient
		OtsConfiguration conf = OtsConfiguration.create();
		//如果默认ots的conf.properties存在,这样都可以不配置
		conf.setProperty(OtsConstants.ZOOKEEPER_QUORUM, configReader.getValue(ConfigConstants.ZOOKEEPER_QUORUM, "127.0.0.1:2181"));
		conf.setProperty(OtsConstants.ZOOKEEPER_TIMEOUT, configReader.getValue(ConfigConstants.ZOOKEEPER_TIMEOUT, "3000"));
		conf.setProperty(OtsConstants.CLIENT_HBASE_RETRIES_NUMBER, getValue("hbase_client_retries_number"));
		conf.setProperty(OtsConstants.POSTGRES_QUORUM, postgres_server);
		conf.setProperty(OtsConstants.POSTGRES_PORT, postgres_port);
		conf.setProperty(OtsConstants.POSTGRES_DBNAME, configReader.getValue(ConfigConstants.OTS_DBNAME, "ots"));
		conf.setProperty(OtsConstants.POSTGRES_USERNAME, configReader.getValue(ConfigConstants.POSTGRESQL_USER, "postgres"));
		conf.setProperty(OtsConstants.POSTGRES_PASSWORD, configReader.getValue(ConfigConstants.POSTGRESQL_PASSWORD, "q1w2e3"));
		conf.setProperty(OtsConstants.INDEX_CONFIG_HOME, getValue("ots_indexer_cfghome"));
		conf.setProperty(OtsConstants.HBASE_INDEXER_QUORUM, configReader.getValue(ConfigConstants.OTS_INDEX_HOST, "127.0.0.1"));
		conf.setProperty(ConfigConstants.YARN_RM_HOST, configReader.getValue(ConfigConstants.YARN_RM_HOST, "127.0.0.1:8088"));
		conf.setProperty(ConfigConstants.REDIS_QUORUM, configReader.getValue(ConfigConstants.REDIS_QUORUM, "127.0.0.1:6379"));
		admin = new OtsAdmin(conf);  
		
		initredisParam();
		
		int license_check = Integer.parseInt(configReader.getValue("license_check_period", "30"));
		licMgr = new LicenseManager();
		licMgr.init(getLicenseServerAddr(), MODULE_NAME, license_check*60*1000L);   	
	}  
		
	//redis init config
	public void initredisParam() throws NumberFormatException, IOException {
    	jedis_pool_maxTotal = Integer.parseInt(getValue("jedis_pool_maxTotal"));
    	jedis_pool_maxIdle = Integer.parseInt(getValue("jedis_pool_maxIdle"));
    	jedis_pool_numTestsPerEvictionRun = Integer.parseInt(getValue("jedis_pool_numTestsPerEvictionRun"));
    	jedis_pool_timeBetweenEvictionRunsMillis = Integer.parseInt(getValue("jedis_pool_timeBetweenEvictionRunsMillis"));
    	jedis_pool_minEvictableIdleTimeMillis = Integer.parseInt(getValue("jedis_pool_minEvictableIdleTimeMillis"));;
    	jedis_pool_softMinEvictableIdleTimeMillis = Integer.parseInt(getValue("jedis_pool_softMinEvictableIdleTimeMillis"));
    	jedis_pool_maxWaitMillis = Integer.parseInt(getValue("jedis_pool_maxWaitMillis"));
    	jedis_pool_testOnBorrow = Boolean.parseBoolean(getValue("jedis_pool_testOnBorrow"));
    	jedis_pool_testWhileIdle = Boolean.parseBoolean(getValue("jedis_pool_testWhileIdle"));
    	jedis_pool_testOnReturn = Boolean.parseBoolean(getValue("jedis_pool_testOnReturn"));
    	jedis_pool_jmxEnabled = Boolean.parseBoolean(getValue("jedis_pool_jmxEnabled"));
    	jedis_pool_jmxNamePrefix = getValue("jedis_pool_jmxNamePrefix_rest");
    	jedis_pool_blockWhenExhausted = Boolean.parseBoolean(getValue("jedis_pool_blockWhenExhausted"));        	
    	
	 	jedisPoolConfig = new JedisPoolConfig();         	  	
        jedisPoolConfig.setMaxTotal(jedis_pool_maxTotal);//the max number of connection  
        jedisPoolConfig.setMaxIdle(jedis_pool_maxIdle);//the max number of free  
        jedisPoolConfig.setMaxWaitMillis(jedis_pool_maxWaitMillis);//the longest time of waiting              
        jedisPoolConfig.setNumTestsPerEvictionRun(jedis_pool_numTestsPerEvictionRun);
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(jedis_pool_timeBetweenEvictionRunsMillis);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(jedis_pool_minEvictableIdleTimeMillis);
        jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(jedis_pool_softMinEvictableIdleTimeMillis);            
        jedisPoolConfig.setTestOnBorrow(jedis_pool_testOnBorrow);
        jedisPoolConfig.setTestWhileIdle(jedis_pool_testWhileIdle);
        jedisPoolConfig.setTestOnReturn(jedis_pool_testOnReturn);
        jedisPoolConfig.setJmxEnabled(jedis_pool_jmxEnabled);
        jedisPoolConfig.setJmxNamePrefix(jedis_pool_jmxNamePrefix);
        jedisPoolConfig.setBlockWhenExhausted(jedis_pool_blockWhenExhausted);
        
        redisutil = new RedisUtil(getValue(ConfigConstants.REDIS_QUORUM), getValue(ConfigConstants.REDIS_PASSWORD), jedisPoolConfig);
	}
	
	public String getValue(String key) throws IOException {		
        String value = configReader.getValue(key, null);
        if (value == null) {
        	LOG.warn("no property:" + key);
			return null;
		}
        Matcher matcher = PATTERN.matcher(value);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String matcherKey = matcher.group(1);
            String matchervalue = configReader.getValue(matcherKey, null);
            if (matchervalue != null) {
                matcher.appendReplacement(buffer, matchervalue);
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
	}
	
	public String getValue(String key, String defaultValue) throws IOException {	
		return configReader.getValue(key, defaultValue);
	}
	
    ///////////////////////////////////////////////////////////////////////////////

    public OtsAdmin getOtsAdmin() {
		return admin;
	}
    
    public RedisUtil getRedisUtil() {
		return redisutil;
	}
	    
    public String getIndexerServerAddrRandom()
    {
    	String hbase_indexer_addr = configReader.getValue(ConfigConstants.OTS_INDEX_HOST, "127.0.0.1");
    	String[] indexerServerAddr = hbase_indexer_addr.split(",");
        return (new StringBuilder()).append(indexerServerAddr[(new Random()).nextInt(indexerServerAddr.length)]).append(":11060").toString();
    }
    
    public String getAuthServerAddr()
    {
    	String aas_host = configReader.getValue(ConfigConstants.AAS_HOST, "127.0.0.1:8080");
		String aas_service_name = configReader.getValue(ConfigConstants.AAS_REST_SERVICE_NAME, "aas");
        return aas_host + "/" + aas_service_name;
    }
    
    private String getLicenseServerAddr()
    {
    	String license_manager_addr = configReader.getValue(ConfigConstants.LICENSE_HOST, "127.0.0.1:8080");
    	String[] licenseServerAddr = license_manager_addr.split(",");
        return (new StringBuilder()).append(licenseServerAddr[(new Random()).nextInt(licenseServerAddr.length)]).toString();
    }
    
    public LicenseManager getLicenseMgr() {
        return licMgr;
    }

	public String getHostName() throws OtsException {
		try {
			if (hostName == null) {
				InetAddress inetAddr = InetAddress.getLocalHost();
				hostName = inetAddr.getHostName();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new OtsException(RestErrorCode.EC_OTS_REST_UNKNOWNHOST, e.getMessage());
		}

		return hostName;
	}
	
	public String getYarnWebappAddr() {
		return configReader.getValue(ConfigConstants.YARN_RM_HOST, "127.0.0.1:8088");
	}
	
	public String getRedisHost() {
		return configReader.getValue(ConfigConstants.REDIS_QUORUM, "127.0.0.1:6379");
	}
}
