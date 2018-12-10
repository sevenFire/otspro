package com.baosight.xinsight.ots.cfgsvr.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.config.ConfigReader;
import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.common.RestErrorCode;
import com.baosight.xinsight.utils.SystemUtil;

public class ConfigUtil {
	private static final Logger LOG = Logger.getLogger(ConfigUtil.class);

	private static ConfigUtil INSTANCE;
	private ConfigReader configReader;
	private static final String MODULE_NAME = "ots";
	private Pattern PATTERN = null;
	private String hostName = null;	

	public Boolean _isDebugModeBoolean = false;	
	private static String PROJECT_NAME = "otscfgsvr";
	//public String system_version = "1.0";	
	private static String systemIdentifier = null;

	public synchronized static ConfigUtil getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new ConfigUtil();
		}
		return INSTANCE;
	}
	
	private ConfigUtil(){
		try {	            
        	initEvn();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
		}  
	}

	public synchronized void stop() {
		com.baosight.xinsight.ots.rest.util.ConfigUtil.getInstance().stop();

		if (INSTANCE != null)  {
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
	
	public static String getProjectName() {
		return PROJECT_NAME;
	}

	public static void setProjectName(String projectName) {
		PROJECT_NAME = projectName;
	}
	
	private void initEvn() throws NumberFormatException, ConfigException, OtsException, Exception {
		com.baosight.xinsight.ots.rest.util.ConfigUtil.getInstance();
		com.baosight.xinsight.ots.rest.util.ConfigUtil.setProjectName(PROJECT_NAME);
		com.baosight.xinsight.ots.rest.util.ConfigUtil.setSystemIdentifier(getSystemIdentifier());

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

		InetAddress inetAddr = InetAddress.getLocalHost();
		hostName = inetAddr.getHostName();
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
	
	public HashMap<String, String> getVisibleValues() throws Exception {	
		return configReader.getAllVisibleConfig();
	}
	
    ///////////////////////////////////////////////////////////////////////////////	     
    
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
	
    public String getAuthServerAddr()
    {
    	String aas_host = configReader.getValue(ConfigConstants.AAS_HOST, "127.0.0.1:8080");
		String aas_service_name = configReader.getValue(ConfigConstants.AAS_REST_SERVICE_NAME, "aas");
        return aas_host + "/" + aas_service_name;
    }
}
