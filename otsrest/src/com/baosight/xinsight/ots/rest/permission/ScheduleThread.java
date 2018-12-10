package com.baosight.xinsight.ots.rest.permission;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.rest.permission.CachePermission.CacheItem;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.ehcache.Cache.Entry;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimerTask;

/**
 * OtsRestScheduleThread
 * 
 * @author zhangyunlong
 * @created 2017.04.28
 */
public class ScheduleThread extends TimerTask {
	private static final Logger LOG = Logger.getLogger(ScheduleThread.class);
	private Iterator<Entry<String, CacheItem>> iter = null;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String calledServiceName = " ";
    
	public ScheduleThread(String calledServiceName){
		this.calledServiceName = calledServiceName;
	}
	
	// generate singleton
	@Override
	public void run() {
		Date threadExecute = new Date();
		try {
			LOG.trace((new StringBuilder()).append("schedule thread start, permission cache map HashCode: ")
					.append(CachePermission.getInstance().hashCode()).append(" schedule mession execute ")
					.append(sdf.format(threadExecute)).toString());

			iter = CachePermission.getInstance().getEhcacheManager().iterator(OtsConstants.OTS_PERMISSION_EHC_CACHE_NAME, String.class, CacheItem.class);
			// update cache permission
			CachePermission.getInstance().updateCachePermission(iter, calledServiceName);
		} catch (NumberFormatException | IOException e) {
			LOG.error(ExceptionUtils.getFullStackTrace(e));
		}
	}
}
