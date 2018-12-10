package com.baosight.xinsight.ots.cfgsvr.timer;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.baosight.xinsight.ots.client.exception.ConfigException;
import com.baosight.xinsight.ots.client.exception.TableException;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;
import com.baosight.xinsight.ots.rest.service.MetricsService;

public class MetricsJob implements Job {
	private static final Logger LOG = Logger.getLogger(MetricsJob.class);

	private static DistributedLock lock = null;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		boolean bLock = false;
		try {	
			LOG.debug("try to get lock to execute metrics job!");	
			if (null == lock){
				lock = new DistributedLock(RestConstants.DEFAULT_METRICS_LOCK_PREFIX, null);
			}
			bLock = lock.tryLock();
			if (!bLock) {
				LOG.debug("fail to get lock to execute metrics job!");	
				return;
			}
			LOG.debug("MetricsJob execute begin!");	
			
			List<Long> listTenants = ConfigUtil.getInstance().getOtsAdmin().getAllTenants();
			if(null != listTenants) {
				for (Long tenantid : listTenants) {
					if (tenantid != null) {							
						try {
							MetricsService.getMetricsInfoByNamespaceFromHbase(tenantid);					
						} catch (Exception e) {
							e.printStackTrace();
							LOG.debug(e.getMessage());
						}
					}
				}
			}
			LOG.debug("MetricsJob execute successful!");
		} catch (ConfigException e) {
			LOG.error("MetricsJob execute error ConfigException!");
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error("MetricsJob execute error IOException!");
			e.printStackTrace();
		} catch (TableException e) {
			LOG.error("MetricsJob execute error TableException!");
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOG.error("MetricsJob execute error InterruptedException!");
			e.printStackTrace();
		} catch (Exception e) {
			LOG.error("MetricsJob execute error Exception!");
			e.printStackTrace();
		}finally{
			if (null != lock && bLock){
				lock.unLock();
			}
		}
	}	
}


