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

public class RowCountJob implements Job {
	private static final Logger LOG = Logger.getLogger(RowCountJob.class);

	private static DistributedLock lock = null;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		boolean bLock = false;
		try {
			LOG.debug("try to get lock to execute rowcount job!");	
			if (null == lock){
				lock = new DistributedLock(RestConstants.DEFAULT_ROWCOUNT_LOCK_PREFIX, null);
			}
			bLock = lock.tryLock();
			if (!bLock) {
				LOG.debug("fail to get lock to execute rowcount job!");	
				return;
			}
			
			LOG.debug("RowCountJob execute begin!");			
			List<Long> listTenants = ConfigUtil.getInstance().getOtsAdmin().getAllTenants();
			if(null != listTenants) {
				for (Long tenantid : listTenants) {
					if (tenantid != null) {							
						try {
							MetricsService.getRowCountByNamespaceFromHbase(tenantid);					
						} catch (Exception e) {
							e.printStackTrace();
							LOG.debug(e.getMessage());
						}
					}
				}
			}
			LOG.debug("RowCountJob execute successful!");
		} catch (ConfigException e) {
			LOG.error("RowCountJob execute error ConfigException!");
			e.printStackTrace();
		} catch (IOException e) {
			LOG.error("RowCountJob execute error IOException!");
			e.printStackTrace();
		} catch (TableException e) {
			LOG.error("RowCountJob execute error TableException!");
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOG.error("RowCountJob execute error InterruptedException!");
			e.printStackTrace();
		} catch (Exception e) {
			LOG.error("RowCountJob execute error Exception!");
			e.printStackTrace();
		}finally{
			if (null != lock && bLock){
				lock.unLock();
			}
		}
	}	
}
