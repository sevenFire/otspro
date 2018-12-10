package com.baosight.xinsight.ots.cfgsvr.timer;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.recipes.lock.WriteLock;

import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.ots.cfgsvr.common.RestErrorCode;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;


public class DistributedLock { 	
	private static final Logger LOG = Logger.getLogger(DistributedLock.class);

	private WriteLock lock = null;
	private String dir;
	private List<ACL> acl;
	private ZooKeeper zk;
	
	public DistributedLock(String dir, List<ACL> acl){
		try {
			this.dir = dir;
			this.acl = acl;
			
			this.zk = new ZooKeeper(ConfigUtil.getInstance().getValue(ConfigConstants.ZOOKEEPER_QUORUM), 
					Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.ZOOKEEPER_TIMEOUT)), null);
			this.lock = new WriteLock(this.zk, this.dir, this.acl);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	public boolean tryLock() throws Exception {
		try {			
			return this.lock.lock();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(String.format("error:%d, try lock fail at %s", RestErrorCode.EC_OTS_REST_METRICS_LOCK, this.lock.getId()));

			if (e instanceof SessionExpiredException) {
				try {
					this.zk.close();
					this.zk = new ZooKeeper(ConfigUtil.getInstance().getValue(ConfigConstants.ZOOKEEPER_QUORUM), 
							Integer.parseInt(ConfigUtil.getInstance().getValue(ConfigConstants.ZOOKEEPER_TIMEOUT)), null);
					this.lock.unlock();
					this.lock = new WriteLock(this.zk, this.dir, this.acl);
				} catch (Exception ex) {
					//ignore
					ex.printStackTrace();
				}				
			}			
			return false;
		} 	
	}
	
	public void unLock(){
		try {
			this.lock.unlock();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}  