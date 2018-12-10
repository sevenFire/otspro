package com.baosight.xinsight.ots.client.index;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.client.index.CollectionLock.LockOwner;

public class CollectionWatcher implements Runnable {
	private static final Logger LOG = Logger.getLogger(CollectionWatcher.class);

	private String zkHost;
	private int zkPort;
	private int timeout; 
	private boolean run;
	public String otsHome = null;

	public CollectionWatcher(String otsHome, String zkHost, int timeout) {
		this.zkHost = zkHost;
		this.timeout = 60000;
		this.otsHome = otsHome;
		this.run = true;
	}
	
	private boolean CollectionBuilding(String collectionName) {
		try {
			int count = 0;
			String cmd = "ps -ef|grep 'collection " +collectionName+" '|wc -l";
			String[] cmds = new String[]{"/bin/sh", "-c", cmd};
			
			Process ps = Runtime.getRuntime().exec(cmds);
			StreamGobbler errorGobbler = new StreamGobbler(ps.getErrorStream(), "Error");  
			StreamGobbler outputGobbler = new StreamGobbler(ps.getInputStream(), "Output");  
			errorGobbler.start();  
			outputGobbler.start();
			
			int ret = ps.waitFor();
			if(0 == ret)
			{
				//System.out.println("Exec complete! exit code:" + ps.exitValue());
				LOG.trace("Exec complete! exit code:" + ps.exitValue());
				
				BufferedReader br = new BufferedReader(new InputStreamReader(outputGobbler.is));
				String resultString = br.readLine();
				if(null != resultString)
				{
					count = Integer.parseInt(resultString);
				}
				
				if(count > 2)
				{
					return true;
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	// names[0] username, names[1] tablename, names[2] indexName
	@SuppressWarnings("unused")
	private String[] parseCollectionName(String collectionName)
	{
		String[] names= new String[3];
		int indexNameStartOffset = collectionName.lastIndexOf(OtsConstants.COLLECTION_NAME_SEPRATOR);
		names[2] = collectionName.substring(indexNameStartOffset + 2);
		String userAndTableName = collectionName.substring(0, indexNameStartOffset);
		int tableNameStartOffset = userAndTableName.lastIndexOf(OtsConstants.COLLECTION_NAME_SEPRATOR);
		names[0] = userAndTableName.substring(0, tableNameStartOffset);
		names[1] = userAndTableName.substring(tableNameStartOffset + 2, indexNameStartOffset);
		
		return names;
	}

	@Override
	public void run() {
		ZooKeeper zkKeeper = null;
		
		while (run) {
			try {
				if(null == zkKeeper)
				{
					zkKeeper = new ZooKeeper(zkHost + OtsConstants.ZKHOST_SEPRATOR + zkPort, timeout, null);
				}
				
				if(!zkKeeper.getState().isConnected())
				{
					zkKeeper.close();
					zkKeeper = new ZooKeeper(zkHost + OtsConstants.ZKHOST_SEPRATOR + zkPort, timeout, null);
				}
				
				org.apache.zookeeper.data.Stat stat= zkKeeper.exists(OtsConstants.BAOSIGHT_OTS, null);
				
				if(null == stat)
				{
					zkKeeper.create(OtsConstants.BAOSIGHT_OTS, OtsConstants.BAOSIGHT_OTS.getBytes(), Ids.OPEN_ACL_UNSAFE, 
							CreateMode.PERSISTENT);
					zkKeeper.create(OtsConstants.BAOSIGHT_OTS_INDEX, OtsConstants.BAOSIGHT_OTS_INDEX.getBytes(), 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				
				// check build lock
				List<String> collectionLocks = zkKeeper.getChildren(OtsConstants.BAOSIGHT_OTS_INDEX, null);
				for(String collection:collectionLocks)
				{
					CollectionLock lock = new CollectionLock();
					if(!CollectionBuilding(collection))
					{
						String hostName = InetAddress.getLocalHost().getHostName();
						LockOwner lockOwner = lock.getOwner(zkKeeper, collection, hostName);
						if(lockOwner.getLockTime() + 10000 < System.currentTimeMillis())
						{
							lock.unLock(zkKeeper, collection, hostName);
						}						
					}
					else {
						//Date now = new Date(System.currentTimeMillis());
						//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						//System.out.println(sdf.format(now) + " Collection:" + collection + " is building!");
						LOG.debug("Collection:" + collection + " is building!");
					}
				}
				
				Thread.sleep(3000);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		if(null != zkKeeper)
		{
			try {
				zkKeeper.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}

	public boolean isRun() {
		return run;
	}

	public void setRun(boolean run) {
		this.run = run;
	}
}
