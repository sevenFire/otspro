package com.baosight.xinsight.ots.client.index;

import java.io.File;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.hadoop.hbase.util.Bytes;

import com.baosight.xinsight.ots.OtsConstants;

public class CollectionLock { 
	private static final String SEQUENTIAL_SEP = "#";
	
	public class LockOperInfo {
		private long sessionId;
		private String hostName;
		private long seqId;
		public void parse(String lockName) {
			int secSep = lockName.lastIndexOf(SEQUENTIAL_SEP);
			seqId = Long.parseLong(lockName.substring(secSep+1));
			int firstSep = lockName.indexOf(SEQUENTIAL_SEP);
			sessionId = Long.parseLong(lockName.substring(firstSep+1, secSep));
			hostName = lockName.substring(0, firstSep);
		}
		
		public long getSessionId() {
			return sessionId;
		}
		
		public void setSessionId(long sessionId) {
			this.sessionId = sessionId;
		}
		
		public String getHostName() {
			return hostName;
		}
		
		public void setHostName(String hostName) {
			this.hostName = hostName;
		}
		
		public long getSeqId() {
			return seqId;
		}
		
		public void setSeqId(long seqId) {
			this.seqId = seqId;
		}
	}
	
	public class LockOwner{
		private String ownerHostName;
		private long ownerSessionID;
		private long lockTime;
		
		public long getLockTime() {
			return lockTime;
		}
		
		public void setLockTime(long lockTime) {
			this.lockTime = lockTime;
		}
		
		public long getOwnerSessionID() {
			return ownerSessionID;
		}
		
		public void setOwnerSessionID(long ownerSessionID) {
			this.ownerSessionID = ownerSessionID;
		}
		
		public String getOwnerHostName() {
			return ownerHostName;
		}
		
		public void setOwnerHostName(String ownerHostName) {
			this.ownerHostName = ownerHostName;
		}
	}
	
	private LockOwner getOwner(ZooKeeper zk, String rootPath) throws KeeperException, InterruptedException {
		List<String> childList = zk.getChildren(rootPath, null);
		int minSeq = -1;
		LockOwner owner = new LockOwner();
		for(String child:childList) {
			int seq = Integer.parseInt(child.substring(child.lastIndexOf(SEQUENTIAL_SEP) + 1));
			if(minSeq == -1) {
				minSeq = seq;
				LockOperInfo operInfo = new LockOperInfo();
				operInfo.parse(child);
				owner.setOwnerHostName(operInfo.getHostName());
				owner.setOwnerSessionID(operInfo.getSessionId()); 
				
			}
			else if(seq < minSeq) {
				minSeq = seq;
				LockOperInfo operInfo = new LockOperInfo();
				operInfo.parse(child);
				owner.setOwnerHostName(operInfo.getHostName());
				owner.setOwnerSessionID(operInfo.getSessionId()); 
			}		
		}
		
		if(minSeq == -1) {
			return null;
		}
		else {
			return owner;
		}
	}

	private String getCollectionRootPath(String collectionName) {
		StringBuilder rootPath = new StringBuilder();
		rootPath.append(OtsConstants.BAOSIGHT_OTS_INDEX);
		rootPath.append(File.separator);
		rootPath.append(collectionName);
		return rootPath.toString();
	}
	
	public boolean tryLock(ZooKeeper zk, String collectionName, String hostName) throws KeeperException, InterruptedException {
		String strRootPath = getCollectionRootPath(collectionName);		
		
		Stat stat= zk.exists(OtsConstants.BAOSIGHT_OTS, null);		
		if(null == stat) {
			zk.create(OtsConstants.BAOSIGHT_OTS, OtsConstants.BAOSIGHT_OTS.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create(OtsConstants.BAOSIGHT_OTS_INDEX, OtsConstants.BAOSIGHT_OTS_INDEX.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		stat= zk.exists(strRootPath, null);
		long mySessionId = zk.getSessionId();
		
		if(null == stat) {
			zk.create(strRootPath, collectionName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	
		StringBuilder zkLockPath = new StringBuilder();
		zkLockPath.append(strRootPath);
		zkLockPath.append(File.separator);
		zkLockPath.append(hostName);
		zkLockPath.append(SEQUENTIAL_SEP);
		zkLockPath.append(mySessionId);
		zkLockPath.append(SEQUENTIAL_SEP);
		zk.create(zkLockPath.toString(), Bytes.toBytes(System.currentTimeMillis()), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		LockOwner owner = getOwner(zk, strRootPath);		
		if(null != owner) {
			if(owner.getOwnerHostName().equals(hostName) && owner.getOwnerSessionID() == mySessionId) {
				return true;
			}
		}
		
		return false;
	}
	
	public void unLock(ZooKeeper zk, String collectionName, String hostName) throws InterruptedException, KeeperException {
		String rootPath = getCollectionRootPath(collectionName);
		LockOwner owner = getOwner(zk,rootPath);
		if(owner.getOwnerHostName().equals(hostName)) {
			List<String> childList = zk.getChildren(rootPath, null);
			for(String child:childList)	{
				zk.delete(rootPath + File.separator + child, -1);
			}
			zk.delete(rootPath, -1);		
		}
	}

	public LockOwner getOwner(ZooKeeper zk, String collectionName, String hostName) throws KeeperException, InterruptedException {
		return getOwner(zk, getCollectionRootPath(collectionName));
	}
}  