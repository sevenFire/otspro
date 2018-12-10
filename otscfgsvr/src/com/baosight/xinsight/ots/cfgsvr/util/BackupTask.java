package com.baosight.xinsight.ots.cfgsvr.util;

import java.io.InputStream;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.ots.cfgsvr.common.RestConstants;
import com.baosight.xinsight.ots.cfgsvr.common.RestErrorCode;
import com.baosight.xinsight.ots.cfgsvr.model.operate.TableBackupModel;
import com.baosight.xinsight.ots.cfgsvr.service.TableService;
import com.baosight.xinsight.ots.client.OtsTable;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;

public class BackupTask implements Runnable {
	private static final Logger LOG = Logger.getLogger(BackupTask.class);

	public int type;
	public TableBackupModel model;
	public OtsTable table;
	private String tablename;
	private String rediskeyTablename;
	private String rediskeyTenantId;
	private String path;
	
	public BackupTask(int t, OtsTable table, TableBackupModel backupModel) {
		this.type = t;
		this.table = table;	
		this.model = backupModel;
	}
	
	public void run(){
		try {
			tablename = table.getTenantidAsString() + ":" + table.getName();
			path = table.getTenantidAsString() + CommonConstants.DEFAULT_SLASH_SPLIT + table.getName();
			rediskeyTablename = TableService.getRedisKeyTableName(table.getTenantid(), table.getId(), table.getName());
			rediskeyTenantId = TableService.getRedisKeyTenantId(table.getTenantid());
		
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_RUNNING);
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_RUNNING);
			Thread.currentThread();
			Thread.sleep(4000);
			switch (type) {
			case RestConstants.DEFAULT_BACKUP_TYPE_BACKUP:
				backup(model, path);
				break;
			case RestConstants.DEFAULT_BACKUP_TYPE_RESTORE:
				restore(model, path);
				break;
			default:
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_HDFS);
		} finally {
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_FINISH);
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTenantId, RestConstants.DEFAULT_BACKUP_STATE, RestConstants.DEFAULT_BACKUP_STATE_FINISH);
		}
	}
	
	public void backup(TableBackupModel backupModel, String path) throws Exception {
		HDFSUtil hdfs = null;
		InputStream inStream = null; 
		FTPUtil ftp = null;
		try {			
			hdfs = new HDFSUtil();
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "0");
			int isJobSuccessful = hdfs.exportTable(tablename, path);
			if (isJobSuccessful != 0) {
				LOG.error("Export table " + tablename + " mapreduce job error, result: " + isJobSuccessful);
				 throw new OtsException(RestErrorCode.EC_OTS_TABLE_BACKUP_HDFS_JOB_ERROR, "Failed to backup table because hdfs error!");
			}
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "80");
		 	FileStatus[] fsList = hdfs.getFileSystemList(path);
			if (backupModel.getMode() == RestConstants.DEFAULT_CONNECT_TYPE_FTP) {
				ftp = new FTPUtil(backupModel.getHost(),backupModel.getPort(),backupModel.getUsername(),backupModel.getPassword());	   
				
				if (ftp.connect()) {
					 for (FileStatus f : fsList) {
				    	  inStream = hdfs.getInputStream(f.getPath());
				    	  int namePos = f.getPath().toString().lastIndexOf(CommonConstants.DEFAULT_SLASH_SPLIT);
			              ftp.upload(inStream, backupModel.getDst(), backupModel.getFileName(), f.getPath().toString().substring(namePos + 1));
			              ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "90");
				      }	
					 inStream.close();
					 ftp.disconnect();
					 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "100");
					 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_SUCCESS);
				 }
				 else {
					 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_FTP);
					 throw new Exception("Failed to backup table because ftp can not be connected!");
				 }
			}
			else if (backupModel.getMode() == RestConstants.DEFAULT_CONNECT_TYPE_SFTP) {
				throw new Exception("Failed to backup table because sftp connect type has not been supported!");
			}
			else {
				throw new Exception("Failed to backup table because invalid connect type!");
			} 
		} catch (OtsException e) {
			e.getStackTrace();
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_HDFS);
		    throw e;
		}  catch (Exception e) {
			e.getStackTrace();
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_FTP);
		    throw e;
		} finally {
			if (inStream != null) {
				inStream.close();
			}
			if (ftp != null) {
				ftp.disconnect();
			}
			if (hdfs != null ) {
				hdfs.deleteFile(path);
			}
		}
	}
	
	public void restore(TableBackupModel backupModel, String path) throws Exception {
		HDFSUtil hdfs = null;
		InputStream inStream = null; 
		FTPUtil ftp = null;
		try {
			rediskeyTablename = TableService.getRedisKeyTableName(table.getTenantid(), table.getId(), table.getName());
			hdfs = new HDFSUtil();
    		if (model.getMode() == RestConstants.DEFAULT_CONNECT_TYPE_FTP) {
    			ftp = new FTPUtil(model.getHost(),model.getPort(),model.getUsername(),model.getPassword());	   
    			if (ftp.connect()) {
					 FTPFile[] fileList = ftp.getFileList(model.getDst());
					 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "0");
					 for (FTPFile f : fileList) {
						 if (!f.isFile()) {
							 continue;
						 }
						 inStream = ftp.download(model.getDst(), f.getName());
						 hdfs.putInputStream(inStream, path, f.getName());
						 ftp.completePendingCommand();
						 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "10");
					 }
					 inStream.close();
    				 ftp.disconnect();
    				
    				 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "20"); 
    		    	
    		    	int isJobSuccessful = hdfs.importTable(tablename, path);
    		    	if (isJobSuccessful != 0) {
    		    		LOG.error("Import table " + tablename + " mapreduce job error, result: " + isJobSuccessful);
    					throw new OtsException(RestErrorCode.EC_OTS_TABLE_BACKUP_HDFS_JOB_ERROR, "Failed to restore table because hdfs error!");
    				}
    		    	ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_PROGRESS, "100");
    		    	ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_SUCCESS);
    			 }
    			 else {
    				 ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_FTP);
    				 throw new Exception("Failed to restore table because ftp can not be connected!");
    			 }
    		}
    		else if (model.getMode() == RestConstants.DEFAULT_CONNECT_TYPE_SFTP) {
    			throw new Exception("Failed to restore table because sftp connect type has not been supported!");
    		}
    		else {
    			throw new Exception("Failed to restore table because invalid connect type!");
    		}       	
		} catch (OtsException e) {
			e.getStackTrace();
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_HDFS);
		    throw e;
		} catch(Exception e) {
			e.getStackTrace();
			ConfigUtil.getInstance().getRedisUtil().setHSet(rediskeyTablename, RestConstants.DEFAULT_BACKUP_RESULT, RestConstants.DEFAULT_BACKUP_RESULT_FAIL_FTP);
		    throw e;
		} finally {
			if (inStream != null) {
				inStream.close();
			}
			if (ftp != null) {
				ftp.disconnect();
			}
			if (hdfs != null ) {
				hdfs.deleteFile(path);
			}
		}
	}
}
