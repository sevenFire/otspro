package com.baosight.xinsight.ots.cfgsvr.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.ots.cfgsvr.common.RestErrorCode;

public class FTPUtil {

	private String host;   
    private int port;   
    private String username;   
    private String password;  
	private FTPClient ftp;
    
    public FTPUtil(String host, int port, String username, String password) {
		super();
		this.host = host;
		this.port = port;
		this.username = username.isEmpty()?"anonymous":username;
		this.password = password.isEmpty()?"anonymous":password;
		this.ftp = new FTPClient();
	}

	public boolean connect() throws IOException {  
    	boolean login = false;
		ftp.connect(host, port);
		login = ftp.login(username, password);
    	return login;
    }  

	/** 
     * Disconnect with server 
	 * @throws ConnException 
	 * @throws IOException 
     */  
    public void disconnect() throws IOException {  
		ftp.disconnect();
    }  
    
    /** 
     * upload all the files to the server 
     * @throws Exception 
     */  
    public void upload(InputStream input, String directory, String folder, String file) throws Exception {
    	String target = directory + CommonConstants.DEFAULT_SLASH_SPLIT + folder;
    		if (input == null) {
    			throw new Exception(file + ": input is null.");
    		}
			ftp.makeDirectory(target);
    		ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
    		ftp.setControlEncoding("UTF-8");
			boolean change = ftp.changeWorkingDirectory(target);
			if (!change) {
				throw new Exception("can not change working directory!");
			}
			ftp.storeFile(file, input);  
    }  
    
    public InputStream download(String directory, String filename) throws IOException {
		InputStream inStream = null;
    		ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
    		ftp.setControlEncoding("UTF-8");
    		ftp.changeWorkingDirectory(directory);
		    inStream = ftp.retrieveFileStream(filename);
    	return inStream;
    }  
    
    public FTPFile[] getFileList(String directory) throws IOException {
    	FTPFile[] fileList = null;
			fileList = ftp.listFiles(directory);
    	return fileList;
    }

	public boolean completePendingCommand() throws IOException {
		boolean flag = false;
			flag = ftp.completePendingCommand();
		return flag;
	}
	
	public long test(String directory) throws IOException {
		long errcode = 0L;
		ftp.connect(host, port);
		if (!ftp.login(username, password)) {
			errcode = RestErrorCode.EC_OTS_TABLE_BACKUP_FTP_CONNECT_FAILURE;
		} else {
			if (!ftp.changeWorkingDirectory(directory)) {
				errcode = RestErrorCode.EC_OTS_TABLE_BACKUP_FTP_FOLDER_NOT_EXIST;
			}
		}
		ftp.disconnect();
		return errcode;
	}
}
