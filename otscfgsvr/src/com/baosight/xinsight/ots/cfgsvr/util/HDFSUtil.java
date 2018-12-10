package com.baosight.xinsight.ots.cfgsvr.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.baosight.xinsight.common.CommonConstants;
import com.baosight.xinsight.config.ConfigConstants;
import com.baosight.xinsight.ots.rest.util.ConfigUtil;

public class HDFSUtil {
	private static final Logger LOG = Logger.getLogger(BackupTask.class);

	private Configuration conf;
	private FileSystem fs = null;
	private String hdfs_backup_path;
    private String hdfs_user;
	
	public HDFSUtil() throws IOException, InterruptedException {
		super();
		init();
	}
	
	private void init() throws IOException, InterruptedException {
		hdfs_user = ConfigUtil.getInstance().getValue(ConfigConstants.HDFS_WEB_USER);
		String hosts[] = ConfigUtil.getInstance().getValue(ConfigConstants.HDFS_SERVER).split(",");
		String hdfsNodes[] = new String[hosts.length];
		
		conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
	    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());	
	    
	    System.out.println("************" + conf.get("dfs.nameservices"));
	    
	    if(hosts.length <= 0) {
			throw new IOException("Invalid hdfs server format, too less option!");
		}
		for (int i=0; i<hosts.length; i++) {
			try {
				hdfsNodes[i] = String.format("hdfs://%s/user/%s/", hosts[i], hdfs_user);
				fs = FileSystem.get(URI.create(hdfsNodes[i]), conf, hdfs_user);  
				hdfs_backup_path = hdfsNodes[i] + "/backup/"; 
				
				boolean exists = fs.exists(new Path(hdfs_backup_path));
				if(!exists) {
					fs.mkdirs(new Path(hdfs_backup_path));
				}
			} catch (IOException | InterruptedException e) {
				if (i >= hosts.length - 1) {
					throw e;
				}
				continue;
			}
			break;
		}
	}
	
	public FileStatus[] getFileSystemList(String path) throws IOException {
		FileStatus[] list = null;
		try {
			list = fs.listStatus(new Path(hdfs_backup_path + path));
		} catch (IOException e) {
			throw e;
		}
		return list;
	}
	
	public InputStream getInputStream(Path path) throws IOException {
		InputStream input = null;
		try {
			input = fs.open(path);
		} catch (IOException e) {
			throw e;
		}
		return input;
	}
	
	public void putInputStream(InputStream inStream, String path, String filename) throws IOException {
		try {
			Path p = new Path(hdfs_backup_path + path + CommonConstants.DEFAULT_SLASH_SPLIT + filename);
			OutputStream outStream = fs.create(p);
			IOUtils.copyBytes(inStream, outStream, 4096, true);
		} catch (IOException e) {
			throw e;
		}
	}
	
	public boolean deleteFile(String path) throws IOException {
		Path p = new Path(hdfs_backup_path + path);
		boolean flag = false;
		flag = fs.delete(p, true);
		return flag;
	}

	public int importTable(String tablename, String path) throws Exception {
		int result = -1;
		 try {
			 String[] cmds = new String[]{"hbase", "org.apache.hadoop.hbase.mapreduce.Driver", "import", tablename, hdfs_backup_path + path};
			 Process ps = Runtime.getRuntime().exec(cmds);
			 result = ps.waitFor();
		} catch (Exception e) {
			LOG.warn("hdfs importTable error: " + e.getMessage());
			throw e;
		}
		 return result;
	}
	
	public int exportTable(String tablename, String path) throws Exception {
		int result = -1;
		 try {
			 String[] cmds = new String[]{"hbase", "org.apache.hadoop.hbase.mapreduce.Driver", "export", tablename, hdfs_backup_path + path};
			 Process ps = Runtime.getRuntime().exec(cmds);
			 result = ps.waitFor();
		} catch (Exception e) {
			LOG.error("hdfs exportTable error: " + e.getMessage());
			throw e;
		}
		 return result;
	}
}
