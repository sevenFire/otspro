package com.baosight.xinsight.ots.client.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.baosight.xinsight.ots.OtsConstants;

public class SolrFileUtil {
	/**
	 * 
	 * @param sourceDirPath
	 * @param targetDirPath
	 * @throws IOException
	 */
	public static void copyDir(String sourceDirPath, String targetDirPath) throws IOException {
		// 创建目标文件夹
		File targetDirFile = new File(targetDirPath);
		targetDirFile.setWritable(true, false);
		targetDirFile.mkdirs();
		
		// 获取源文件夹当前下的文件或目录
		File[] file = (new File(sourceDirPath)).listFiles();
		for (int i = 0; i < file.length; i++) {
			if (file[i].isFile()) {
				SolrFileUtil.copyFile(file[i], new File(targetDirPath + File.separator + file[i].getName()));
			}
			if (file[i].isDirectory()) {
				// 复制目录
				String sourceDir = sourceDirPath + File.separator + file[i].getName();
				String targetDir = targetDirPath + File.separator + file[i].getName();
				SolrFileUtil.copyDir(sourceDir, targetDir);
			}
		}
	}
    
    public static void copyFile(File sourceFile, File targetFile) throws IOException {
        BufferedInputStream inBuff = null;
        BufferedOutputStream outBuff = null;
        try {
            // 新建文件输入流并对它进行缓冲
            inBuff = new BufferedInputStream(new FileInputStream(sourceFile));

            // 新建文件输出流并对它进行缓冲
            outBuff = new BufferedOutputStream(new FileOutputStream(targetFile));

            // 缓冲数组
            byte[] b = new byte[1024 * 5];
            int len;
            while ((len = inBuff.read(b)) != -1) {
                outBuff.write(b, 0, len);
            }
            // 刷新此缓冲的输出流
            outBuff.flush();
        } finally {
            // 关闭流
            if (inBuff != null)
                inBuff.close();
            if (outBuff != null)
                outBuff.close();
        }
    }

	public static void delDir(String filepath) throws IOException {   
		File f = new File(filepath);// 定义文件路径    
		if(f.isDirectory()) {  
			File delFile[] = f.listFiles(); 
			if (delFile.length > 0) {
				for (int j = 0; j < delFile.length; j++) {   
					if (delFile[j].isDirectory()) {   
						delDir(delFile[j].getAbsolutePath());// 递归调用del方法并取得子目录路径   
					}   
		            delFile[j].delete();// 删除文件   
				}   		                   
			} 
		}
		f.delete();
	} 
	
	//string[0]namesapce, string[1]tablename, string[2]collectionname
	public static String[] getIndexNameFromSolrCollectionPath(String solrCollectionPath) {
		String[] returns = new String[3];
		int startIndexOfCollectionName = "/solr/collections".length() + 1;
		int endIndexOfCollectionName = solrCollectionPath.indexOf('/', startIndexOfCollectionName);
		
		if(-1 == endIndexOfCollectionName) {
			String collectionName = solrCollectionPath.substring(startIndexOfCollectionName);
			int indexNameStartIndex = collectionName.lastIndexOf(OtsConstants.COLLECTION_NAME_SEPRATOR);
			returns[2] = collectionName;
			int tableNameStartIndex = collectionName.substring(0, indexNameStartIndex - 1).lastIndexOf(OtsConstants.COLLECTION_NAME_SEPRATOR);
			returns[1] = collectionName.substring(tableNameStartIndex+1, indexNameStartIndex);
			returns[0] = collectionName.substring(0, tableNameStartIndex);
			
			return returns;
		} else {
			return null;
		}
	}
}

