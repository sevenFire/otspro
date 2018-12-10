package com.baosight.xinsight.ots.client.index;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.w3c.dom.Document;  
import org.w3c.dom.Element;  
import org.w3c.dom.Node; 
import org.xml.sax.SAXException;  

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.client.exception.IndexException;
import com.baosight.xinsight.ots.client.util.SolrFileUtil;
import com.baosight.xinsight.ots.common.index.Column;
import com.baosight.xinsight.ots.common.index.IndexInfo;

public class IndexConfigurator {
	private static final Logger LOG = Logger.getLogger(IndexConfigurator.class);

	private static String zkHost;
	private static String otsHome;
	private static int zkTimeout = 3000;
	private static String solrServerAddr; 
	private static String[] indexerServerAddr;
	private static final String TMPBASE_STRING = "/tmp/ots";

	public static void Init(String zkHost, int timeout, String otsHome, String indexerServer) {
		IndexConfigurator.zkHost = zkHost;
		IndexConfigurator.otsHome = otsHome;
		IndexConfigurator.zkTimeout = timeout;
		IndexConfigurator.solrServerAddr = zkHost + "/solr";
		IndexConfigurator.indexerServerAddr = indexerServer.split(",");
	}
	
	public static void Release() {		
	}

	private String getIndexerServerAddrRandom() {
		return indexerServerAddr[new Random().nextInt(indexerServerAddr.length)]+":11060";
	}
	
	private void CheckFields(List<Column> columns) throws IndexException {
		for(Column column:columns) {
			if(column.getName().equals("hash_key") || column.getName().equals("range_key"))
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NAME, "Invalid column name, column name can not be 'hash_key' and 'range_key' !");
			if(column.getName().equals("_version_"))
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NAME, "Invalid column name, column name can not be '_version_'!");
			
			if(!column.getType().equals("int")&&!column.getType().equals("float")&&!column.getType().equals("double")
					&&!column.getType().equals("long")&&!column.getType().equals("string")&&!column.getType().equals("boolean")
					&&!column.getType().equals("binary")&&!column.getType().equals("location")&&!column.getType().equals("location_rpt"))
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_TYPE, "Invalid column type, type must be int, long, float, double, string, binary, boolean!");
		}
		
		if(columns.size() <= 0)
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_COLUMN_NUM, "0 column, you need to have at least one column!");
	}
	
	private boolean exist(ZooKeeper zk, String name) {
		try {
			Stat stat = zk.exists(OtsConstants.ZK_COLLECTION_PATH + name, false);
			if(null != stat)
				return true;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	private byte[] GenerateMophlinesMapper(String indexRootPath, List<Column> columnList) throws IndexException	{
		String prefix = "morphlines : [\n  {\n    id : morphline1\n    importCommands : [\"org.kitesdk.morphline.**\", \"com.ngdata.**\"]\n\n    commands : [\n    {\n        extractHBaseCells {\n          mappings : ";
		List<String> mappingStrings = new ArrayList<String>();
		
		for(Column column: columnList) {
			String mapString = "{\n              inputColumn : \"f:" + column.getName() + "\"\n              outputField : \"";
			mapString = mapString + column.getName() + "\"\n              type : string\n               source : value\n            }";				
			mappingStrings.add(mapString);
		}
		String end = "\n        }\n      }\n    ]\n  }\n]";

		return Bytes.toBytes(prefix + mappingStrings.toString() + end);
	}
	
	private byte[] GenerateMorphlinesFiles(String indexRootPath, String tenantid, String tableName) throws IndexException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();  
		DocumentBuilder builder;
		try {
			builder = dbf.newDocumentBuilder();
			Document doc = builder.newDocument();
	  
			Element indexerElement = doc.createElement("indexer");
			indexerElement.setAttribute("table", tenantid + ":" +tableName);
			indexerElement.setAttribute("row-field", "id");
			indexerElement.setAttribute("unique-key-formatter", "com.ngdata.hbaseindexer.uniquekey.HexUniqueKeyFormatter");
			indexerElement.setAttribute("mapper", "com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper");
			Element paramElement = doc.createElement("param");
			paramElement.setAttribute("name", "morphlineFile");
			paramElement.setAttribute("value", indexRootPath +File.separator + OtsConstants.CONFIG_FILE_MORPHLINES);
			indexerElement.appendChild(paramElement);
			doc.appendChild(indexerElement);
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty("encoding","utf-8");
			ByteArrayOutputStream   bos   =   new   ByteArrayOutputStream();
			transformer.transform(new DOMSource(doc), new StreamResult(bos));
			return bos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_GEN_MOLPHINE_FILE, "Failed to generate morphline file!\n" + e.getMessage());
		}
	}
	
	private String GetTemplatePath() throws IndexException {
		String strPath = "/apps/xinsight/ots/template/index/config";
		File file = new File(strPath);
		if (file.exists()) {
			return strPath;
		}

		String env_home = System.getenv("XINSIGHT_HOME"); // "/apps/xinsight"
		if (env_home != null) {
			if (env_home.endsWith("/")) {
				env_home = env_home.substring(0, env_home.lastIndexOf("/"));
			}
			strPath = env_home + "/ots/template/index/config";
			file = new File(strPath);
			if (file.exists()) {
				return strPath;
			}
		}
		
		URL project = getClass().getResource("/template/index/config"); 
		if (project != null) {		
			try {
				file = new File(project.toURI());
				if (file.exists()) {
					return file.getAbsolutePath();
				}
			} catch (URISyntaxException e) {
				e.printStackTrace();
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_NO_EXIST_CONFIG_TEMPLATE, "Index config template no exist!\n" + e.getMessage());
			}
		}
		
		throw new IndexException(OtsErrorCode.EC_OTS_INDEX_NO_EXIST_CONFIG_TEMPLATE, "Index config template no exist!");
	}
	
	private void GenerateIndexConfig(String tenantid, String tableName, String collectionName, List<Column> columnList) throws IndexException {
		
		String pathString = TMPBASE_STRING+ File.separator + tenantid + File.separator + tableName + File.separator + collectionName;		
		File file = new File(pathString);
		if(!file.exists())
			file.mkdirs();
		file.setWritable(true, false);
		String templateUrl = GetTemplatePath();
		if(null != templateUrl)	{
			try {
				File file2 = new File(templateUrl);
				SolrFileUtil.copyDir(file2.getAbsolutePath(), pathString + File.separator);
				ModifySchema(pathString, columnList);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_COPY_CONFIG_TEMPLATE, "Copy config template file failed!\n" + e.getMessage());
			}
		}
		else {
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_NO_EXIST_CONFIG_TEMPLATE, "Index config template no exist!");
		}
	}
	
	private void ModifySchema(String indexRootPath, List<Column> columnList) throws IndexException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();  
		DocumentBuilder builder;
		
		try {
			builder = dbf.newDocumentBuilder();
			File file = new File(indexRootPath + File.separator + "schema.xml");  
			InputStream in = new FileInputStream(file);  
			Document doc = builder.parse(in);  
	 
			Element root = doc.getDocumentElement(); 
			Node fieldsNode = root.getElementsByTagName("fields").item(0);
			for(int i = 0; i < columnList.size(); ++i)	{
				if(!columnList.get(i).getName().equals("id")) {
					Element fieldNode = doc.createElement("field");
					fieldNode.setAttribute("name", columnList.get(i).getName());
					fieldNode.setAttribute("type", columnList.get(i).getType());
					if(columnList.get(i).getIndexed()) {
						fieldNode.setAttribute("indexed", "true");
					}
					else {
						fieldNode.setAttribute("indexed", "false");
					}
					if(columnList.get(i).getStored()) {
						fieldNode.setAttribute("stored", "true");
					}
					else {
						fieldNode.setAttribute("stored", "false");
					}
					
					fieldNode.setAttribute("required", "false");
					fieldNode.setAttribute("multiValued", "false");
					fieldNode.setAttribute("termVectors", "false");
					fieldNode.setAttribute("termPositions", "false");
					fieldNode.setAttribute("termOffsets", "false");
					fieldsNode.appendChild(fieldNode);
				}
			}
			
			TransformerFactory transFactory = TransformerFactory.newInstance();  
			Transformer transFormer = transFactory.newTransformer();  
			DOMSource domSource = new DOMSource(doc);  
			FileOutputStream out = new FileOutputStream(file);           
			StreamResult xmlResult = new StreamResult(out);  
			transFormer.transform(domSource, xmlResult);   

		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "parse Schema.xml failed!\n" + e.getMessage());
		} catch (SAXException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_PARSE_COLLECTION_SCHEMA, "parse Schema.xml failed!\n" + e.getMessage());
		} catch (TransformerException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_MODIFY_COLLECTION_SCHEMA, "parse Schema.xml failed!\n" + e.getMessage());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_NO_EXIST_COLLECTION_SCHEMA, "Schema.xml no exist!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_MODIFY_COLLECTION_SCHEMA, "parse Schema.xml failed!\n" + e.getMessage());
		}
	}

	private void DelConfigFromZookeeper(ZooKeeper zk, String path) throws IndexException {
		LOG.debug("Delete Config From Zookeeper:"  + path);

		try {
			Stat stat = zk.exists(path, false);
			if(null != stat) {
				List<String> childrens = zk.getChildren(path, false);
				if(childrens.size() > 0) {
					for(int i = 0; i < childrens.size(); ++i) {
						DelConfigFromZookeeper(zk, path + File.separator + childrens.get(i));
					}			
				}

				zk.delete(path, -1);			
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DEL_CONFIG_FROM_ZK, "Delete config from zookeeper failed!\n" + e.getMessage());
		} catch (KeeperException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DEL_CONFIG_FROM_ZK, "Delete config from zookeeper failed!\n" + e.getMessage());
		}
	}
	
	private void CreateSolrCollection(String name, Integer shardNum, Integer replicationNum, Integer maxShardNumPerNode) throws IndexException { 
		CloudSolrServer server = null;
		LOG.debug("Create Solr Collection:"  + name);

		try {
			server = new CloudSolrServer(solrServerAddr);  
			CollectionAdminRequest.createCollection(name, shardNum, replicationNum, maxShardNumPerNode, null, name, null, true, server);
			//hm!! default shardNum=3,replicationNum=1,maxShardNumPerNode=3, autoAddReplicas=true
		} catch (SolrServerException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_ACCESS_SOLR_CLOUD_SERVER, "Failed to access solr cloud server!\n" + e.getMessage());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_SOLR_SERVER_ADDR, "Solr cloud server address is invalid!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE_COLLECTION, "Failed to create solr collection!\n" + e.getMessage());
		}finally {
		   if (null != server){
			   server.shutdown();
		   }
		}
	}
	
	private void DeleteSolrCollection(String name) throws IndexException {
		CloudSolrServer server = null;
		LOG.debug("Delete Solr Collection:"  + name);

		try {
			server = new CloudSolrServer(solrServerAddr);
			CollectionAdminRequest.deleteCollection(name, server);
		} catch (SolrServerException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_ACCESS_SOLR_CLOUD_SERVER, "Failed to access solr cloud server!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DELETE_COLLECTION, "Failed to delete solr collection!\n" + e.getMessage());
		} catch(RemoteSolrException e){
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DELETE_COLLECTION, "Failed to delete solr collection!\n" + e.getMessage());
		}finally{
			if (null != server){
				server.shutdown();
			}
		}
	}
	
	private void CreateHBaseIndex(String indexRootPath, String tenantid, String tableName, String collectionName, IndexInfo indexInfo) throws IndexException {
		HttpClient client = new DefaultHttpClient();
		HttpPost request = new HttpPost("http://" + getIndexerServerAddrRandom() + "/indexer");
		
		try {
	        ObjectNode node = JsonNodeFactory.instance.objectNode();

	        node.put("name", collectionName);
	        node.put("lifecycleState", "ACTIVE");
	        node.put("batchIndexingState", "BUILD_REQUESTED");
			if(OtsConstants.OTS_INDEX_PATTERN_ONLINE == (int)indexInfo.getPattern()) {
				node.put("incrementalIndexingState", "SUBSCRIBE_AND_CONSUME");
			}
			else {
				node.put("incrementalIndexingState", "SUBSCRIBE_DO_NOT_CONSUME");
			}
			
	        node.put("occVersion", 1);
	        node.put("indexerComponentFactory", "com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory");
	        node.put("configuration", Base64.encodeBytes(GenerateMorphlinesFiles(indexRootPath, tenantid, tableName), Base64.GZIP));
	        node.put("morphlineFilePath", indexRootPath + File.separator + OtsConstants.CONFIG_FILE_MORPHLINES);
	        node.put("morphlineFile", Base64.encodeBytes(GenerateMophlinesMapper(indexRootPath, indexInfo.getColumns())));
	        node.put("connectionType", "solr");
	        ObjectNode paramsNode = node.putObject("connectionParams");
	        paramsNode.put("solr.collection", collectionName);
	        paramsNode.put("solr.zk", solrServerAddr);
            ArrayNode arrayNode = node.putArray("batchIndexCliArguments");
            if(null != indexInfo.getStartKey()) {
            	arrayNode.add("--hbase-start-row");
            	arrayNode.add(indexInfo.getStartKey());
            }
            
            if(null != indexInfo.getEndKey()) {
            	arrayNode.add("--hbase-end-row");
            	arrayNode.add(indexInfo.getEndKey());
            }
            node.put("batchIndexCliArguments", arrayNode);
            
            StringEntity entity = new StringEntity(node.toString(), ContentType.create(ContentType.APPLICATION_JSON.getMimeType(), OtsConstants.DEFAULT_ENCODING));
			request.setEntity(entity);
			LOG.debug("Create Solr index:"  + request.toString());

			client.execute(request);			
		} catch (ClientProtocolException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE_HBASE_INDEXER, "Failed to execute create hbase indexer command!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE_HBASE_INDEXER, "Failed to execute create hbase indexer command!\n" + e.getMessage());
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}
	
	public static class Field {
		public String name;
		public String type;
		boolean bKey;
		
		Field(String name, String type, boolean bKey) {
			this.name = name;
			this.type = type;
			this.bKey = bKey;
		}
	}
	
	private void UploadFileToZookeeper(ZooKeeper zk, String zooPath, String filePath) throws IOException {
		FileInputStream inputStream = new FileInputStream(filePath);
		int fileSize = inputStream.available();
		byte[] data = new byte[fileSize];
		inputStream.read(data);
		inputStream.close();	
		try {
			zk.create(zooPath, data, Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	private void UploadDirToZookeeper(ZooKeeper zk, String zooPath, String dirPath) throws IOException {
		try {
			if(null == zk.exists(zooPath, false)) {
				zk.create(zooPath, null, Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);			
			}

			File file = new File(dirPath); 
			File[] files = file.listFiles();
			for(int i = 0; i< files.length; ++i) {
				if(files[i].isFile()) {
					UploadFileToZookeeper(zk, zooPath +'/'+ files[i].getName(), files[i].getAbsolutePath());
				}
				
				if(files[i].isDirectory()) {
					UploadDirToZookeeper(zk, zooPath +'/'+ files[i].getName(), files[i].getAbsolutePath());
				}
			}			
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}	
	
	public void CreateIndex(String tenantid, String tableName, String indexName,  IndexInfo indexInfo) throws IndexException {
		String collectionName = tenantid + OtsConstants.COLLECTION_NAME_SEPRATOR + tableName + OtsConstants.COLLECTION_NAME_SEPRATOR + indexName;
		LOG.debug("Create index:"  + collectionName);

		ZooKeeper zk = null;
		try {
			zk = new ZooKeeper(zkHost, zkTimeout, null);
			if(exist(zk, collectionName)) {
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_ALREADY_EXIST, "Index already exist!");
			}
			CheckFields(indexInfo.getColumns());
		
			String indexRootPath = otsHome + File.separator + tenantid + File.separator + tableName + File.separator + collectionName;
			try {
				// generate index config file
				GenerateIndexConfig(tenantid, tableName, collectionName, indexInfo.getColumns());
			} catch (IndexException e) {
				e.printStackTrace();
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw e;
			} catch (Exception e) {
				e.printStackTrace();
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE, "Failed to create index!\n" + e.getMessage());
			}
			
			try {
				if(null == zk.exists("/solr/configs", false)) {
					zk.create("/solr/configs", null, Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);			
				}
				// upload config file to zookeeper
				UploadDirToZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName, 
						TMPBASE_STRING + File.separator + tenantid + File.separator + tableName + File.separator + collectionName);
			
			} catch (Exception e) {
				e.printStackTrace();
				
				DelConfigFromZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName);
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_UPLOAD_CONFIG_TO_ZK, "Failed to upload config to zookeeper.\n" + e.getMessage());
			}
			
			try {
				// create collection
				CreateSolrCollection(collectionName, indexInfo.getShardNum(), indexInfo.getReplicationNum(), indexInfo.getMaxShardNumPerNode());
			} catch (IndexException e) {
				e.printStackTrace();
				
				DelConfigFromZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName);
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw e;
			} catch (Exception e) {
				e.printStackTrace();
				
				DelConfigFromZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName);
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE, "Failed to create index!\n" + e.getMessage());
			}
			
			try {
				CreateHBaseIndex(indexRootPath, tenantid, tableName, collectionName, indexInfo);	
			} catch (IndexException e) {
				e.printStackTrace();
				
				DeleteSolrCollection(collectionName);
				DelConfigFromZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName);
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw e;
			}catch (Exception e) {
				e.printStackTrace();
				
				DeleteSolrCollection(collectionName);
				DelConfigFromZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName);
				DeleteIndexConfigFile(tenantid, tableName, collectionName);
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE, "Failed to create index!\n" + e.getMessage());
			}			

			DeleteIndexConfigFile(tenantid, tableName, collectionName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CREATE, "Failed to create index!\n" + e.getMessage());
		}finally{
			if (null != zk){
				try {
					zk.close();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void BuildIndex(String tenantid, String tableName, String indexName) throws IndexException {	
		String collectionName = tenantid + OtsConstants.COLLECTION_NAME_SEPRATOR + tableName + OtsConstants.COLLECTION_NAME_SEPRATOR + indexName;	
		LOG.debug("Build index:"  + collectionName);
		boolean bSuccess = true;
		IndexException reasonException = null;
		
		ZooKeeper zk = null;
		try {
			try {
				zk = new ZooKeeper(zkHost, zkTimeout, null);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CONN_TO_ZK, "Failed to connect to zookeeper!\n" + e.getMessage());
			}

			CollectionLock collectionLock = new CollectionLock();
			try {
				boolean bLock = collectionLock.tryLock(zk, collectionName, InetAddress.getLocalHost().getHostName());
				if (!bLock) {
					throw new IndexException(OtsErrorCode.EC_OTS_INDEX_BUILDING, "Failed to lock index, it's building!");
				}
			} catch (UnknownHostException e2) {
				e2.printStackTrace();
			} catch (KeeperException e2) {
				e2.printStackTrace();
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}
			
			HttpClient client = new DefaultHttpClient();
			HttpPut request = new HttpPut("http://"+getIndexerServerAddrRandom() +"/indexer/build/" + collectionName);
			try {
				client.execute(request);
			} catch (ClientProtocolException e) {
				//e.printStackTrace();
				bSuccess = false;
				reasonException = new IndexException(OtsErrorCode.EC_OTS_INDEX_BUILDING, "Failed to build index!\n" + e.getMessage());
			} catch (IOException e) {
				//e.printStackTrace();
				bSuccess = false;
				reasonException = new IndexException(OtsErrorCode.EC_OTS_INDEX_BUILDING, "Failed to build index!\n" + e.getMessage());
			} finally {
				if (request != null) {
					request.releaseConnection();
				}
			}
			
			try {
				collectionLock.unLock(zk, collectionName, InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (KeeperException e1) {
				e1.printStackTrace();
			}

			if (!bSuccess && null != reasonException) {
				throw reasonException;
			}			
			
		} finally {
			try {
				if(zk != null){
					zk.close();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void ClearIndex(String tenantid, String tableName, String indexName) throws IndexException {
		CloudSolrServer server = null;
		try {			
			String collectionName = tenantid + OtsConstants.COLLECTION_NAME_SEPRATOR + tableName + OtsConstants.COLLECTION_NAME_SEPRATOR + indexName;
			LOG.debug("Clear index:"  + collectionName);
			
			server = new CloudSolrServer(solrServerAddr);
			server.setDefaultCollection(collectionName);			
			server.deleteByQuery("*:*");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_INVALID_SOLR_SERVER_ADDR, "Failed to clear index!\n" + e.getMessage());
		} catch (SolrServerException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_ACCESS_SOLR_CLOUD_SERVER, "Failed to clear index!\n" + e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CLEAR, "Failed to clear index!\n" + e.getMessage());
		}finally{
			if (null != server){
				server.shutdown();
			}
		}
	}
	
	public static void DeleteIndexConfigFile(String tenantid, String tableName, String collectionName) throws IndexException { 
		String path = TMPBASE_STRING + File.separator + tenantid + File.separator + tableName + File.separator + collectionName;
		LOG.debug("Delete index config file:"  + path);

		try {
			SolrFileUtil.delDir(path);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DELETE_CONFIG_FILE, "Failed to delete index config files!\n" + e.getMessage());
		}
	}
	
	public void DeleteIndex(String tenantid, String tableName, String indexName) throws IndexException {
		boolean bSuccess = true;
		IndexException reasonException = null;
		String collectionName = tenantid + OtsConstants.COLLECTION_NAME_SEPRATOR + tableName + OtsConstants.COLLECTION_NAME_SEPRATOR + indexName;
		LOG.debug("Delete index:"  + collectionName);

		ZooKeeper zk = null;
		try {
			try {
				zk = new ZooKeeper(zkHost, zkTimeout, null);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_CONN_TO_ZK, "Failed to connect to zookeeper!\n" + e.getMessage());
			}

			CollectionLock collectionLock = new CollectionLock();
			try {
				boolean bLock = collectionLock.tryLock(zk, collectionName, InetAddress.getLocalHost().getHostName());
				if (!bLock) {
					throw new IndexException(OtsErrorCode.EC_OTS_INDEX_BUILDING, "Index is building!");
				}
			} catch (UnknownHostException e2) {
				e2.printStackTrace();
			} catch (KeeperException e2) {
				e2.printStackTrace();
			} catch (InterruptedException e2) {
				e2.printStackTrace();
			}

			HttpClient client = new DefaultHttpClient();
			HttpDelete request = new HttpDelete("http://" + getIndexerServerAddrRandom() + "/indexer/" + collectionName);
			try {
				client.execute(request);
			} catch (ClientProtocolException e2) {
				bSuccess = false;
				reasonException = new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DELETE, e2.getMessage());
			} catch (IOException e2) {
				bSuccess = false;
				reasonException = new IndexException(OtsErrorCode.EC_OTS_INDEX_FAILED_DELETE, e2.getMessage());
			} finally {
				if (request != null) {
					request.releaseConnection();
				}
			}

			try {
				DeleteSolrCollection(collectionName);
			} catch (IndexException e) {
				if (exist(zk, collectionName)) {
					bSuccess = false;
					reasonException = new IndexException(e.getErrorCode(), "Failed to delete index!\n" + e.getMessage());
				}
			}

			try {
				DelConfigFromZookeeper(zk, OtsConstants.ZK_SOLR_CONFIG_PATH	+ collectionName);
			} catch (IndexException e) {
				if (exist(zk, OtsConstants.ZK_SOLR_CONFIG_PATH + collectionName)) {
					bSuccess = false;
					reasonException = new IndexException(e.getErrorCode(), "Failed to delete index!\n" + e.getMessage());
				}
			}

			try {
				collectionLock.unLock(zk, collectionName, InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (KeeperException e1) {
				e1.printStackTrace();
			}

			if (!bSuccess && null != reasonException) {
				throw reasonException;
			}
		} finally {
			try {
				if(zk != null){
					zk.close();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
