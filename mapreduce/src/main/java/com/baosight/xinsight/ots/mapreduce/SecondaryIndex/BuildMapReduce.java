package com.baosight.xinsight.ots.mapreduce.SecondaryIndex;

import java.io.IOException;
import java.util.List;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.common.util.SecondaryIndexUtil;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.ots.mapreduce.OtsMapreduce;
import com.baosight.xinsight.yarn.YarnTagGenerator;

public class BuildMapReduce implements OtsMapreduce{	
	private static final String OTS_INDEX_NAME = "ots.indexname";
	
	public static class Map extends TableMapper<ImmutableBytesWritable, Put> { 
		private static final Log LOG = LogFactory.getLog(Map.class);
		private SecondaryIndexInfo index = null;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String tableName = conf.get(TableInputFormat.INPUT_TABLE);
			String indexName = conf.get(OTS_INDEX_NAME);
			try {
				index = getIndexInfo(conf, tableName, indexName);
			} catch (OtsException e) {
				throw new IOException(e.getMessage());
			}
			if(index == null) {
				throw new IOException("index(" + index + ") no exist ");
			}
		}
		
		public void map(ImmutableBytesWritable row, Result result, Context context) throws IOException, InterruptedException {
			try {
				byte[] rowKey = row.get();
				byte[] newRowKey = new byte[index.getKeyLength() + rowKey.length];
				int offset = 0;
				byte[] realValue = null;
				
				for(SecondaryIndexColumn column:index.getColumns()) {
				  List<Cell> cells = result.getColumnCells(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), Bytes.toBytes(column.getName()));
				  int lack = column.getMaxLen();
				  if(cells.size() > 0) {
					  byte[] cellValue = CellUtil.cloneValue(cells.get(0));
					  LOG.info("Cell value:" + Bytes.toStringBinary(cellValue));
					  switch(column.getType())
					  {
					  case string:
					  case binary:	
						  if(cellValue.length > column.getMaxLen()) {
							  System.arraycopy(cellValue, 0, newRowKey, offset, column.getMaxLen());
							  offset += column.getMaxLen();
							  lack -= column.getMaxLen();
						  }
						  else {
							  System.arraycopy(cellValue, 0, newRowKey, offset, cellValue.length);
							  offset += cellValue.length;	
							  lack -= cellValue.length;
						  }						  
						  break;
					  case int32:
						  realValue = Bytes.toBytes(Integer.parseInt(Bytes.toString(cellValue)));
						  System.arraycopy(realValue, 0, newRowKey, offset, column.getMaxLen());
						  offset += column.getMaxLen();
						  lack -= column.getMaxLen();
						  break;
					  case float32:
						  realValue = Bytes.toBytes(Float.parseFloat(Bytes.toString(cellValue)));
						  System.arraycopy(realValue, 0, newRowKey, offset, column.getMaxLen());
						  offset += column.getMaxLen();
						  lack -= column.getMaxLen();
						  break;
					  case float64:
						  realValue = Bytes.toBytes(Double.parseDouble(Bytes.toString(cellValue)));
						  System.arraycopy(realValue, 0, newRowKey, offset, column.getMaxLen());
						  offset += column.getMaxLen();
						  lack -= column.getMaxLen();					  
						  break;
					  case int64:
						  realValue = Bytes.toBytes(Long.parseLong(Bytes.toString(cellValue)));
						  System.arraycopy(realValue, 0, newRowKey, offset, column.getMaxLen());
						  offset += column.getMaxLen();
						  lack -= column.getMaxLen();
						  break;
					  default:
						  break;
					  }
				  }
	  				  
				  for(int j = 0; j < lack; ++j) {
					  newRowKey[offset + j] = 0;
				  }
				  
				  offset+=lack;
			   }
				
				System.arraycopy(rowKey, 0, newRowKey, offset, rowKey.length);
				Put put = new Put(newRowKey);
				put.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), Bytes.toBytes("v"), Bytes.toBytes("v"));
				context.write(new ImmutableBytesWritable(newRowKey), put);
				
			} catch (OtsException e) {
				throw new IOException(e);
			}			
	    } 
	}
	
	private static SecondaryIndexInfo getIndexInfo(Configuration conf, String tableName, String indexName) throws OtsException, IOException {
		Admin admin = null;
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
			String strIndexes = tableDescriptor.getValue(OtsConstants.OTS_INDEXES);			
			if(null == strIndexes) {
				return null;
			}
	    	List<SecondaryIndexInfo> indexes = SecondaryIndexUtil.parseIndexes(strIndexes);
	    	for(SecondaryIndexInfo index:indexes) {
	    		if(index.getName().equals(indexName)) {
	    			return index;
	    		}
	    	}
		} catch (MasterNotRunningException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_FAILED_GET_INDEX_INFO, 
					"Failed to get index info! " + e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_FAILED_GET_INDEX_INFO, 
					"Failed to get index info! " + e.getMessage());
		} catch (IOException e) {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_FAILED_GET_INDEX_INFO, 
					"Failed to get index info! " + e.getMessage());
		} finally{
			if(null != admin) {
				admin.close();
			}
		}
    	
    	return null;
	}

	public void run(String[] args) throws Exception {
		ArgumentParser parser = ArgumentParsers.newArgumentParser("BuildMapReduce").defaultHelp(true).description("Build secondary index.");
        parser.addArgument("-zk").help("property zk address");
        parser.addArgument("-table").help("property table name");
        parser.addArgument("-tid").help("property table id");
        parser.addArgument("-index").help("property index name");
        parser.addArgument("-iid").help("property index id");
        
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
            if(null == ns) {
            	System.out.println("lack arguments!");
            	parser.printHelp();
            	System.exit(1);
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        
        String table = ns.getString("table");
        String tid = ns.getString("tid");
        String index = ns.getString("index");
        String iid = ns.getString("iid");
        String zk = ns.getString("zk");
        
        if (null == table || null == index || null == zk || null == tid || null == iid) {
        	System.out.println("invalid arguments");
        	parser.printHelp();
        	System.exit(1);
        }
	     
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", zk); 
		conf.set(TableOutputFormat.OUTPUT_TABLE, SecondaryIndexUtil.getIndexTableName(table, index)); 
		conf.set(TableInputFormat.INPUT_TABLE, table);
		conf.set(OTS_INDEX_NAME, index);
		String yarTagName = YarnTagGenerator.GenSecIndexBuildMapreduceTag(Long.parseLong(tid), table, Long.parseLong(iid), index);
		conf.set("mapreduce.job.tags", yarTagName);
		    
		//Job job = new Job(conf, yarTagName); 
		Job job = Job.getInstance(conf, yarTagName);
		job.setJarByClass(BuildMapReduce.class);    
	
		job.setInputFormatClass(TableInputFormat.class); 
		job.setOutputFormatClass(TableOutputFormat.class); 
		    
		Scan scan = new Scan();
		SecondaryIndexInfo indexInfo= getIndexInfo(conf, table, index);
		List<SecondaryIndexColumn> columns = indexInfo.getColumns();
		System.out.println("Columns size:" + columns.size());
		for(SecondaryIndexColumn column:columns) {
			scan.addColumn(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), Bytes.toBytes(column.getName()));
		}    
	    TableMapReduceUtil.initTableMapperJob(table, scan, BuildMapReduce.Map.class, ImmutableBytesWritable.class, Put.class, job);
	    job.waitForCompletion(true);
	} 
}
