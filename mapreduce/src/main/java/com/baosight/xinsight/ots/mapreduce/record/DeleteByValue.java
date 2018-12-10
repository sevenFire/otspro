package com.baosight.xinsight.ots.mapreduce.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baosight.xinsight.ots.mapreduce.OtsMapreduce;

public class DeleteByValue implements OtsMapreduce {
	public static Configuration config;
	public final static String TABLE_NAME = "tableName";
	public final static String COLUMN_LIST = "column_list";
	public final static String COLUMN_CONNECTOR = "$_$_#_#";
	public final static String RANGE_START_SUFFIX = "_start";
	public final static String RANGE_END_SUFFIX = "_end";

	@SuppressWarnings("unused")
	private static Logger logger = LoggerFactory.getLogger(DeleteByValue.class);
	
	static {
		config = HBaseConfiguration.create();
	}

	static class HbaseDeleteMapper extends TableMapper<Text, LongWritable> {
		private Connection connection;
		private Table table;
		private List<Delete> deleteList = new ArrayList<Delete>();
		private String[] columnList;
		final private int BATCH_SIZE = 10000; /* Buffer size, tune it as desired */

		protected void setup(Context context) throws IOException, InterruptedException {
			connection = ConnectionFactory.createConnection(config);
			String tableName = context.getConfiguration().get(TABLE_NAME);
			table = connection.getTable(TableName.valueOf(tableName));
			String columns = context.getConfiguration().get(COLUMN_LIST);
			columnList = columns.split(COLUMN_CONNECTOR);
		}

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			Boolean bMatched = true;
			Map<String, Integer> recordColumns = new HashMap<String, Integer>();
			for (Cell cell : value.rawCells()) {
				String columnName = Bytes.toString(cell.getQualifierArray(),
						cell.getQualifierOffset(), cell.getQualifierLength());
				recordColumns.put(columnName, 0);
				String valueStart = context.getConfiguration().get(columnName + RANGE_START_SUFFIX);
				String valueEnd = context.getConfiguration().get(columnName + RANGE_END_SUFFIX);

				if (null != valueStart && !valueStart.equals("*")) {
					if (Bytes.compareTo(cell.getValueArray(),
							cell.getValueOffset(), cell.getValueLength(),
							valueStart.getBytes(), 0, valueStart.length()) < 0) {
						bMatched = false;
						break;
					}
				}
				if (null != valueEnd && !valueEnd.equals("*")) {
					if (Bytes.compareTo(cell.getValueArray(),
							cell.getValueOffset(), cell.getValueLength(),
							valueEnd.getBytes(), 0, valueEnd.length()) > 0) {
						bMatched = false;
						break;
					}
				}
			}

			for (int i = 0; i < columnList.length; i++) {
				if (!recordColumns.containsKey(columnList[i])) {
					bMatched = false;
					break;
				}
			}
			recordColumns.clear();

			/*
			 * Add delete to the
			 * batch
			 */
			if (bMatched) {
				deleteList.add(new Delete(row.get())); 
			}
			if (deleteList.size() == BATCH_SIZE) {
				table.delete(deleteList); /* Submit batch */
				deleteList.clear(); /* Clear batch */
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (deleteList.size() > 0) {
				table.delete(deleteList); /* Submit remaining batch */
			}
			table.close(); /* Close table */
			connection.close();
		}
	}

	@Override
	public void run(String[] args) {
		ArgumentParser parser = ArgumentParsers
				.newArgumentParser("RecordDelete").defaultHelp(true)
				.description("Delete table records by value.");
		parser.addArgument("-zk").help("property zk address");
		parser.addArgument("-table").help("property table name");
		parser.addArgument("-condition").help("value range condition");

		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
			if (null == ns) {
				System.out.println("lack arguments!");
				parser.printHelp();
				System.exit(1);
			}
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}

		String table = ns.getString("table");
		String condition = ns.getString("condition");
		String zk = ns.getString("zk");

		if (null == table || null == zk || null == condition) {
			System.out.println("invalid arguments");
			parser.printHelp();
			System.exit(1);
		}

		config.set("hbase.zookeeper.quorum", zk);
		config.set("mapreduce.job.tags", "DeleteByValue" + table);

		config.set(TABLE_NAME, table);

		Pattern pattern = Pattern.compile("(.+?:\\[.+?\\sTO\\s.+?\\])");
		Pattern subPattern = Pattern
				.compile("(.+?):\\[(.+?)\\s+TO\\s+(.+?)\\]");
		Matcher matcher = pattern.matcher(condition);
		StringBuilder builderColumns = new StringBuilder(10240);
		StringBuilder builderKey = new StringBuilder(1024);
		StringBuilder builderValue = new StringBuilder(1024);
		while (matcher.find()) {
			String matchCondition = matcher.group();
			Matcher subMatcher = subPattern.matcher(matchCondition);
			while (subMatcher.find()) {
				builderColumns.append(subMatcher.group(1));
				builderColumns.append(COLUMN_CONNECTOR);
				builderKey.append(subMatcher.group(1));
				builderKey.append(RANGE_START_SUFFIX);
				builderValue.append(subMatcher.group(2));
				config.set(builderKey.toString(), builderValue.toString());
				builderKey.delete(0, builderKey.length());
				builderValue.delete(0, builderValue.length());
				builderKey.append(subMatcher.group(1));
				builderKey.append(RANGE_END_SUFFIX);
				builderValue.append(subMatcher.group(3));
				config.set(builderKey.toString(), builderValue.toString());
			}
		}
		int nIndex = builderColumns.lastIndexOf(COLUMN_CONNECTOR);
		if (nIndex != -1) {
			builderColumns.replace(nIndex, builderColumns.length(), "");
		}
		config.set(COLUMN_LIST, builderColumns.toString());

		try {
			//Job job = new Job(config, "DeleteByValue");
			Job job = Job.getInstance(config, "DeleteByValue");
			job.setJarByClass(DeleteByValue.class); // class that contains mapper
			Scan scan = new Scan();
			scan.setCaching(500); // 1 is the default in Scan, which will be bad
									// for MapReduce jobs
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			TableMapReduceUtil.initTableMapperJob(table, // input HBase table  name
					scan, // Scan instance to control CF and attribute selection
					HbaseDeleteMapper.class, // mapper
					null, // mapper output key
					null, // mapper output value
					job);
			job.setNumReduceTasks(0);
			job.setOutputFormatClass(NullOutputFormat.class); 
			job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
