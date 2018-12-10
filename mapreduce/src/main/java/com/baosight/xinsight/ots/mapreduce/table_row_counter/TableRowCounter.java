
package com.baosight.xinsight.ots.mapreduce.table_row_counter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Job;

import com.baosight.xinsight.ots.mapreduce.OtsMapreduce;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * @author baoyuansong
 *
 */
public class TableRowCounter implements OtsMapreduce{
	public final String METRICS_COUNTERS = "org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters";
	public final String METRICS_ROWS = "ROWS";
	public final String DEFAULT_METRICS_PREFIX = "ots_metrics_";
	public final String DEFAULT_NAMESPACE = "default";
	public final String FIELD_ROW_COUNT = "record_count";
	String _zookeeperQuorum;
	String _fullTableName;
	
	public TableRowCounter()
	{
		
	}
	
	public TableRowCounter(String zookeeperQuorum, String fullTableName){
		_zookeeperQuorum = zookeeperQuorum;
		_fullTableName = fullTableName;
	}
	

	/**
	 * get table row counter
	 * @return
	 * @throws IOException
	 */
	public long getTableRowCount() throws IOException {
	    try {
	    	Configuration conf = HBaseConfiguration.create();
	    	conf.setStrings(HConstants.ZOOKEEPER_QUORUM, _zookeeperQuorum);
			String[] args = {_fullTableName};
		    Job job = RowCounter.createSubmittableJob(conf, args);
		    job.waitForCompletion(true);
		    return job.getCounters().findCounter(METRICS_COUNTERS, METRICS_ROWS).getValue();	
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    return -1;
	}
	
	/**
	 * save the table row counter to redis cluster
	 * @param rowCounter row counter
	 * @param redis_quorum redis cluster,the format is:host1:port,host2:port
	 * @param redis_pass redis password if needed
	 */
	private void saveTableRowCounter(long rowCounter, String redis_quorum, String redis_pass) {
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		String [] redis_hosts = redis_quorum.split(",");
		for(String host:redis_hosts){
			String []host_ports = host.split(":");
			//only with host,the port can be omitted if we use the default port 6379
			if (1 == host_ports.length){
				JedisShardInfo shardInfo = new JedisShardInfo(host_ports[0]);
				if (redis_pass != null && redis_pass.length() > 0){
					shardInfo.setPassword(redis_pass);
				}
				shards.add(shardInfo);
			}
			//with host and port
			else if (2 == host_ports.length){
				JedisShardInfo shardInfo = new JedisShardInfo(host_ports[0], Integer.parseInt(host_ports[1]));
				if (redis_pass != null && redis_pass.length() > 0){
					shardInfo.setPassword(redis_pass);
				}
				shards.add(shardInfo);
			}
			else {
				continue;
			}
		}
		
		JedisPoolConfig config = new JedisPoolConfig();
		ShardedJedisPool shardedJedisPool = new ShardedJedisPool(config, shards);
		ShardedJedis jedis = shardedJedisPool.getResource();
		HashMap<String, String > map = new HashMap<String, String >();
		
		map.put(FIELD_ROW_COUNT, String.valueOf(rowCounter));
		jedis.hmset(DEFAULT_METRICS_PREFIX + _fullTableName, map);
		shardedJedisPool.returnResource(jedis);
		shardedJedisPool.close();
	}
	
	public void run(String[] args) throws Exception {
		ArgumentParser parser = ArgumentParsers.newArgumentParser("TableRowCounter")
	                .defaultHelp(true)
	                .description("calculate the table row counter.");
        parser.addArgument("-rq", "--redis_quorum")
        .help("redis cluster address,the format is:host1:port1,host2:port2");
        parser.addArgument("-rp", "--redis_pass")
        .help("the redis password");
        parser.addArgument("-t", "--table_name")
        .help("the table name with namespace");
        parser.addArgument("-zk", "--zookeeper_quorum")
        .help("the zookeeper cluster hosts");
        
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        
        if (null == ns.getString("table_name") || null == ns.getString("redis_quorum")){
        	System.out.println("invalid arguments");
        	System.exit(1);
        }
	    
       TableRowCounter tableRowCounter = new TableRowCounter(ns.getString("zookeeper_quorum"), ns.getString("table_name"));
	   long table_row_counter = tableRowCounter.getTableRowCount();
	   tableRowCounter.saveTableRowCounter(table_row_counter, ns.getString("redis_quorum"), ns.getString("redis_pass"));
	}
}