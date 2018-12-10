/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baosight.xinsight.ots.client.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * Singleton class encapsulating global REST servlet state and functions.
 */
public class ConnectionUtil {
	private static ConnectionUtil INSTANCE;
	private final Configuration conf;
	private final Connection connection;  

	
	/**
	 *@return the RESTServlet singleton instance
	 */
	public synchronized static ConnectionUtil getInstance() {
		assert(INSTANCE != null);
		return INSTANCE;
	}

	/**
	* @param conf Existing configuration to use in rest servlet
	* @param userProvider the login user provider
	* @return the RESTServlet singleton instance
	* @throws IOException
	*/
	public synchronized static ConnectionUtil init(Configuration conf) throws IOException {
		if (INSTANCE == null) {
			INSTANCE = new ConnectionUtil(conf);
		}
		return INSTANCE;
	}

	public synchronized void stop() {
		if (INSTANCE != null)  {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			INSTANCE = null;
		}
	}

	/**
	* Constructor with existing configuration
	* @param conf existing configuration
	* @param userProvider the login user provider
	* @throws IOException
	*/
	ConnectionUtil(final Configuration conf) throws IOException {
		this.conf = conf;
		connection = ConnectionFactory.createConnection(conf);
	}
	
	public Connection getConnection() throws IOException {
		return connection;
	}

	public Admin getAdmin() throws IOException {
		return connection.getAdmin();
	}
	
	/**
	* Caller closes the table afterwards.
	*/
	public Table getTable(String tableName) throws IOException {
		return connection.getTable(TableName.valueOf(tableName));
	}

	public Configuration getConfiguration() {
		return conf;
	}
}
