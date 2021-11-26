/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.flink.doris;

import java.io.Serializable;
import java.util.Properties;

/**
 * doris sink options
 **/
public class DorisSinkOptions implements Serializable {


	private final Integer batchSize;
	private final Integer maxRetries;
	private  Integer flushIntervalSecond;
	private final String database;
	private final String table;
	private final String username;
	private final String password;
	private final String[] feHostPorts;


	/**
	 * Properties for the StreamLoad.
	 */
	private final Properties streamLoadProp;

	public DorisSinkOptions(Integer batchSize, Integer maxRetries, Integer flushIntervalSecond, String database, String table, String username, String password, String[] feHostPorts, Properties streamLoadProp) {
		this.batchSize = batchSize;
		this.maxRetries = maxRetries;
		this.flushIntervalSecond = flushIntervalSecond;
		this.database = database;
		this.table = table;
		this.username = username;
		this.password = password;
		this.feHostPorts = feHostPorts;
		this.streamLoadProp = streamLoadProp;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public Integer getMaxRetries() {
		return maxRetries;
	}

	public Integer getFlushIntervalSecond() {
		return flushIntervalSecond;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String[] getFeHostPorts() {
		return feHostPorts;
	}

	public Properties getStreamLoadProp() {
		return streamLoadProp;
	}
}
