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
