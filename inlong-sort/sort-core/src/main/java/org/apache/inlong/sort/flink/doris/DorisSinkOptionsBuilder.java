package org.apache.inlong.sort.flink.doris;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * doris sink options
 **/
public class DorisSinkOptionsBuilder {
	private Integer batchSize;
	private Integer maxRetries;
	private Integer flushIntervalSecond;
	private String database;
	private String table;
	private String username;
	private String password;
	private String[] feHostPorts;
	public static final Integer DEFAULT_BATCH_SIZE = 1000;
	public static final Integer DEFAULT_MAX_RETRY_TIMES = 3;
	private static final Integer DEFAULT_INTERVAL_SECOND = 10;

	/**
	 * required, tableIdentifier. example => db1.tb1
	 */
	public DorisSinkOptionsBuilder setTableIdentifier(String tableIdentifier) {
		this.database = tableIdentifier.split("\\.")[0];
		this.table = tableIdentifier.split("\\.")[1];
		return this;
	}

	/**
	 * optional, batch size
	 */
	public DorisSinkOptionsBuilder setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * optional, max retry times
	 */
	public DorisSinkOptionsBuilder setMaxRetries(Integer maxRetries) {
		this.maxRetries = maxRetries;
		return this;
	}

	/**
	 * optional, max retry times
	 */
	public DorisSinkOptionsBuilder setFlushIntervalSecond(Integer flushIntervalSecond) {
		this.flushIntervalSecond = flushIntervalSecond;
		return this;
	}

	/**
	 * optional, internal scheduler to flush batch
	 */
	public DorisSinkOptionsBuilder setUsername(String username) {
		this.username = username;
		return this;
	}

	/**
	 * optional, password.
	 */
	public DorisSinkOptionsBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	/**
	 * required, fe web host port .example => host1:8030,host1:8030
	 */
	public DorisSinkOptionsBuilder setFeHostPorts(String fenodes) {
		this.feHostPorts = fenodes.split(",");
		return this;
	}


	public DorisSinkOptions build() {
		checkNotNull(feHostPorts, "Please set  feHostPort .");
		checkNotNull(database, "Please set  database .");
		checkNotNull(table, "Please set  table .");
		if (batchSize == null) {
			this.batchSize = DEFAULT_BATCH_SIZE;
		}

		if (flushIntervalSecond == null) {
			this.flushIntervalSecond = DEFAULT_INTERVAL_SECOND;
		}

		if (maxRetries == null) {
			this.maxRetries = DEFAULT_MAX_RETRY_TIMES;
		}
		return new DorisSinkOptions(batchSize, maxRetries, flushIntervalSecond,
				database, table, username, password, feHostPorts,
				new Properties());
	}
}
