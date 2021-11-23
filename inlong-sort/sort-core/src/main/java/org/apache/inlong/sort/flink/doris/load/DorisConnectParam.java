package org.apache.inlong.sort.flink.doris.load;

import org.apache.commons.codec.binary.Base64;
import org.stringtemplate.v4.ST;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @program: incubator-inlong
 * @author: huzekang
 * @create: 2021-11-23 16:08
 **/
public class DorisConnectParam {
	private String username;
	private String password;
	private String loadUrlStr;
	private String hostPort;
	private String database;
	private String tableName;
	private String basicAuthStr;
	private Properties streamLoadProp;



	public DorisConnectParam(String username, String password, String hostPort, String database, String tableName, Properties streamLoadProp) {
		this.username = username;
		this.password = password;
		this.hostPort = hostPort;
		this.database = database;
		this.tableName = tableName;
		this.streamLoadProp = streamLoadProp;
		// convert hostPort to loadUrlStr
		String loadUrlPattern = "http://<hostPort>/api/<database>/<tableName>/_stream_load?";
		final ST st = new ST(loadUrlPattern);
		st.add("hostPort", hostPort);
		st.add("database", database);
		st.add("tableName", tableName);
		this.loadUrlStr = st.render();
		// build auth basic
		this.basicAuthStr = basicAuthHeader(username,password);
	}

	private String basicAuthHeader(String username, String password) {
		final String tobeEncode = username + ":" + password;
		byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
		return "Basic " + new String(encoded);
	}


	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getLoadUrlStr() {
		return loadUrlStr;
	}

	public void setLoadUrlStr(String loadUrlStr) {
		this.loadUrlStr = loadUrlStr;
	}

	public String getHostPort() {
		return hostPort;
	}

	public void setHostPort(String hostPort) {
		this.hostPort = hostPort;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getBasicAuthStr() {
		return basicAuthStr;
	}

	public void setBasicAuthStr(String basicAuthStr) {
		this.basicAuthStr = basicAuthStr;
	}

	public Properties getStreamLoadProp() {
		return streamLoadProp;
	}

	public void setStreamLoadProp(Properties streamLoadProp) {
		this.streamLoadProp = streamLoadProp;
	}
}
