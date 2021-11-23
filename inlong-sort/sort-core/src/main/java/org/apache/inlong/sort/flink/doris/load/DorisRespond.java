package org.apache.inlong.sort.flink.doris.load;

/**
 * @program: incubator-inlong
 * @author: huzekang
 * @create: 2021-11-23 18:57
 **/
public class DorisRespond {
	private int responseCode;
	private String responseStatusReason;
	private String responseBody;

	public DorisRespond(int responseCode, String responseStatusReason, String responseBody) {
		this.responseCode = responseCode;
		this.responseStatusReason = responseStatusReason;
		this.responseBody = responseBody;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public String getResponseStatusReason() {
		return responseStatusReason;
	}

	public void setResponseStatusReason(String responseStatusReason) {
		this.responseStatusReason = responseStatusReason;
	}

	public String getResponseBody() {
		return responseBody;
	}

	public void setResponseBody(String responseBody) {
		this.responseBody = responseBody;
	}
}
