package org.apache.inlong.sort.flink.doris.load;

import java.util.List;
import java.util.Map;

/**
 * @program: incubator-inlong
 * @author: huzekang
 * @create: 2021-11-24 14:23
 **/
public class BeInfoResponse {
	private String msg;
	private String code;
	private Map data;

	public BeInfoResponse(String msg, String code, Map data) {
		this.msg = msg;
		this.code = code;
		this.data = data;
	}

	public Map getData() {
		return data;
	}

	public void setData(Map data) {
		this.data = data;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}
}
