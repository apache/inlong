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
package org.apache.inlong.sort.flink.doris.load;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;


public class DorisStreamLoad implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

	private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));
	private DorisConnectParam dorisConnectParam;

	public DorisStreamLoad(DorisConnectParam dorisConnectParam) {
		this.dorisConnectParam = dorisConnectParam;
	}


	public void load(String value) {
		// use dorisConnectParam default stream load url
		load(value,this.dorisConnectParam.getStreamLoadUrl());
	}
	public void load(String value,String streamLoadUrl) {

		DorisRespond dorisRespond = null;

		// generate unique label
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
		String formatDate = sdf.format(new Date());
		String label = String.format("Inlong_%s_%s", formatDate,
				UUID.randomUUID().toString().replaceAll("-", ""));

		// permit  request  fe then redirect be
		final HttpClientBuilder httpClientBuilder = HttpClients
				.custom()
				.setRedirectStrategy(new DefaultRedirectStrategy() {
					@Override
					protected boolean isRedirectable(String method) {
						return true;
					}
				});

		try (CloseableHttpClient client = httpClientBuilder.build()) {
			HttpPut put = new HttpPut(streamLoadUrl);
			put.setHeader(HttpHeaders.EXPECT, "100-continue");
			put.setHeader(HttpHeaders.AUTHORIZATION, dorisConnectParam.getBasicAuthStr());
			put.setHeader("label", label);
			// use json format to  load data to Doris
			put.setHeader("format", "json");
			put.setHeader("strip_outer_array", "true");

			StringEntity entity = new StringEntity(value, "UTF-8");
			put.setEntity(entity);

			try (CloseableHttpResponse response = client.execute(put)) {
				final int statusCode = response.getStatusLine().getStatusCode();

				final String reasonPhrase = response.getStatusLine().getReasonPhrase();
				String loadResult = "";
				if (response.getEntity() != null) {
					loadResult = EntityUtils.toString(response.getEntity());
				}
				dorisRespond = new DorisRespond(statusCode, reasonPhrase, loadResult);
			}
		} catch (Exception e) {
			String err = "httpclient failed to  load data  to Apache Doris with label: " + label;
			LOG.warn(err, e);
		}

		LOG.info("Stream load Response: {} ", dorisRespond);
		final String responseBody = dorisRespond.getResponseBody();
		// assert responseCode
		if (dorisRespond.getResponseCode() != HttpStatus.SC_OK) {
			throw new RuntimeException("stream load error: " + responseBody);
		} else {
			// parse respond body to POJO
			final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			DorisRespondMsg dorisRespondMsg = gson.fromJson(dorisRespond.getResponseBody(), DorisRespondMsg.class);
			// get Error Detail from Msg
			if (!DORIS_SUCCESS_STATUS.contains(dorisRespondMsg.getStatus())) {
				final String errorMsgDetail = getErrorMsgDetail(dorisRespondMsg.getErrorURL());
				String errMsg = String.format("Stream load error .Reason: %s, the detail Error Msg is : 【 %s 】", dorisRespondMsg.getMessage(), errorMsgDetail);
				throw new RuntimeException(errMsg);
			}
		}
	}


	private String getErrorMsgDetail(String errorUrl) {
		String errorMsgDetail = "";
		HttpClientBuilder httpClientBuilder = HttpClients.custom();

		try (CloseableHttpClient client = httpClientBuilder.build()) {
			HttpGet httpGet = new HttpGet(errorUrl);

			CloseableHttpResponse response = client.execute(httpGet);

			if (response.getEntity() != null) {
				errorMsgDetail = EntityUtils.toString(response.getEntity());
			}
		} catch (Exception e) {
			String err = "httpclient failed to  get Error Detail Message  ";
			LOG.warn(err, e);
		}
		return errorMsgDetail;
	}

}
