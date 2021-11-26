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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DorisHttpUtils {
	private static final Logger LOG = LoggerFactory.getLogger(DorisHttpUtils.class);

	private static final String BACKENDS = "/rest/v1/system?path=//backends";


	public static String getRandomBeNode(String[] feHostPorts, int attemptTimes, String username, String password) {
		// random fe node
		final List<String> feHostList = Arrays.asList(feHostPorts);
		Collections.shuffle(feHostList);
		final String feNode = feHostList.get(0).trim();

		// commit request to get be nodes info
		String beUrl = "http://" + feNode + BACKENDS;
		HttpGet httpGet = new HttpGet(beUrl);
		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectTimeout(30 * 1000)
				.setSocketTimeout(30 * 1000)
				.build();
		httpGet.setConfig(requestConfig);
		HttpClientBuilder httpClientBuilder = HttpClients.custom();
		String basicAuthStr = Base64.getEncoder().encodeToString(String.format("%s:%s", username, password).getBytes(StandardCharsets.UTF_8));
		httpGet.setHeader("Authorization", "Basic " + basicAuthStr);

		for (int i = 0; i < attemptTimes; i++) {
			try (CloseableHttpClient client = httpClientBuilder.build()) {

				CloseableHttpResponse response = client.execute(httpGet);
				if (response.getEntity() != null) {
					String responseData = EntityUtils.toString(response.getEntity());
					final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
					// parse response to get random be node
					final DorisBeInfoResponse beInfoResponse = gson.fromJson(responseData, DorisBeInfoResponse.class);
					final String msg = beInfoResponse.getMsg();
					if (msg.equals("success")) {
						List<Map<String, Object>> rowList = (List<Map<String, Object>>) beInfoResponse.getData().get("rows");
						final List<String> beNodeList = rowList.stream().map(e -> {
							String bePort = (String) e.get("HttpPort");
							String beHost = (String) e.get("IP");
							return beHost + ":" + bePort;
						}).collect(Collectors.toList());
						Collections.shuffle(beNodeList);
						return beNodeList.get(0);
					}

				}
			} catch (Exception e) {
				String err = " Failed to  get be nodes info .The request url is :" + beUrl;
				LOG.warn(err);

			}
		}
		return null;
	}
}
