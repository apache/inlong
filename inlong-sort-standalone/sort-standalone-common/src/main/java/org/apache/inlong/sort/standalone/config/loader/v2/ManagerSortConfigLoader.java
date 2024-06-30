/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.config.loader.v2;

import org.apache.inlong.common.pojo.sort.SortConfig;
import org.apache.inlong.common.pojo.sort.SortConfigResponse;
import org.apache.inlong.common.util.Utils;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.holder.ManagerUrlHandler;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.util.concurrent.TimeUnit;

@Slf4j
public class ManagerSortConfigLoader implements SortConfigLoader {

    private Context context;
    private CloseableHttpClient httpClient;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String md5;

    private static synchronized CloseableHttpClient constructHttpClient() {
        long timeoutInMs = TimeUnit.MILLISECONDS.toMillis(50000);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout((int) timeoutInMs)
                .setSocketTimeout((int) timeoutInMs).build();
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        return httpClientBuilder.build();
    }

    @Override
    public void configure(Context context) {
        this.context = context;
        this.httpClient = constructHttpClient();
    }

    @Override
    public SortConfig load() {
        HttpGet httpGet = null;
        try {
            String clusterName = this.context.getString(CommonPropertiesHolder.KEY_CLUSTER_ID);
            String url = ManagerUrlHandler.getSortConfigUrl() + "?clusterName="
                    + clusterName + "&md5=";
            if (StringUtils.isNotBlank(this.md5)) {
                url += this.md5;
            }
            log.info("start to request {} to get config info", url);
            httpGet = new HttpGet(url);
            httpGet.addHeader(HttpHeaders.CONNECTION, "close");

            // request with get
            CloseableHttpResponse response = httpClient.execute(httpGet);
            String returnStr = EntityUtils.toString(response.getEntity());
            log.info("end to request {}, result={}", url, returnStr);

            SortConfigResponse clusterResponse = objectMapper.readValue(returnStr, SortConfigResponse.class);
            int errCode = clusterResponse.getCode();
            if (errCode != SortConfigResponse.SUCC && errCode != SortConfigResponse.NO_UPDATE) {
                log.error("failed to get config info from url={}, error code={}, msg={}",
                        url, clusterResponse.getCode(), clusterResponse.getMsg());
                return null;
            }

            this.md5 = clusterResponse.getMd5();
            byte[] decompress = Utils
                    .gzipDecompress(clusterResponse.getData(), 0, clusterResponse.getData().length);
            return objectMapper.readValue(decompress, SortConfig.class);
        } catch (Exception ex) {
            log.error("exception caught", ex);
            return null;
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
    }
}
