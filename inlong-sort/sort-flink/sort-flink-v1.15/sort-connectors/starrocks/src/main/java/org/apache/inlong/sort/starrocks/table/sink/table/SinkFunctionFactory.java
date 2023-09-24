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

package org.apache.inlong.sort.starrocks.table.sink.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionBase;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkSemantic;
import com.starrocks.connector.flink.tools.ConnectionUtils;
import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Create sink function according to the configuration. */
public class SinkFunctionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(
            SinkFunctionFactory.class);

    enum SinkVersion {
        // Implement exactly-once using stream load which has a
        // poor performance. All versions of StarRocks are supported
        V1,
        // Implement exactly-once using transaction load since StarRocks 2.4
        V2,
        // Select sink version automatically according to whether StarRocks
        // supports transaction load
        AUTO
    }

    public static boolean isStarRocksSupportTransactionLoad(StarRocksSinkOptions sinkOptions) {
        String host = ConnectionUtils.selectAvailableHttpHost(
                sinkOptions.getLoadUrlList(), sinkOptions.getConnectTimeout());
        if (host == null) {
            throw new RuntimeException("Can't find an available host in " + sinkOptions.getLoadUrlList());
        }

        String beginUrlStr = StreamLoadConstants.getBeginUrl(host);
        HttpPost httpPost = new HttpPost(beginUrlStr);
        httpPost.addHeader(HttpHeaders.AUTHORIZATION,
                StreamLoadUtils.getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
        httpPost.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).setRedirectsEnabled(true).build());
        LOG.info("Transaction load probe post {}", httpPost);

        HttpClientBuilder clientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {

                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = clientBuilder.build()) {
            CloseableHttpResponse response = client.execute(httpPost);
            String responseBody = EntityUtils.toString(response.getEntity());
            LOG.info("Transaction load probe response {}", responseBody);

            JSONObject bodyJson = JSON.parseObject(responseBody);
            String status = bodyJson.getString("status");
            String msg = bodyJson.getString("msg");

            // If StarRocks does not support transaction load, FE's NotFoundAction#executePost
            // will be called where you can know how the response json is constructed
            if ("FAILED".equals(status) && "Not implemented".equals(msg)) {
                return false;
            }
            return true;
        } catch (IOException e) {
            String errMsg = "Failed to probe transaction load for " + host;
            LOG.warn("{}", errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    public static void detectStarRocksFeature(StarRocksSinkOptions sinkOptions) {
        try {
            boolean supportTransactionLoad = isStarRocksSupportTransactionLoad(sinkOptions);
            sinkOptions.setSupportTransactionStreamLoad(supportTransactionLoad);
            if (supportTransactionLoad) {
                LOG.info("StarRocks supports transaction load");
            } else {
                LOG.info("StarRocks does not support transaction load");
            }
        } catch (Exception e) {
            LOG.warn("Can't decide whether StarRocks supports transaction load, and enable it by default.");
            sinkOptions.setSupportTransactionStreamLoad(true);
        }
    }

    public static SinkFunctionFactory.SinkVersion chooseSinkVersionAutomatically(StarRocksSinkOptions sinkOptions) {
        if (StarRocksSinkSemantic.AT_LEAST_ONCE.equals(sinkOptions.getSemantic())) {
            LOG.info("Choose sink version V2 for at-least-once.");
            return SinkVersion.V2;
        }

        if (sinkOptions.isSupportTransactionStreamLoad()) {
            LOG.info("StarRocks supports transaction load, and choose sink version V2");
            return SinkVersion.V2;
        } else {
            LOG.info("StarRocks does not support transaction load, and choose sink version V1");
            return SinkVersion.V1;
        }
    }

    public static SinkVersion getSinkVersion(StarRocksSinkOptions sinkOptions) {
        String sinkTypeOption = sinkOptions.getSinkVersion().trim().toUpperCase();
        SinkVersion sinkVersion;
        if (SinkVersion.V1.name().equals(sinkTypeOption)) {
            sinkVersion = SinkVersion.V1;
        } else if (SinkVersion.V2.name().equals(sinkTypeOption)) {
            sinkVersion = SinkVersion.V2;
        } else if (SinkVersion.AUTO.name().equals(sinkTypeOption)) {
            sinkVersion = chooseSinkVersionAutomatically(sinkOptions);
        } else {
            throw new UnsupportedOperationException("Unsupported sink type " + sinkTypeOption);
        }
        LOG.info("Choose sink version {}", sinkVersion.name());
        return sinkVersion;
    }

    public static <T> StarRocksDynamicSinkFunctionBase<T> createSinkFunction(
            StarRocksSinkOptions sinkOptions, TableSchema schema, StarRocksIRowTransformer<T> rowTransformer,
            String inlongMetric, String auditHostAndPorts, String auditKeys) {
        detectStarRocksFeature(sinkOptions);
        SinkVersion sinkVersion = getSinkVersion(sinkOptions);
        switch (sinkVersion) {
            case V1:
                return new StarRocksDynamicSinkFunction<>(sinkOptions, schema, rowTransformer);
            case V2:
                return new StarRocksDynamicSinkFunctionV2<>(sinkOptions, schema, rowTransformer,
                        inlongMetric, auditHostAndPorts, auditKeys);
            default:
                throw new UnsupportedOperationException("Unsupported sink type " + sinkVersion.name());
        }
    }

}
