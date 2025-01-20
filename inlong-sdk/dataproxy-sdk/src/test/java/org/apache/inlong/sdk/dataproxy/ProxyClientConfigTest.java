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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;

import org.junit.Assert;
import org.junit.Test;

public class ProxyClientConfigTest {

    @Test
    public void testManagerConfig() throws Exception {
        HttpMsgSenderConfig httpConfig = new HttpMsgSenderConfig(
                "http://127.0.0.1:800", "test_id", "secretId", "secretKey");
        HttpMsgSenderConfig httpConfig1 = httpConfig.clone();
        Assert.assertEquals(httpConfig, httpConfig1);
        httpConfig1.setRegionName("sz");
        httpConfig1.setHttpAsyncRptPoolConfig(50, 10);
        Assert.assertNotEquals(httpConfig1.getRegionName(), httpConfig.getRegionName());
        Assert.assertNotEquals(httpConfig1.getHttpAsyncRptCacheSize(), httpConfig.getHttpAsyncRptCacheSize());
        Assert.assertNotEquals(httpConfig1.getHttpAsyncRptWorkerNum(), httpConfig.getHttpAsyncRptWorkerNum());
        httpConfig.setRptDataByHttps(true);
        httpConfig.setMetaCacheExpiredMs(30000);
        Assert.assertNotEquals(httpConfig1.isRptDataByHttps(), httpConfig.isRptDataByHttps());
        Assert.assertNotEquals(httpConfig1.getMetaCacheExpiredMs(), httpConfig.getMetaCacheExpiredMs());

        SubHttpClass subHttpClass = new SubHttpClass(httpConfig);
        subHttpClass.getHttpConfig().setRptDataByHttps(false);
        subHttpClass.getHttpConfig().setMetaCacheExpiredMs(99999);
        Assert.assertNotEquals(
                subHttpClass.getHttpConfig().isRptDataByHttps(), httpConfig.isRptDataByHttps());
        Assert.assertNotEquals(
                subHttpClass.getHttpConfig().getMetaCacheExpiredMs(), httpConfig.getMetaCacheExpiredMs());
    }

    public static class BaseClass {

        protected final ProxyClientConfig proxyConfig;

        public BaseClass(ProxyClientConfig configure) {
            proxyConfig = configure.clone();
        }
    }

    public static class SubHttpClass extends BaseClass {

        private final HttpMsgSenderConfig httpConfig;

        public SubHttpClass(HttpMsgSenderConfig configure) {
            super(configure);
            this.httpConfig = (HttpMsgSenderConfig) proxyConfig;
        }

        public HttpMsgSenderConfig getHttpConfig() {
            return httpConfig;
        }
    }
}
