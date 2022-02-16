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

package org.apache.inlong.manager.client.api.inner;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.auth.Authentication;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.shiro.util.Assert;

@Slf4j
public class InnerInlongManagerClient {

    private OkHttpClient httpClient;

    private String uname;

    private String passwd;

    public InnerInlongManagerClient(InlongClientImpl inlongClient) {
        ClientConfiguration configuration = inlongClient.getConfiguration();
        Authentication authentication = configuration.getAuthentication();
        Assert.notNull(authentication, "Inlong should be authenticated");
        Assert.isTrue(authentication instanceof DefaultAuthentication,
                "Inlong only support default authentication");
        DefaultAuthentication defaultAuthentication = (DefaultAuthentication) authentication;
        this.uname = defaultAuthentication.getUserName();
        this.passwd = defaultAuthentication.getPassword();
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(configuration.getConnectTimeout(), configuration.getTimeUnit())
                .readTimeout(configuration.getReadTimeout(), configuration.getTimeUnit())
                .writeTimeout(configuration.getWriteTimeout(), configuration.getTimeUnit())
                .retryOnConnectionFailure(true)
                .build();
    }

    public BusinessInfo getBusinessInfo() {

        return null;
    }
}
