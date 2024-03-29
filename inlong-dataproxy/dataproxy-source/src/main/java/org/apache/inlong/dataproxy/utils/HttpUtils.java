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

package org.apache.inlong.dataproxy.utils;

import org.apache.inlong.dataproxy.config.AuthUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;

public class HttpUtils {

    public static final String APPLICATION_JSON = "application/json";
    private static final Gson GSON = new GsonBuilder().create();

    public static StringEntity getEntity(Object obj) throws UnsupportedEncodingException {
        StringEntity se = new StringEntity(GSON.toJson(obj));
        se.setContentType(APPLICATION_JSON);
        return se;
    }

    public static HttpPost getHttPost(String url) {
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader(HttpHeaders.CONNECTION, "close");
        httpPost.addHeader(HttpHeaders.AUTHORIZATION, AuthUtils.genBasicAuth());
        return httpPost;
    }
}
