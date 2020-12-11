/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.common.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.tubemq.corebase.utils.AddressUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * This class is used to process http connection and return result conversion,
 * currently does not support https
 */
public class HttpUtils {
    // log printer
    private static final Logger logger =
            LoggerFactory.getLogger(HttpUtils.class);


    /* Send request to target server. */
    public static JsonObject requestWebService(String url,
                                               Map<String, String> inParamMap) throws Exception {
        if (url == null) {
            throw new Exception("Web service url is null!");
        }
        if (url.trim().toLowerCase().startsWith("https://")) {
            throw new Exception("Unsupported https protocol!");
        }
        // process business parameters
        ArrayList<BasicNameValuePair> params = new ArrayList<>();
        if (inParamMap != null && !inParamMap.isEmpty()) {
            for (Map.Entry<String, String> entry : inParamMap.entrySet()) {
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            if (inParamMap.containsKey(WebFieldDef.CALLERIP.shortName)
                    || inParamMap.containsKey(WebFieldDef.CALLERIP.name)) {
                params.add(new BasicNameValuePair(WebFieldDef.CALLERIP.name,
                        AddressUtils.getIPV4LocalAddress()));
            }
        }
        // build connect configure
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(50000).setSocketTimeout(60000).build();
        // build HttpClient and HttpPost objects
        CloseableHttpClient httpclient = null;
        HttpPost httpPost = null;
        JsonObject jsonRes = null;
        JsonParser jsonParser = new JsonParser();
        try {
            httpclient = HttpClients.custom()
                    .setDefaultRequestConfig(requestConfig).build();
            httpPost = new HttpPost(url);
            UrlEncodedFormEntity se = new UrlEncodedFormEntity(params);
            httpPost.setEntity(se);
            // send http request and process response
            CloseableHttpResponse response = httpclient.execute(httpPost);
            String returnStr = EntityUtils.toString(response.getEntity());
            if (TStringUtils.isNotBlank(returnStr)
                    && response.getStatusLine().getStatusCode() == 200) {
                jsonRes = jsonParser.parse(returnStr).getAsJsonObject();
            }
        } catch (Throwable e) {
            throw new Exception("Connecting " + url + " throw an error!", e);
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException ie) {
                    logger.error("Close HttpClient error.", ie);
                }
            }
        }
        return jsonRes;
    }

}
