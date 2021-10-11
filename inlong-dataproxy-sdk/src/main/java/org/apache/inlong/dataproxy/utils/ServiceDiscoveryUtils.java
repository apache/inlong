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

package org.apache.inlong.dataproxy.utils;

import static org.apache.inlong.dataproxy.ConfigConstants.REQUEST_HEADER_AUTHORIZATION;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.dataproxy.ProxyClientConfig;
import org.apache.inlong.dataproxy.network.ProxysdkException;
import org.apache.inlong.dataproxy.network.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by elroyzhang on 2018/5/10.
 */
public class ServiceDiscoveryUtils {

    private static final Logger log = LoggerFactory.getLogger(ServiceDiscoveryUtils.class);

    private static String latestManagerIPList = "";
    private static String arraySed = ",";

    public static String getManagerIpList(ProxyClientConfig proxyClientConfig) throws ProxysdkException {
        String managerIpList;
        String managerAddress = proxyClientConfig.getManagerIP() + ":" + proxyClientConfig.getManagerPort();
        if (StringUtils.isBlank(managerAddress)) {
            log.error("managerAddress is blank.");
            return null;
        }

        managerIpList = getManagerIpListByHttp(managerAddress, proxyClientConfig);
        if (!StringUtils.isBlank(managerIpList)) {
            latestManagerIPList = managerIpList;
            return managerIpList;
        }
        log.error("ServiceDiscovery get managerIpList from "
                + "managerHost occur error, will try to get from managerIpList.");

        String[] managerIps = latestManagerIPList.split(arraySed);
        if (managerIps.length > 0) {
            for (String managerIp : managerIps) {
                if (!StringUtils.isBlank(managerIp)) {
                    String currentAddress = managerIp + ":" + proxyClientConfig.getManagerPort();
                    managerIpList = getManagerIpListByHttp(currentAddress, proxyClientConfig);
                    if (!StringUtils.isBlank(managerIpList)) {
                        latestManagerIPList = managerIpList;
                        return managerIpList;
                    } else {
                        log.error("ServiceDiscovery request " + managerIp
                                + " got by latestManagerIPList[" + latestManagerIPList
                                + "] got nothing, will try next ip.");
                    }
                } else {
                    log.error("ServiceDiscovery managerIp is null, "
                            + "latestManagerIPList is [" + latestManagerIPList
                            + "].");
                }
            }
        } else {
            log.error("ServiceDiscovery latestManagerIpList["
                    + latestManagerIPList + "] format error, or not contain ip");
        }

        String existedTdmIpList = getLocalManagerIpList(proxyClientConfig.getManagerIpLocalPath());
        if (!StringUtils.isBlank(existedTdmIpList)) {
            String[] existedTdmIps = existedTdmIpList.split(arraySed);
            if (existedTdmIps.length > 0) {
                for (String existedTdmIp : existedTdmIps) {
                    if (!StringUtils.isBlank(existedTdmIp)) {
                        String currentAddress = existedTdmIp + ":" + proxyClientConfig.getManagerPort();
                        managerIpList = getManagerIpListByHttp(currentAddress, proxyClientConfig);
                        if (!StringUtils.isBlank(managerIpList)) {
                            latestManagerIPList = managerIpList;
                            return managerIpList;
                        } else {
                            log.error("ServiceDiscovery request " + existedTdmIp + " got by local file["
                                    + proxyClientConfig.getManagerIpLocalPath() + "] got nothing, will try next ip.");
                        }
                    } else {
                        log.error("ServiceDiscovery get illegal format TdmIpList from local file, "
                                + "exist one ip is empty, managerIpList is ["
                                + existedTdmIpList + "], "
                                + "local file is [" + proxyClientConfig.getManagerIpLocalPath() + "]");
                    }
                }
            } else {
                log.error("ServiceDiscovery get illegal format TdmIpList from local file, "
                        + "managerIpList is [" + existedTdmIpList + "], "
                        + "local file is [" + proxyClientConfig.getManagerIpLocalPath() + "]");
            }
        } else {
            log.error("ServiceDiscovery get empty TdmIpList from local file, "
                    + "file path is [" + proxyClientConfig.getManagerIpLocalPath() + "].");
        }

        return managerIpList;
    }

    public static String getManagerIpListByHttp(String managerIp,
            ProxyClientConfig proxyClientConfig) throws ProxysdkException {
        String url =
                (proxyClientConfig.isLocalVisit() ? "http://" : "https://") + managerIp + "/api/getmanagervirtualip";
        ArrayList<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
        params.add(new BasicNameValuePair("operation", "query"));
        params.add(new BasicNameValuePair("username", proxyClientConfig.getUserName()));
        log.info("Begin to get configure from manager {}, param is {}", url, params);
        CloseableHttpClient httpClient = null;
        HttpPost httpPost = null;
        String returnStr = null;
        HttpParams myParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(myParams, proxyClientConfig.getManagerConnectionTimeout());
        HttpConnectionParams.setSoTimeout(myParams, proxyClientConfig.getManagerSocketTimeout());
        if (proxyClientConfig.isLocalVisit()) {
            httpClient = new DefaultHttpClient(myParams);
        } else {
            try {
                ArrayList<Header> headers = new ArrayList<Header>();
                for (BasicNameValuePair paramItem : params) {
                    headers.add(new BasicHeader(paramItem.getName(), paramItem.getValue()));
                }
                RequestConfig requestConfig = RequestConfig.custom()
                        .setConnectTimeout(10000).setSocketTimeout(30000).build();
                SSLContext sslContext = SSLContexts.custom().build();
                SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                        new String[]{"TLSv1"}, null,
                        SSLConnectionSocketFactory.getDefaultHostnameVerifier());
                httpClient = HttpClients.custom().setDefaultHeaders(headers)
                        .setDefaultRequestConfig(requestConfig).setSSLSocketFactory(sslsf).build();
            } catch (Throwable eHttps) {
                log.error("Create Https cliet failure, error 1 is ", eHttps);
                eHttps.printStackTrace();
                return null;
            }
        }

        try {
            httpPost = new HttpPost(url);
            if (proxyClientConfig.isNeedAuthentication()) {
                long timestamp = System.currentTimeMillis();
                int nonce = new SecureRandom(String.valueOf(timestamp).getBytes()).nextInt(Integer.MAX_VALUE);
                httpPost.setHeader(REQUEST_HEADER_AUTHORIZATION,
                        Utils.getAuthorizenInfo(proxyClientConfig.getUserName(),
                                proxyClientConfig.getSecretKey(), timestamp, nonce));
            }
            UrlEncodedFormEntity se = new UrlEncodedFormEntity(params);
            httpPost.setEntity(se);
            HttpResponse response = httpClient.execute(httpPost);
            returnStr = EntityUtils.toString(response.getEntity());
            if (Utils.isNotBlank(returnStr) && response.getStatusLine().getStatusCode() == 200) {
                log.info("Get configure from manager is " + returnStr);
                JsonParser jsonParser = new JsonParser();
                JsonObject jb = jsonParser.parse(returnStr).getAsJsonObject();
                JsonObject rd = jb.get("resultData").getAsJsonObject();
                String ip = rd.get("ip").getAsString();
                log.info("ServiceDiscovery updated managerVirtualIP success, ip : " + ip + ", retStr : " + returnStr);
                return ip;
            }
            return null;
        } catch (Throwable e) {
            log.error("Connect Manager error, {}", e.getMessage());
            return null;
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            if (httpClient != null) {
                httpClient.getConnectionManager().shutdown();
            }
        }
    }

    public static String getLocalManagerIpList(String managerIpLocalPath) {
        log.info("ServiceDiscovery start loading config for :{} from file ...", managerIpLocalPath);
        BufferedReader reader = null;
        String newestIp = null;
        try {
            File managerIpListFile = new File(managerIpLocalPath);
            if (!managerIpListFile.exists()) {
                log.info("ServiceDiscovery no found local bidInfo file, "
                        + "doesn't matter, path is [" + managerIpLocalPath + "].");
                return null;
            }
            byte[] serialized;
            serialized = FileUtils.readFileToByteArray(managerIpListFile);
            if (serialized == null) {
                return null;
            }
            newestIp = new String(serialized, "UTF-8");
            log.info("ServiceDiscovery get manager ip list from local success, result is : {}", newestIp);
        } catch (FileNotFoundException e) {
            log.error("ServiceDiscovery load manager config error, file not found. Exception info : {}", e);
        } catch (IOException e) {
            log.error("ServiceDiscovery load manager config error. Exception info : {}", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("ServiceDiscovery close bufferedReader "
                            + "error after loading InLong Manager config . Exception info : {}.", e);
                }
            }
        }
        return newestIp;
    }

    public static void updateManagerInfo2Local(String storeString, String path) {
        if (StringUtils.isBlank(storeString)) {
            log.warn("ServiceDiscovery updateTdmInfo2Local error, configMap is empty or managerIpList is blank.");
            return;
        }
        BufferedWriter writer = null;
        try {

            File localPath = new File(path);
            if (!localPath.getParentFile().exists()) {
                localPath.getParentFile().mkdirs();
            }
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(localPath), StandardCharsets.UTF_8));
            writer.write(storeString);
            writer.flush();
        } catch (UnsupportedEncodingException e) {
            log.error("ServiceDiscovery save manager config error1 .", e);
        } catch (FileNotFoundException e) {
            log.error("ServiceDiscovery save manager config error2 .", e);
        } catch (IOException e) {
            log.error("ServiceDiscovery save manager config error3 .", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("ServiceDiscovery close manager writer error.", e);
                }
            }
        }
    }
}
