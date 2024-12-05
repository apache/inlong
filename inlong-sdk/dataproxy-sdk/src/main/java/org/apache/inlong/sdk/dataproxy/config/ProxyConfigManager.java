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

package org.apache.inlong.sdk.dataproxy.config;

import org.apache.inlong.common.pojo.dataproxy.DataProxyNodeInfo;
import org.apache.inlong.common.pojo.dataproxy.DataProxyNodeResponse;
import org.apache.inlong.common.util.BasicAuth;
import org.apache.inlong.sdk.dataproxy.ConfigConstants;
import org.apache.inlong.sdk.dataproxy.LoadBalance;
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.network.ClientMgr;
import org.apache.inlong.sdk.dataproxy.network.HashRing;
import org.apache.inlong.sdk.dataproxy.network.IpUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This thread requests dataproxy-host list from manager, including these functions:
 * 1. request dataproxy-host, support retry
 * 2. local file disaster
 * 3. based on request result, do update (including cache, local file, ClientMgr.proxyInfoList and ClientMgr.channels)
 */
public class ProxyConfigManager extends Thread {

    public static final String APPLICATION_JSON = "application/json";
    private static final Logger logger = LoggerFactory.getLogger(ProxyConfigManager.class);
    private final ProxyClientConfig clientConfig;
    private final ClientMgr clientManager;
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();
    private final Gson gson = new Gson();
    private final HashRing hashRing = HashRing.getInstance();
    private List<HostInfo> proxyInfoList = new ArrayList<HostInfo>();
    /* the status of the cluster.if this value is changed,we need rechoose three proxy */
    private int oldStat = 0;
    private String inlongGroupId;
    private String localMd5;
    private boolean bShutDown = false;
    private long lstUpdatedTime = 0;
    private EncryptConfigEntry userEncryptConfigEntry;

    public ProxyConfigManager(final ProxyClientConfig configure, final ClientMgr clientManager) {
        this.clientConfig = configure;
        this.clientManager = clientManager;
        this.hashRing.setVirtualNode(configure.getVirtualNode());
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId;
    }

    public void shutDown() {
        logger.info("Begin to shut down ProxyConfigManager!");
        bShutDown = true;
    }

    @Override
    public void run() {
        while (!bShutDown) {
            try {
                doProxyEntryQueryWork();
                updateEncryptConfigEntry();
                logger.info("ProxyConf update!");
            } catch (Throwable e) {
                logger.error("Refresh proxy ip list runs into exception {}, {}", e.toString(), e.getStackTrace());
                e.printStackTrace();
            }

            /* Sleep some time.240-360s */
            try {
                Random random = new Random();
                int proxyUpdateIntervalSec = this.clientConfig.getProxyUpdateIntervalMinutes() * 60;

                int sleepTimeSec = proxyUpdateIntervalSec;
                if (proxyUpdateIntervalSec > 5) {
                    sleepTimeSec = proxyUpdateIntervalSec + random.nextInt() % (proxyUpdateIntervalSec / 5);
                }
                logger.info("sleep time {}", sleepTimeSec);
                Thread.sleep(sleepTimeSec * 1000);
            } catch (Throwable e2) {
                //
            }
        }
        logger.info("ProxyConfigManager worker existed!");
    }

    /**
     * try to read cache of proxy entry
     *
     * @return
     */
    private ProxyConfigEntry tryToReadCacheProxyEntry(String configCachePath) {
        rw.readLock().lock();
        try {
            File file = new File(configCachePath);
            long diffTime = System.currentTimeMillis() - file.lastModified();

            if (diffTime < clientConfig.getMaxProxyCacheTimeInMs()) {
                JsonReader reader = new JsonReader(new FileReader(configCachePath));
                ProxyConfigEntry proxyConfigEntry = gson.fromJson(reader, ProxyConfigEntry.class);
                logger.info("{} has a backup! {}", inlongGroupId, proxyConfigEntry);
                return proxyConfigEntry;
            }
        } catch (Exception ex) {
            logger.warn("try to read local cache, caught {}", ex.getMessage());
        } finally {
            rw.readLock().unlock();
        }
        return null;
    }

    private void tryToWriteCacheProxyEntry(ProxyConfigEntry entry, String configCachePath) {
        rw.writeLock().lock();
        try {
            File file = new File(configCachePath);
            if (!file.getParentFile().exists()) {
                // try to create parent
                file.getParentFile().mkdirs();
            }
            logger.info("try to write {}} to local cache {}", entry, configCachePath);
            FileWriter fileWriter = new FileWriter(configCachePath);
            gson.toJson(entry, fileWriter);
            fileWriter.flush();
            fileWriter.close();
        } catch (Exception ex) {
            logger.warn("try to write local cache, caught {}", ex.getMessage());
        } finally {
            rw.writeLock().unlock();
        }
    }

    private ProxyConfigEntry requestProxyEntryQuietly() {
        try {
            return requestProxyList(this.clientConfig.getManagerUrl());
        } catch (Exception e) {
            logger.warn("try to request proxy list by http, caught {}", e.getMessage());
        }
        return null;
    }

    /**
     * get groupId config
     *
     * @return proxyConfigEntry
     * @throws Exception
     */
    public ProxyConfigEntry getGroupIdConfigure() throws Exception {
        ProxyConfigEntry proxyEntry;
        String configAddr = clientConfig.getConfStoreBasePath() + inlongGroupId;
        if (this.clientConfig.isReadProxyIPFromLocal()) {
            configAddr = configAddr + ".local";
            proxyEntry = getLocalProxyListFromFile(configAddr);
        } else {
            configAddr = configAddr + ".proxyip";

            proxyEntry = tryToReadCacheProxyEntry(configAddr);
            if (proxyEntry == null) {
                proxyEntry = requestProxyEntryQuietly();
                int requestCount = 0;

                while (requestCount < 3 && proxyEntry == null) {
                    proxyEntry = requestProxyEntryQuietly();
                    requestCount += 1;
                    if (proxyEntry == null) {
                        // sleep then retry
                        TimeUnit.MILLISECONDS.sleep(500);
                    }
                }
            }
            if (proxyEntry == null) {
                throw new Exception("Visit manager error, please check log!");
            } else {
                tryToWriteCacheProxyEntry(proxyEntry, configAddr);
            }
        }
        return proxyEntry;
    }

    /**
     * request proxyHost list from manager, update ClientMgr.proxyHostList and channels
     *
     * @throws Exception
     */
    public void doProxyEntryQueryWork() throws Exception {
        /* Request the configuration from manager. */
        if (localMd5 == null) {
            localMd5 = calcHostInfoMd5(proxyInfoList);
        }
        ProxyConfigEntry proxyEntry = null;
        String configAddr = clientConfig.getConfStoreBasePath() + inlongGroupId;
        if (clientConfig.isReadProxyIPFromLocal()) {
            configAddr = configAddr + ".local";
            proxyEntry = getLocalProxyListFromFile(configAddr);
        } else {
            /* Do a compare and see if it needs to re-choose the channel. */
            configAddr = configAddr + ".managerip";
            int retryCount = 1;
            while (proxyEntry == null && retryCount < this.clientConfig.getProxyUpdateMaxRetry()) {
                proxyEntry = requestProxyEntryQuietly();
                retryCount++;
                if (proxyEntry == null) {
                    // sleep then retry.
                    TimeUnit.SECONDS.sleep(1);
                }
            }
            if (proxyEntry != null) {
                tryToWriteCacheProxyEntry(proxyEntry, configAddr);
            }
            /* We should exit if no local IP list and can't request it from manager. */
            if (localMd5 == null && proxyEntry == null) {
                logger.error("Can't connect manager at the start of proxy API {}",
                        this.clientConfig.getManagerUrl());
                proxyEntry = tryToReadCacheProxyEntry(configAddr);
            }
            if (localMd5 != null && proxyEntry == null && proxyInfoList != null) {
                StringBuffer s = new StringBuffer();
                for (HostInfo tmp : proxyInfoList) {
                    s.append(tmp.getHostName()).append(";").append(tmp.getPortNumber())
                            .append(",");
                }
                logger.warn("Backup proxyEntry [{}]", s);
            }
        }
        if (localMd5 == null && proxyEntry == null && proxyInfoList == null) {
            if (clientConfig.isReadProxyIPFromLocal()) {
                throw new Exception("Local proxy address configure "
                        + "read failure, please check first!");
            } else {
                throw new Exception("Connect Manager failure, please check first!");
            }
        }
        compareProxyList(proxyEntry);

    }

    /**
     * compare proxy list
     *
     * @param proxyEntry
     */
    private void compareProxyList(ProxyConfigEntry proxyEntry) {
        if (proxyEntry != null) {
            logger.info("{}", proxyEntry.toString());
            if (proxyEntry.getSize() != 0) {
                /* Initialize the current proxy information list first. */
                clientManager.setLoadThreshold(proxyEntry.getLoad());

                List<HostInfo> newProxyInfoList = new ArrayList<HostInfo>();
                for (Map.Entry<String, HostInfo> entry : proxyEntry.getHostMap().entrySet()) {
                    newProxyInfoList.add(entry.getValue());
                }

                String newMd5 = calcHostInfoMd5(newProxyInfoList);
                String oldMd5 = calcHostInfoMd5(proxyInfoList);
                if (newMd5 != null && !newMd5.equals(oldMd5)) {
                    /* Choose random alive connections to send messages. */
                    logger.info("old md5 {} new md5 {}", oldMd5, newMd5);
                    proxyInfoList.clear();
                    proxyInfoList = newProxyInfoList;
                    clientManager.setProxyInfoList(proxyInfoList);
                    lstUpdatedTime = System.currentTimeMillis();
                } else if (proxyEntry.getSwitchStat() != oldStat) {
                    /* judge cluster's switch state */
                    oldStat = proxyEntry.getSwitchStat();
                    if ((System.currentTimeMillis() - lstUpdatedTime) > 3 * 60 * 1000) {
                        logger.info("switch the cluster!");
                        proxyInfoList.clear();
                        proxyInfoList = newProxyInfoList;
                        clientManager.setProxyInfoList(proxyInfoList);
                    } else {
                        logger.info("only change oldStat ");
                    }
                } else {
                    newProxyInfoList.clear();
                    logger.info("proxy IP list doesn't change, load {}", proxyEntry.getLoad());
                }
                if (clientConfig.getLoadBalance() == LoadBalance.CONSISTENCY_HASH) {
                    updateHashRing(proxyInfoList);
                }
            } else {
                logger.error("proxyEntry's size is zero");
            }
        }
    }

    public EncryptConfigEntry getEncryptConfigEntry(final String userName) {
        if (StringUtils.isBlank(userName)) {
            return null;
        }
        EncryptConfigEntry encryptEntry = this.userEncryptConfigEntry;
        if (encryptEntry == null) {
            int retryCount = 0;
            encryptEntry = requestPubKey(this.clientConfig.getRsaPubKeyUrl(), userName, false);
            while (encryptEntry == null && retryCount < this.clientConfig.getProxyUpdateMaxRetry()) {
                encryptEntry = requestPubKey(this.clientConfig.getRsaPubKeyUrl(), userName, false);
                retryCount++;
            }
            if (encryptEntry == null) {
                encryptEntry = getStoredPubKeyEntry(userName);
                if (encryptEntry != null) {
                    encryptEntry.getRsaEncryptedKey();
                    synchronized (this) {
                        if (this.userEncryptConfigEntry == null) {
                            this.userEncryptConfigEntry = encryptEntry;
                        } else {
                            encryptEntry = this.userEncryptConfigEntry;
                        }
                    }
                }
            } else {
                synchronized (this) {
                    if (this.userEncryptConfigEntry == null || this.userEncryptConfigEntry != encryptEntry) {
                        storePubKeyEntry(encryptEntry);
                        encryptEntry.getRsaEncryptedKey();
                        this.userEncryptConfigEntry = encryptEntry;
                    } else {
                        encryptEntry = this.userEncryptConfigEntry;
                    }
                }
            }
        }
        return encryptEntry;
    }

    private void updateEncryptConfigEntry() {
        if (StringUtils.isBlank(this.clientConfig.getUserName())) {
            return;
        }
        int retryCount = 0;
        EncryptConfigEntry encryptConfigEntry = requestPubKey(this.clientConfig.getRsaPubKeyUrl(),
                this.clientConfig.getUserName(), false);
        while (encryptConfigEntry == null && retryCount < this.clientConfig.getProxyUpdateMaxRetry()) {
            encryptConfigEntry = requestPubKey(this.clientConfig.getRsaPubKeyUrl(),
                    this.clientConfig.getUserName(), false);
            retryCount++;
        }
        if (encryptConfigEntry == null) {
            return;
        }
        synchronized (this) {
            if (this.userEncryptConfigEntry == null || this.userEncryptConfigEntry != encryptConfigEntry) {
                storePubKeyEntry(encryptConfigEntry);
                encryptConfigEntry.getRsaEncryptedKey();
                this.userEncryptConfigEntry = encryptConfigEntry;
            }
        }
        return;
    }

    private EncryptConfigEntry getStoredPubKeyEntry(String userName) {
        if (StringUtils.isBlank(userName)) {
            logger.warn(" userName(" + userName + ") is not available");
            return null;
        }
        EncryptConfigEntry entry;
        FileInputStream fis = null;
        ObjectInputStream is = null;
        rw.readLock().lock();
        try {
            File file = new File(clientConfig.getConfStoreBasePath() + userName + ".pubKey");
            if (file.exists()) {
                fis = new FileInputStream(file);
                is = new ObjectInputStream(fis);
                entry = (EncryptConfigEntry) is.readObject();
                // is.close();
                fis.close();
                return entry;
            } else {
                return null;
            }
        } catch (Throwable e1) {
            logger.error("Read " + userName + " stored PubKeyEntry error ", e1);
            return null;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (Throwable e2) {
                    //
                }
            }
            rw.readLock().unlock();
        }
    }

    private void storePubKeyEntry(EncryptConfigEntry entry) {
        FileOutputStream fos = null;
        ObjectOutputStream p = null;
        rw.writeLock().lock();
        try {
            File file = new File(clientConfig.getConfStoreBasePath() + entry.getUserName() + ".pubKey");
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdir();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            fos = new FileOutputStream(file);
            p = new ObjectOutputStream(fos);
            p.writeObject(entry);
            p.flush();
            // p.close();
        } catch (Throwable e) {
            logger.error("store EncryptConfigEntry " + entry.toString() + " exception ", e);
            e.printStackTrace();
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (Throwable e2) {
                    //
                }
            }
            rw.writeLock().unlock();
        }
    }

    private String calcHostInfoMd5(List<HostInfo> hostInfoList) {
        if (hostInfoList == null || hostInfoList.isEmpty()) {
            return null;
        }
        Collections.sort(hostInfoList);
        StringBuffer hostInfoMd5 = new StringBuffer();
        for (HostInfo hostInfo : hostInfoList) {
            if (hostInfo == null) {
                continue;
            }
            hostInfoMd5.append(hostInfo.getHostName());
            hostInfoMd5.append(";");
            hostInfoMd5.append(hostInfo.getPortNumber());
            hostInfoMd5.append(";");
        }

        return DigestUtils.md5Hex(hostInfoMd5.toString());
    }

    private EncryptConfigEntry requestPubKey(String pubKeyUrl, String userName, boolean needGet) {
        if (StringUtils.isBlank(userName)) {
            logger.error("Queried userName is null!");
            return null;
        }
        List<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
        params.add(new BasicNameValuePair("operation", "query"));
        params.add(new BasicNameValuePair("username", userName));
        String returnStr = requestConfiguration(pubKeyUrl, params);
        if (StringUtils.isBlank(returnStr)) {
            logger.info("No public key information returned from manager");
            return null;
        }
        JsonObject pubKeyConf = JsonParser.parseString(returnStr).getAsJsonObject();
        if (pubKeyConf == null) {
            logger.info("No public key information returned from manager");
            return null;
        }
        if (!pubKeyConf.has("resultCode")) {
            logger.info("Parse pubKeyConf failure: No resultCode key information returned from manager");
            return null;
        }
        int resultCode = pubKeyConf.get("resultCode").getAsInt();
        if (resultCode != 0) {
            logger.info("query pubKeyConf failure, error code is " + resultCode + ", errInfo is "
                    + pubKeyConf.get("message").getAsString());
            return null;
        }
        if (!pubKeyConf.has("resultData")) {
            logger.info("Parse pubKeyConf failure: No resultData key information returned from manager");
            return null;
        }
        JsonObject resultData = pubKeyConf.get("resultData").getAsJsonObject();
        if (resultData != null) {
            String publicKey = resultData.get("publicKey").getAsString();
            if (StringUtils.isBlank(publicKey)) {
                return null;
            }
            String username = resultData.get("username").getAsString();
            if (StringUtils.isBlank(username)) {
                return null;
            }
            String versionStr = resultData.get("version").getAsString();
            if (StringUtils.isBlank(versionStr)) {
                return null;
            }
            return new EncryptConfigEntry(username, versionStr, publicKey);
        }
        return null;
    }

    public ProxyConfigEntry getLocalProxyListFromFile(String filePath) throws Exception {
        DataProxyNodeResponse proxyCluster;
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            proxyCluster = gson.fromJson(new String(fileBytes), DataProxyNodeResponse.class);
        } catch (Throwable e) {
            throw new Exception("Read local proxyList File failure by " + filePath + ", reason is " + e.getCause());
        }
        if (ObjectUtils.isEmpty(proxyCluster)) {
            logger.warn("no proxyCluster configure from local file");
            return null;
        }

        return getProxyConfigEntry(proxyCluster);
    }

    private Map<String, Integer> getStreamIdMap(JsonObject localProxyAddrJson) {
        Map<String, Integer> streamIdMap = new HashMap<String, Integer>();
        if (localProxyAddrJson.has("tsn")) {
            JsonArray jsonStreamId = localProxyAddrJson.getAsJsonArray("tsn");
            for (int i = 0; i < jsonStreamId.size(); i++) {
                JsonObject jsonItem = jsonStreamId.get(i).getAsJsonObject();
                if (jsonItem != null && jsonItem.has("streamId") && jsonItem.has("sn")) {
                    streamIdMap.put(jsonItem.get("streamId").getAsString(), jsonItem.get("sn").getAsInt());
                }
            }
        }
        return streamIdMap;
    }

    public ProxyConfigEntry requestProxyList(String url) {
        ArrayList<BasicNameValuePair> params = new ArrayList<BasicNameValuePair>();
        params.add(new BasicNameValuePair("ip", IpUtils.getLocalIp()));
        params.add(new BasicNameValuePair("protocolType", clientConfig.getProtocolType()));
        logger.info("Begin to get configure from manager {}, param is {}", url, params);

        String resultStr = requestConfiguration(url, params);
        ProxyClusterConfig clusterConfig = gson.fromJson(resultStr, ProxyClusterConfig.class);
        if (clusterConfig == null || !clusterConfig.isSuccess() || clusterConfig.getData() == null) {
            return null;
        }

        DataProxyNodeResponse proxyCluster = clusterConfig.getData();
        return getProxyConfigEntry(proxyCluster);
    }

    private ProxyConfigEntry getProxyConfigEntry(DataProxyNodeResponse proxyCluster) {
        List<DataProxyNodeInfo> nodeList = proxyCluster.getNodeList();
        if (CollectionUtils.isEmpty(nodeList)) {
            logger.error("dataproxy nodeList is empty in DataProxyNodeResponse!");
            return null;
        }
        Map<String, HostInfo> hostMap = formatHostInfoMap(nodeList);
        if (MapUtils.isEmpty(hostMap)) {
            return null;
        }

        int clusterId = -1;
        if (ObjectUtils.isNotEmpty(proxyCluster.getClusterId())) {
            clusterId = proxyCluster.getClusterId();
        }
        int load = ConfigConstants.LOAD_THRESHOLD;
        if (ObjectUtils.isNotEmpty(proxyCluster.getLoad())) {
            load = proxyCluster.getLoad() > 200 ? 200 : (Math.max(proxyCluster.getLoad(), 0));
        }
        boolean isIntranet = true;
        if (ObjectUtils.isNotEmpty(proxyCluster.getIsSwitch())) {
            isIntranet = proxyCluster.getIsIntranet() == 1 ? true : false;
        }
        int isSwitch = 0;
        if (ObjectUtils.isNotEmpty(proxyCluster.getIsSwitch())) {
            isSwitch = proxyCluster.getIsSwitch();
        }
        ProxyConfigEntry proxyEntry = new ProxyConfigEntry();
        proxyEntry.setClusterId(clusterId);
        proxyEntry.setGroupId(clientConfig.getInlongGroupId());
        proxyEntry.setInterVisit(isIntranet);
        proxyEntry.setHostMap(hostMap);
        proxyEntry.setSwitchStat(isSwitch);
        proxyEntry.setLoad(load);
        proxyEntry.setSize(nodeList.size());
        proxyEntry.setMaxPacketLength(
                proxyCluster.getMaxPacketLength() != null ? proxyCluster.getMaxPacketLength() : -1);
        return proxyEntry;
    }

    private Map<String, HostInfo> formatHostInfoMap(List<DataProxyNodeInfo> nodeList) {
        HostInfo tmpHostInfo;
        Map<String, HostInfo> hostMap = new HashMap<>();
        for (DataProxyNodeInfo proxy : nodeList) {
            if (ObjectUtils.isEmpty(proxy.getId()) || StringUtils.isEmpty(proxy.getIp()) || ObjectUtils
                    .isEmpty(proxy.getPort()) || proxy.getPort() < 0) {
                logger.error("invalid proxy node, id:{}, ip:{}, port:{}", proxy.getId(), proxy.getIp(),
                        proxy.getPort());
                continue;
            }
            tmpHostInfo = new HostInfo(proxy.getIp(), proxy.getPort());
            hostMap.put(tmpHostInfo.getReferenceName(), tmpHostInfo);

        }
        if (hostMap.isEmpty()) {
            logger.error("Parse proxyList failure: address is empty for response from manager!");
            return null;
        }
        return hostMap;
    }

    /* Request new configurations from Manager. */
    private String requestConfiguration(String url, List<BasicNameValuePair> params) {
        if (StringUtils.isBlank(url)) {
            logger.error("request url is null");
            return null;
        }
        HttpPost httpPost = null;
        HttpParams myParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(myParams, 10000);
        HttpConnectionParams.setSoTimeout(myParams, clientConfig.getManagerSocketTimeout());
        CloseableHttpClient httpClient;
        if (this.clientConfig.isRequestByHttp()) {
            httpClient = new DefaultHttpClient(myParams);
        } else {
            try {
                httpClient = getCloseableHttpClient(params);
            } catch (Throwable eHttps) {
                logger.error("Create Https cliet failure, error 1 is ", eHttps);
                eHttps.printStackTrace();
                return null;
            }
        }
        logger.info("Request url : {}, params : {}", url, params);
        try {
            httpPost = new HttpPost(url);
            httpPost.addHeader(BasicAuth.BASIC_AUTH_HEADER,
                    BasicAuth.genBasicAuthCredential(clientConfig.getAuthSecretId(),
                            clientConfig.getAuthSecretKey()));
            UrlEncodedFormEntity urlEncodedFormEntity = new UrlEncodedFormEntity(params, "UTF-8");
            httpPost.setEntity(urlEncodedFormEntity);
            HttpResponse response = httpClient.execute(httpPost);
            String returnStr = EntityUtils.toString(response.getEntity());
            if (StringUtils.isNotBlank(returnStr)
                    && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                logger.info("Get configure from manager is {}", returnStr);
                return returnStr;
            }
            return null;
        } catch (Throwable e) {
            logger.error("Connect Manager error, message: {}, url is {}", e.getMessage(), url);
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

    private StringEntity getEntity(List<BasicNameValuePair> params) throws UnsupportedEncodingException {
        JsonObject jsonObject = new JsonObject();
        for (BasicNameValuePair pair : params) {
            jsonObject.addProperty(pair.getName(), pair.getValue());
        }
        StringEntity se = new StringEntity(jsonObject.toString());
        se.setContentType(APPLICATION_JSON);
        return se;
    }

    private CloseableHttpClient getCloseableHttpClient(List<BasicNameValuePair> params)
            throws NoSuchAlgorithmException, KeyManagementException {
        CloseableHttpClient httpClient;
        ArrayList<Header> headers = new ArrayList<Header>();
        for (BasicNameValuePair paramItem : params) {
            headers.add(new BasicHeader(paramItem.getName(), paramItem.getValue()));
        }
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(10000)
                .setSocketTimeout(clientConfig.getManagerSocketTimeout()).build();
        SSLContext sslContext = SSLContexts.custom().build();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                new String[]{clientConfig.getTlsVersion()}, null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        httpClient = HttpClients.custom().setDefaultHeaders(headers).setDefaultRequestConfig(requestConfig)
                .setSSLSocketFactory(sslsf).build();
        return httpClient;
    }

    public void updateHashRing(List<HostInfo> newHosts) {
        this.hashRing.updateNode(newHosts);
        logger.debug("update hash ring {}", hashRing.getVirtualNode2RealNode());
    }
}
