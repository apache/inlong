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
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.network.ClientMgr;
import org.apache.inlong.sdk.dataproxy.network.IpUtils;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.Tuple2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This thread requests dataproxy-host list from manager, including these functions:
 * 1. request dataproxy-host, support retry
 * 2. local file disaster
 * 3. based on request result, do update (including cache, local file, ClientMgr.proxyInfoList and ClientMgr.channels)
 */
public class ProxyConfigManager extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ProxyConfigManager.class);
    private static final LogCounter exptCounter = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter parseCounter = new LogCounter(10, 100000, 60 * 1000L);
    private static final Map<String, Tuple2<AtomicLong, String>> fetchFailProxyMap =
            new ConcurrentHashMap<>();
    private static final Map<String, Tuple2<AtomicLong, String>> fetchFailEncryptMap =
            new ConcurrentHashMap<>();
    private static final ReentrantReadWriteLock fileRw = new ReentrantReadWriteLock();

    private final String callerId;
    private final Gson gson = new Gson();
    private final ClientMgr clientManager;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final AtomicBoolean shutDown = new AtomicBoolean(false);
    // proxy configure info
    private ProxyClientConfig clientConfig = null;
    private String localProxyConfigStoreFile;
    private String proxyConfigVisitUrl;
    private String proxyQueryFailKey;
    private String proxyConfigCacheFile;
    private List<HostInfo> proxyInfoList = new ArrayList<>();
    private int oldStat = 0;
    private String localMd5;
    private long lstUpdateTime = 0;
    // encrypt configure info
    private String encryptConfigVisitUrl;
    private String encryptQueryFailKey;
    private String encryptConfigCacheFile;
    private EncryptConfigEntry userEncryptConfigEntry;

    public ProxyConfigManager(ProxyClientConfig configure) {
        this("MetaQuery", configure, null);
    }

    public ProxyConfigManager(String callerId, ProxyClientConfig configure, ClientMgr clientManager) {
        this.callerId = callerId;
        this.clientManager = clientManager;
        if (configure != null) {
            this.storeAndBuildMetaConfigure(configure);
        }
        if (this.clientManager != null) {
            this.setName("ConfigManager-" + this.callerId);
            logger.info("ConfigManager({}) started, groupId={}",
                    this.callerId, clientConfig.getInlongGroupId());
        }
    }

    /**
     * Update proxy client configure for query case
     *
     * @param configure  proxy client configure
     * @return process result
     */
    public Tuple2<Boolean, String> updProxyClientConfig(ProxyClientConfig configure) {
        if (this.shutDown.get()) {
            return new Tuple2<>(false, "SDK has shutdown!");
        }
        if (configure == null) {
            return new Tuple2<>(false, "ProxyClientConfig is null");
        }
        if (this.clientManager != null) {
            return new Tuple2<>(false, "Not allowed for non meta-query case!");
        }
        this.storeAndBuildMetaConfigure(configure);
        return new Tuple2<>(true, "OK");
    }

    public void shutDown() {
        if (clientManager == null) {
            return;
        }
        if (shutDown.compareAndSet(false, true)) {
            this.interrupt();
            logger.info("ConfigManager({}) begin to shutdown, groupId={}!",
                    this.callerId, clientConfig.getInlongGroupId());
        }
    }

    /**
     * get groupId config
     *
     * @return proxyConfigEntry
     * @throws Exception ex
     */
    public Tuple2<ProxyConfigEntry, String> getGroupIdConfigure(boolean needRetry) throws Exception {
        if (shutDown.get()) {
            return new Tuple2<>(null, "SDK has shutdown!");
        }
        if (clientConfig == null) {
            return new Tuple2<>(null, "Configure not initialized!");
        }
        if (clientConfig.isOnlyUseLocalProxyConfig()) {
            return getLocalProxyListFromFile(this.localProxyConfigStoreFile);
        } else {
            boolean readFromRmt = false;
            Tuple2<ProxyConfigEntry, String> result;
            result = tryToReadCacheProxyEntry();
            if (result.getF0() == null) {
                int retryCount = 0;
                do {
                    result = requestProxyEntryQuietly();
                    if (result.getF0() != null || !needRetry || shutDown.get()) {
                        if (result.getF0() != null) {
                            readFromRmt = true;
                        }
                        break;
                    }
                    // sleep then retry
                    Thread.sleep(500L);
                } while (++retryCount < clientConfig.getConfigSyncMaxRetryIfFail());
            }
            if (shutDown.get()) {
                return new Tuple2<>(null, "SDK has shutdown!");
            }
            if (result.getF0() == null) {
                return new Tuple2<>(null, "Visit manager error:" + result.getF1());
            } else if (readFromRmt) {
                tryToWriteCacheProxyEntry(result.getF0());
            }
            return result;
        }
    }

    /**
     * get encrypt config
     *
     * @return proxyConfigEntry
     * @throws Exception ex
     */
    public Tuple2<EncryptConfigEntry, String> getEncryptConfigure(boolean needRetry) throws Exception {
        if (!clientConfig.isEnableDataEncrypt()) {
            return new Tuple2<>(null, "Not need data encrypt!");
        }
        if (shutDown.get()) {
            return new Tuple2<>(null, "SDK has shutdown!");
        }
        if (clientConfig == null) {
            return new Tuple2<>(null, "Configure not initialized!");
        }
        EncryptConfigEntry encryptEntry = this.userEncryptConfigEntry;
        if (encryptEntry != null) {
            return new Tuple2<>(encryptEntry, "Ok");
        }
        boolean readFromRmt = false;
        Tuple2<EncryptConfigEntry, String> result = readCachedPubKeyEntry();
        if (result.getF0() == null) {
            int retryCount = 0;
            do {
                result = requestPubKeyFromManager();
                if (result.getF0() != null || !needRetry || shutDown.get()) {
                    if (result.getF0() != null) {
                        readFromRmt = true;
                    }
                    break;
                }
                // sleep then retry
                Thread.sleep(500L);
            } while (++retryCount < clientConfig.getConfigSyncMaxRetryIfFail());
        }
        if (shutDown.get()) {
            return new Tuple2<>(null, "SDK has shutdown!");
        }
        if (result.getF0() == null) {
            return new Tuple2<>(null, "Visit manager error:" + result.getF1());
        } else if (readFromRmt) {
            updateEncryptConfigEntry(result.getF0());
            writeCachePubKeyEntryFile(result.getF0());
        }
        return result;
    }

    @Override
    public void run() {
        logger.info("ConfigManager({}) thread start, groupId={}",
                this.callerId, clientConfig.getInlongGroupId());
        while (!shutDown.get()) {
            // update proxy nodes meta configures
            try {
                doProxyEntryQueryWork();
            } catch (Throwable ex) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) refresh proxy configure exception, groupId={}",
                            this.callerId, clientConfig.getInlongGroupId(), ex);
                }
            }
            // update encrypt configure
            if (clientConfig.isEnableDataEncrypt()) {
                try {
                    doEncryptConfigEntryQueryWork();
                } catch (Throwable ex) {
                    if (exptCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) refresh encrypt info exception, groupId={}",
                                this.callerId, clientConfig.getInlongGroupId(), ex);
                    }
                }
            }
            if (shutDown.get()) {
                break;
            }
            // sleep some time
            try {
                Thread.sleep(clientConfig.getManagerConfigSyncInrMs() + random.nextInt(100) * 100);
            } catch (Throwable e2) {
                //
            }
        }
        logger.info("ConfigManager({}) worker existed, groupId={}",
                this.callerId, this.clientConfig.getInlongGroupId());
    }

    /**
     * request proxyHost list from manager, update ClientMgr.proxyHostList and channels
     *
     * @throws Exception
     */
    public void doProxyEntryQueryWork() throws Exception {
        if (shutDown.get() || this.clientManager == null) {
            return;
        }
        /* Request the configuration from manager. */
        if (localMd5 == null) {
            localMd5 = calcHostInfoMd5(proxyInfoList);
        }
        Tuple2<ProxyConfigEntry, String> result;
        if (clientConfig.isOnlyUseLocalProxyConfig()) {
            result = getLocalProxyListFromFile(this.localProxyConfigStoreFile);
        } else {
            int retryCnt = 0;
            do {
                result = requestProxyEntryQuietly();
                if (result.getF0() != null || shutDown.get()) {
                    break;
                }
                // sleep then retry.
                Thread.sleep(2000L);
            } while (++retryCnt < this.clientConfig.getConfigSyncMaxRetryIfFail() && !shutDown.get());
            if (shutDown.get()) {
                return;
            }
            if (result.getF0() != null) {
                tryToWriteCacheProxyEntry(result.getF0());
            }
            /* We should exit if no local IP list and can't request it from TDManager. */
            if (localMd5 == null && result.getF0() == null) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) connect manager({}) failure, get cached configure, groupId={}",
                            this.callerId, this.proxyConfigVisitUrl, this.clientConfig.getInlongGroupId());
                }
                result = tryToReadCacheProxyEntry();
            }
            if (localMd5 != null && result.getF0() == null && proxyInfoList != null) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) connect manager({}) failure, using the last configure, groupId={}",
                            this.callerId, this.proxyConfigVisitUrl, this.clientConfig.getInlongGroupId());
                }
            }
        }
        if (localMd5 == null && result.getF0() == null && proxyInfoList == null) {
            if (clientConfig.isOnlyUseLocalProxyConfig()) {
                throw new Exception("Read local proxy configure failure, please check first!");
            } else {
                throw new Exception("Connect Manager failure, please check first!");
            }
        }
        compareAndUpdateProxyList(result.getF0());
    }

    private void doEncryptConfigEntryQueryWork() throws Exception {
        if (shutDown.get() || this.clientManager == null) {
            return;
        }
        int retryCount = 0;
        Tuple2<EncryptConfigEntry, String> result;
        do {
            result = requestPubKeyFromManager();
            if (result.getF0() != null || shutDown.get()) {
                break;
            }
            // sleep then retry
            Thread.sleep(500L);
        } while (++retryCount < clientConfig.getConfigSyncMaxRetryIfFail());
        if (shutDown.get()) {
            return;
        }
        if (result.getF0() == null) {
            if (this.userEncryptConfigEntry != null) {
                logger.warn("ConfigManager({}) connect manager({}) failure, using the last pubKey, secretId={}",
                        this.callerId, this.encryptConfigVisitUrl, this.clientConfig.getAuthSecretId());
                return;
            }
            throw new Exception("Visit manager error:" + result.getF1());
        }
        updateEncryptConfigEntry(result.getF0());
        writeCachePubKeyEntryFile(result.getF0());
    }

    public Tuple2<ProxyConfigEntry, String> getLocalProxyListFromFile(String filePath) {
        String strRet;
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            strRet = new String(fileBytes);
        } catch (Throwable ex) {
            return new Tuple2<>(null, "Read local configure failure from "
                    + filePath + ", reason is " + ex.getMessage());
        }
        if (StringUtils.isBlank(strRet)) {
            return new Tuple2<>(null, "Blank configure local file from " + filePath);
        }
        return getProxyConfigEntry(false, strRet);
    }

    private Tuple2<ProxyConfigEntry, String> requestProxyEntryQuietly() {
        // check cache failure
        String qryResult = getManagerQryResultInFailStatus(true);
        if (qryResult != null) {
            return new Tuple2<>(null, "Query fail(" + qryResult + ") just now, please retry later!");
        }
        // request meta info from manager
        List<BasicNameValuePair> params = buildProxyNodeQueryParams();
        logger.debug("ConfigManager({}) request configure to manager({}), param={}",
                this.callerId, this.proxyConfigVisitUrl, params);
        Tuple2<Boolean, String> queryResult =
                requestConfiguration(true, this.proxyConfigVisitUrl, params);
        if (!queryResult.getF0()) {
            return new Tuple2<>(null, queryResult.getF1());
        }
        // parse result
        logger.debug("ConfigManager({}) received configure, from manager({}), groupId={}, result={}",
                callerId, proxyConfigVisitUrl, clientConfig.getInlongGroupId(), queryResult.getF1());
        try {
            Tuple2<ProxyConfigEntry, String> parseResult =
                    getProxyConfigEntry(true, queryResult.getF1());
            if (parseResult.getF0() == null) {
                bookManagerQryFailStatus(true, parseResult.getF1());
            } else {
                rmvManagerQryFailStatus(true);
            }
            return parseResult;
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) parse failure, from manager({}), groupId={}, result={}",
                        callerId, proxyConfigVisitUrl, clientConfig.getInlongGroupId(), queryResult.getF1(), ex);
            }
            bookManagerQryFailStatus(true, ex.getMessage());
            return new Tuple2<>(null, ex.getMessage());
        }
    }

    private String calcHostInfoMd5(List<HostInfo> hostInfoList) {
        if (hostInfoList == null || hostInfoList.isEmpty()) {
            return null;
        }
        Collections.sort(hostInfoList);
        StringBuilder hostInfoMd5 = new StringBuilder();
        for (HostInfo hostInfo : hostInfoList) {
            if (hostInfo == null) {
                continue;
            }
            hostInfoMd5.append(hostInfo.getHostName());
            hostInfoMd5.append(":");
            hostInfoMd5.append(hostInfo.getPortNumber());
            hostInfoMd5.append(";");
        }
        return DigestUtils.md5Hex(hostInfoMd5.toString());
    }

    /**
     * compare proxy list
     *
     * @param proxyEntry
     */
    private void compareAndUpdateProxyList(ProxyConfigEntry proxyEntry) {
        if ((proxyEntry == null || proxyEntry.isNodesEmpty())
                && (proxyInfoList.isEmpty()
                        || (System.currentTimeMillis() - lstUpdateTime) < clientConfig.getForceReChooseInrMs())) {
            return;
        }
        int newSwitchStat;
        List<HostInfo> newBusInfoList;
        if (proxyEntry == null || proxyEntry.isNodesEmpty()) {
            newSwitchStat = oldStat;
            newBusInfoList = new ArrayList<>(proxyInfoList.size());
            newBusInfoList.addAll(proxyInfoList);
        } else {
            newSwitchStat = proxyEntry.getSwitchStat();
            newBusInfoList = new ArrayList<>(proxyEntry.getSize());
            for (Map.Entry<String, HostInfo> entry : proxyEntry.getHostMap().entrySet()) {
                newBusInfoList.add(entry.getValue());
            }
        }
        String newMd5 = calcHostInfoMd5(newBusInfoList);
        String oldMd5 = calcHostInfoMd5(proxyInfoList);
        boolean nodeChanged = newMd5 != null && !newMd5.equals(oldMd5);
        if (nodeChanged || newSwitchStat != oldStat
                || (System.currentTimeMillis() - lstUpdateTime) >= clientConfig.getForceReChooseInrMs()) {
            proxyInfoList = newBusInfoList;
            clientManager.updateProxyInfoList(nodeChanged, proxyInfoList);
            lstUpdateTime = System.currentTimeMillis();
            oldStat = newSwitchStat;
        }
    }

    private void tryToWriteCacheProxyEntry(ProxyConfigEntry entry) {
        logger.debug("ConfigManager({}) write {} to cache file ({})",
                this.callerId, entry, this.proxyConfigCacheFile);
        fileRw.writeLock().lock();
        try {
            File file = new File(this.proxyConfigCacheFile);
            if (!file.getParentFile().exists()) {
                // try to create parent
                file.getParentFile().mkdirs();
            }
            FileWriter fileWriter = new FileWriter(this.proxyConfigCacheFile);
            gson.toJson(entry, fileWriter);
            fileWriter.flush();
            fileWriter.close();
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) write cache file({}) exception, groupId={}, data={}",
                        this.callerId, this.clientConfig.getInlongGroupId(),
                        this.proxyConfigCacheFile, entry.toString(), ex);
            }
        } finally {
            fileRw.writeLock().unlock();
        }
    }

    /**
     * try to read cache of proxy entry
     *
     * @return read result
     */
    private Tuple2<ProxyConfigEntry, String> tryToReadCacheProxyEntry() {
        fileRw.readLock().lock();
        try {
            File file = new File(this.proxyConfigCacheFile);
            if (file.exists()) {
                long diffTime = System.currentTimeMillis() - file.lastModified();
                if (clientConfig.getConfigCacheExpiredMs() > 0
                        && diffTime < clientConfig.getConfigCacheExpiredMs()) {
                    JsonReader reader = new JsonReader(new FileReader(this.proxyConfigCacheFile));
                    ProxyConfigEntry proxyConfigEntry = gson.fromJson(reader, ProxyConfigEntry.class);
                    return new Tuple2<>(proxyConfigEntry, "Ok");
                }
                return new Tuple2<>(null, "cache configure expired!");
            } else {
                return new Tuple2<>(null, "no cache configure!");
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) read cache file({}) exception, groupId={}",
                        this.callerId, this.proxyConfigCacheFile, this.clientConfig.getInlongGroupId(), ex);
            }
            return new Tuple2<>(null, "read cache configure failure:" + ex.getMessage());
        } finally {
            fileRw.readLock().unlock();
        }
    }

    private Tuple2<EncryptConfigEntry, String> requestPubKeyFromManager() {
        // check cache failure
        String qryResult = getManagerQryResultInFailStatus(false);
        if (qryResult != null) {
            return new Tuple2<>(null, "Query fail(" + qryResult + ") just now, please retry later!");
        }
        // request meta info from manager
        List<BasicNameValuePair> params = buildPubKeyQueryParams();
        logger.debug("ConfigManager({}) request pubkey to manager({}), param={}",
                this.callerId, this.encryptConfigVisitUrl, params);
        Tuple2<Boolean, String> queryResult =
                requestConfiguration(false, this.encryptConfigVisitUrl, params);
        if (!queryResult.getF0()) {
            return new Tuple2<>(null, queryResult.getF1());
        }
        logger.debug("ConfigManager({}) received pubkey from manager({}), result={}",
                this.callerId, this.encryptConfigVisitUrl, queryResult.getF1());
        String errorMsg;
        JsonObject pubKeyConf;
        try {
            pubKeyConf = JsonParser.parseString(queryResult.getF1()).getAsJsonObject();
        } catch (Throwable ex) {
            if (parseCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) parse failure, secretId={}, config={}!",
                        this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
            }
            errorMsg = "parse pubkey failure:" + ex.getMessage();
            bookManagerQryFailStatus(false, errorMsg);
            return new Tuple2<>(null, errorMsg);
        }
        if (pubKeyConf == null) {
            errorMsg = "No public key information";
            bookManagerQryFailStatus(false, errorMsg);
            return new Tuple2<>(null, errorMsg);
        }
        try {
            if (!pubKeyConf.has("resultCode")) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) config failure: resultCode field not exist, secretId={}, config={}!",
                            this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
                }
                throw new Exception("resultCode field not exist");
            }
            int resultCode = pubKeyConf.get("resultCode").getAsInt();
            if (resultCode != 0) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) config failure: resultCode != 0, secretId={}, config={}!",
                            this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
                }
                throw new Exception("resultCode != 0!");
            }
            if (!pubKeyConf.has("resultData")) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) config failure: resultData field not exist, secretId={}, config={}!",
                            this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
                }
                throw new Exception("resultData field not exist");
            }
            JsonObject resultData = pubKeyConf.get("resultData").getAsJsonObject();
            if (resultData != null) {
                String publicKey = resultData.get("publicKey").getAsString();
                if (StringUtils.isBlank(publicKey)) {
                    if (parseCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) config failure: publicKey is blank, secretId={}, config={}!",
                                this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
                    }
                    throw new Exception("publicKey is blank!");
                }
                String username = resultData.get("username").getAsString();
                if (StringUtils.isBlank(username)) {
                    if (parseCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) config failure: username is blank, secretId={}, config={}!",
                                this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
                    }
                    throw new Exception("username is blank!");
                }
                String versionStr = resultData.get("version").getAsString();
                if (StringUtils.isBlank(versionStr)) {
                    if (parseCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) config failure: version is blank, secretId={}, config={}!",
                                this.callerId, this.clientConfig.getAuthSecretId(), queryResult.getF1());
                    }
                    throw new Exception("version is blank!");
                }
                rmvManagerQryFailStatus(false);
                return new Tuple2<>(new EncryptConfigEntry(username, versionStr, publicKey), "Ok");
            }
            throw new Exception("resultData value is null!");
        } catch (Throwable ex) {
            bookManagerQryFailStatus(false, ex.getMessage());
            return new Tuple2<>(null, ex.getMessage());
        }
    }

    private void updateEncryptConfigEntry(EncryptConfigEntry newEncryptEntry) {
        newEncryptEntry.getRsaEncryptedKey();
        this.userEncryptConfigEntry = newEncryptEntry;
    }

    private Tuple2<EncryptConfigEntry, String> readCachedPubKeyEntry() {
        ObjectInputStream is;
        FileInputStream fis = null;
        EncryptConfigEntry entry;
        fileRw.readLock().lock();
        try {
            File file = new File(this.encryptConfigCacheFile);
            if (file.exists()) {
                long diffTime = System.currentTimeMillis() - file.lastModified();
                if (clientConfig.getConfigCacheExpiredMs() > 0
                        && diffTime < clientConfig.getConfigCacheExpiredMs()) {
                    fis = new FileInputStream(file);
                    is = new ObjectInputStream(fis);
                    entry = (EncryptConfigEntry) is.readObject();
                    // is.close();
                    fis.close();
                    return new Tuple2<>(entry, "Ok");
                }
                return new Tuple2<>(null, "cache PubKeyEntry expired!");
            } else {
                return new Tuple2<>(null, "no PubKeyEntry file!");
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) read({}) file exception, secretId={}",
                        callerId, encryptConfigCacheFile, clientConfig.getAuthSecretId(), ex);
            }
            return new Tuple2<>(null, "read PubKeyEntry file failure:" + ex.getMessage());
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (Throwable e2) {
                    //
                }
            }
            fileRw.readLock().unlock();
        }
    }

    private void writeCachePubKeyEntryFile(EncryptConfigEntry entry) {
        ObjectOutputStream p;
        FileOutputStream fos = null;
        fileRw.writeLock().lock();
        try {
            File file = new File(this.encryptConfigCacheFile);
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
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) write file({}) exception, secretId={}, content={}",
                        callerId, encryptConfigCacheFile, clientConfig.getAuthSecretId(), entry.toString(), ex);
            }
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (Throwable e2) {
                    //
                }
            }
            fileRw.writeLock().unlock();
        }
    }

    /* Request new configurations from Manager. */
    private Tuple2<Boolean, String> requestConfiguration(
            boolean queryProxyInfo, String url, List<BasicNameValuePair> params) {
        HttpParams myParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(myParams, clientConfig.getManagerConnTimeoutMs());
        HttpConnectionParams.setSoTimeout(myParams, clientConfig.getManagerSocketTimeoutMs());
        CloseableHttpClient httpClient;
        // build http(s) client
        try {
            if (this.clientConfig.isVisitManagerByHttp()) {
                httpClient = new DefaultHttpClient(myParams);
            } else {
                httpClient = getCloseableHttpClient(params);
            }
        } catch (Throwable eHttp) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) create Http(s) client failure, url={}, params={}",
                        this.callerId, url, params, eHttp);
            }
            return new Tuple2<>(false, eHttp.getMessage());
        }
        // post request and get response
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(url);
            this.addAuthorizationInfo(httpPost);
            UrlEncodedFormEntity urlEncodedFormEntity =
                    new UrlEncodedFormEntity(params, StandardCharsets.UTF_8);
            httpPost.setEntity(urlEncodedFormEntity);
            HttpResponse response = httpClient.execute(httpPost);
            String errMsg;
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                errMsg = response.getStatusLine().getStatusCode()
                        + ":" + response.getStatusLine().getReasonPhrase();
                if (response.getStatusLine().getStatusCode() >= 500) {
                    bookManagerQryFailStatus(queryProxyInfo, errMsg);
                }
                return new Tuple2<>(false, errMsg);
            }
            String returnStr = EntityUtils.toString(response.getEntity());
            if (StringUtils.isBlank(returnStr)) {
                errMsg = "server return blank entity!";
                bookManagerQryFailStatus(queryProxyInfo, errMsg);
                return new Tuple2<>(false, errMsg);
            }
            return new Tuple2<>(true, returnStr);
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) connect manager({}) exception, params={}",
                        this.callerId, url, params, ex);
            }
            return new Tuple2<>(false, ex.getMessage());
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            if (httpClient != null) {
                httpClient.getConnectionManager().shutdown();
            }
        }
    }

    private CloseableHttpClient getCloseableHttpClient(List<BasicNameValuePair> params)
            throws NoSuchAlgorithmException, KeyManagementException {
        CloseableHttpClient httpClient;
        ArrayList<Header> headers = new ArrayList<>();
        for (BasicNameValuePair paramItem : params) {
            headers.add(new BasicHeader(paramItem.getName(), paramItem.getValue()));
        }
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(clientConfig.getManagerConnTimeoutMs())
                .setSocketTimeout(clientConfig.getManagerSocketTimeoutMs()).build();
        SSLContext sslContext = SSLContexts.custom().build();
        SSLConnectionSocketFactory sslSf = new SSLConnectionSocketFactory(sslContext,
                new String[]{clientConfig.getTlsVersion()}, null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        httpClient = HttpClients.custom().setDefaultHeaders(headers).setDefaultRequestConfig(requestConfig)
                .setSSLSocketFactory(sslSf).build();
        return httpClient;
    }

    private void storeAndBuildMetaConfigure(ProxyClientConfig config) {
        this.clientConfig = config;
        StringBuilder strBuff = new StringBuilder(512);
        this.proxyConfigVisitUrl = strBuff
                .append(clientConfig.isVisitManagerByHttp() ? ConfigConstants.HTTP : ConfigConstants.HTTPS)
                .append(clientConfig.getManagerIP()).append(":").append(clientConfig.getManagerPort())
                .append(ConfigConstants.MANAGER_DATAPROXY_API).append(clientConfig.getInlongGroupId())
                .toString();
        strBuff.delete(0, strBuff.length());
        this.proxyQueryFailKey = strBuff
                .append("proxy:").append(clientConfig.getInlongGroupId())
                .append("#").append(clientConfig.getRegionName())
                .append("#").append(clientConfig.getProtocolType()).toString();
        strBuff.delete(0, strBuff.length());
        this.localProxyConfigStoreFile = strBuff
                .append(clientConfig.getConfigStoreBasePath())
                .append(ConfigConstants.META_STORE_SUB_DIR)
                .append(clientConfig.getInlongGroupId())
                .append(ConfigConstants.LOCAL_DP_CONFIG_FILE_SUFFIX)
                .toString();
        strBuff.delete(0, strBuff.length());
        this.proxyConfigCacheFile = strBuff
                .append(clientConfig.getConfigStoreBasePath())
                .append(ConfigConstants.META_STORE_SUB_DIR)
                .append(clientConfig.getInlongGroupId())
                .append(ConfigConstants.REMOTE_DP_CACHE_FILE_SUFFIX)
                .toString();
        strBuff.delete(0, strBuff.length());
        this.encryptConfigVisitUrl = clientConfig.getRsaPubKeyUrl();
        this.encryptQueryFailKey = strBuff
                .append("encrypt:").append(clientConfig.getUserName()).toString();
        strBuff.delete(0, strBuff.length());
        this.encryptConfigCacheFile = strBuff
                .append(clientConfig.getConfigStoreBasePath())
                .append(ConfigConstants.META_STORE_SUB_DIR)
                .append(clientConfig.getAuthSecretId())
                .append(ConfigConstants.REMOTE_ENCRYPT_CACHE_FILE_SUFFIX)
                .toString();
        strBuff.delete(0, strBuff.length());
    }

    private void addAuthorizationInfo(HttpPost httpPost) {
        httpPost.addHeader(BasicAuth.BASIC_AUTH_HEADER,
                BasicAuth.genBasicAuthCredential(clientConfig.getAuthSecretId(),
                        clientConfig.getAuthSecretKey()));
    }

    private List<BasicNameValuePair> buildProxyNodeQueryParams() {
        ArrayList<BasicNameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("ip", IpUtils.getLocalIp()));
        params.add(new BasicNameValuePair("protocolType", clientConfig.getProtocolType()));
        return params;
    }

    private List<BasicNameValuePair> buildPubKeyQueryParams() {
        List<BasicNameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("operation", "query"));
        params.add(new BasicNameValuePair("username", clientConfig.getUserName()));
        return params;
    }

    private void bookManagerQryFailStatus(boolean proxyQry, String errMsg) {
        if (proxyQry) {
            fetchFailProxyMap.put(proxyQueryFailKey,
                    new Tuple2<>(new AtomicLong(System.currentTimeMillis()), errMsg));
        } else {
            fetchFailEncryptMap.put(encryptQueryFailKey,
                    new Tuple2<>(new AtomicLong(System.currentTimeMillis()), errMsg));
        }
    }

    private void rmvManagerQryFailStatus(boolean proxyQry) {
        if (proxyQry) {
            fetchFailProxyMap.remove(proxyQueryFailKey);
        } else {
            fetchFailEncryptMap.remove(encryptQueryFailKey);
        }
    }

    private String getManagerQryResultInFailStatus(boolean proxyQry) {
        if (clientConfig.getConfigFailStatusExpiredMs() <= 0) {
            return null;
        }
        Tuple2<AtomicLong, String> queryResult;
        if (proxyQry) {
            queryResult = fetchFailProxyMap.get(proxyQueryFailKey);
        } else {
            queryResult = fetchFailEncryptMap.get(encryptQueryFailKey);
        }
        if (queryResult != null
                && (System.currentTimeMillis() - queryResult.getF0().get() < clientConfig
                        .getConfigFailStatusExpiredMs())) {
            return queryResult.getF1();
        }
        return null;
    }

    private Tuple2<ProxyConfigEntry, String> getProxyConfigEntry(boolean fromManager, String strRet) {
        DataProxyNodeResponse proxyNodeConfig;
        if (fromManager) {
            ProxyClusterConfig clusterConfig;
            try {
                clusterConfig = gson.fromJson(strRet, ProxyClusterConfig.class);
            } catch (Throwable ex) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) parse exception, groupId={}, config={}",
                            this.callerId, clientConfig.getInlongGroupId(), strRet, ex);
                }
                return new Tuple2<>(null, "parse failure:" + ex.getMessage());
            }
            if (clusterConfig == null) {
                return new Tuple2<>(null, "content parse result is null!");
            }
            if (!clusterConfig.isSuccess()) {
                return new Tuple2<>(null, clusterConfig.getErrMsg());
            }
            if (clusterConfig.getData() == null) {
                return new Tuple2<>(null, "return data content is null!");
            }
            proxyNodeConfig = clusterConfig.getData();
        } else {
            try {
                proxyNodeConfig = gson.fromJson(strRet, DataProxyNodeResponse.class);
            } catch (Throwable ex) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) parse local file exception, groupId={}, config={}",
                            this.callerId, clientConfig.getInlongGroupId(), strRet, ex);
                }
                return new Tuple2<>(null, "parse file failure:" + ex.getMessage());
            }
            if (proxyNodeConfig == null) {
                return new Tuple2<>(null, "file content parse result is null!");
            }
        }
        // parse nodeList
        List<DataProxyNodeInfo> nodeList = proxyNodeConfig.getNodeList();
        if (CollectionUtils.isEmpty(nodeList)) {
            return new Tuple2<>(null, "nodeList is empty!");
        }
        HostInfo tmpHostInfo;
        Map<String, HostInfo> hostMap = new HashMap<>();
        for (DataProxyNodeInfo proxy : nodeList) {
            if (ObjectUtils.isEmpty(proxy.getId())
                    || StringUtils.isEmpty(proxy.getIp())
                    || ObjectUtils.isEmpty(proxy.getPort())
                    || proxy.getPort() < 0) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("Invalid proxy node: groupId={}, id={}, ip={}, port={}",
                            clientConfig.getInlongGroupId(), proxy.getId(), proxy.getIp(), proxy.getPort());
                }
                continue;
            }
            tmpHostInfo = new HostInfo(proxy.getIp(), proxy.getPort());
            hostMap.put(tmpHostInfo.getReferenceName(), tmpHostInfo);
        }
        if (hostMap.isEmpty()) {
            return new Tuple2<>(null, "no valid nodeList records!");
        }
        // parse clusterId
        int clusterId = -1;
        if (ObjectUtils.isNotEmpty(proxyNodeConfig.getClusterId())) {
            clusterId = proxyNodeConfig.getClusterId();
        }
        // parse load
        int load = ConfigConstants.LOAD_THRESHOLD;
        if (ObjectUtils.isNotEmpty(proxyNodeConfig.getLoad())) {
            load = proxyNodeConfig.getLoad() > 200 ? 200 : (Math.max(proxyNodeConfig.getLoad(), 0));
        }
        // parse isIntranet
        boolean isIntranet = true;
        if (ObjectUtils.isNotEmpty(proxyNodeConfig.getIsIntranet())) {
            isIntranet = proxyNodeConfig.getIsIntranet() == 1;
        }
        // parse isSwitch
        int isSwitch = 0;
        if (ObjectUtils.isNotEmpty(proxyNodeConfig.getIsSwitch())) {
            isSwitch = proxyNodeConfig.getIsSwitch();
        }
        // build ProxyConfigEntry
        ProxyConfigEntry proxyEntry = new ProxyConfigEntry();
        proxyEntry.setClusterId(clusterId);
        proxyEntry.setGroupId(clientConfig.getInlongGroupId());
        proxyEntry.setInterVisit(isIntranet);
        proxyEntry.setHostMap(hostMap);
        proxyEntry.setSwitchStat(isSwitch);
        proxyEntry.setLoad(load);
        proxyEntry.setMaxPacketLength(
                proxyNodeConfig.getMaxPacketLength() != null ? proxyNodeConfig.getMaxPacketLength() : -1);
        return new Tuple2<>(proxyEntry, "ok");
    }
}
