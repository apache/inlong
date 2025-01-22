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
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;
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
    private final ConfigHolder configHolder;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final AtomicBoolean shutDown = new AtomicBoolean(false);
    // proxy configure info
    private ProxyClientConfig mgrConfig = null;
    private String localProxyConfigStoreFile;
    private String proxyConfigVisitUrl;
    private String proxyQueryFailKey;
    private String proxyConfigCacheFile;
    private ProxyConfigEntry proxyConfigEntry = null;
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

    public ProxyConfigManager(String callerId, ProxyClientConfig configure, ConfigHolder configHolder) {
        this.callerId = callerId;
        this.configHolder = configHolder;
        if (configure != null) {
            this.storeAndBuildMetaConfigure(configure);
        }
        if (this.configHolder != null) {
            this.setName("ConfigManager-" + this.callerId);
            logger.info("ConfigManager({}) started, groupId={}",
                    this.callerId, mgrConfig.getInlongGroupId());
        }
    }

    /**
     * Update proxy client configure for query case
     *
     * @param configure  proxy client configure
     * @return process result
     */
    public boolean updProxyClientConfig(ProxyClientConfig configure, ProcessResult procResult) {
        if (this.shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (this.configHolder != null) {
            return procResult.setFailResult(ErrorCode.ILLEGAL_CALL_STATE);
        }
        this.storeAndBuildMetaConfigure(configure);
        return procResult.setSuccess();
    }

    public void shutDown() {
        if (this.configHolder == null) {
            return;
        }
        if (shutDown.compareAndSet(false, true)) {
            this.interrupt();
            logger.info("ConfigManager({}) begin to shutdown, groupId={}!",
                    this.callerId, mgrConfig.getInlongGroupId());
        }
    }

    /**
     * query groupId configure from remote manager
     *
     * @return proxyConfigEntry
     */
    public boolean getGroupIdConfigure(boolean needRetry, ProcessResult procResult) {
        if (shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (mgrConfig == null) {
            return procResult.setFailResult(ErrorCode.CONFIGURE_NOT_INITIALIZED);
        }
        if (mgrConfig.isOnlyUseLocalProxyConfig()) {
            return getLocalProxyListFromFile(this.localProxyConfigStoreFile, procResult);
        } else {
            boolean readFromRmt = false;
            if (!tryToReadCacheProxyEntry(procResult)) {
                int retryCount = 0;
                while (!shutDown.get()) {
                    if (requestProxyEntryQuietly(procResult)) {
                        readFromRmt = true;
                        break;
                    }
                    if (!needRetry
                            || ++retryCount >= mgrConfig.getMetaQueryMaxRetryIfFail()
                            || shutDown.get()) {
                        break;
                    }
                    // sleep then retry
                    ProxyUtils.sleepSomeTime(mgrConfig.getMetaQueryWaitMsIfFail());
                }
            }
            if (shutDown.get()) {
                return procResult.setFailResult(ErrorCode.SDK_CLOSED);
            }
            if (readFromRmt && procResult.isSuccess()) {
                tryToWriteCacheProxyEntry((ProxyConfigEntry) procResult.getRetData());
            }
            return procResult.isSuccess();
        }
    }

    /**
     * query encrypt configure from remote manager
     *
     * @return proxyConfigEntry
     */
    public boolean getEncryptConfigure(boolean needRetry, ProcessResult procResult) {
        if (shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (mgrConfig == null) {
            return procResult.setFailResult(ErrorCode.CONFIGURE_NOT_INITIALIZED);
        }
        boolean readFromRmt = false;
        if (!readCachedPubKeyEntry(procResult)) {
            int retryCount = 0;
            while (!shutDown.get()) {
                if (requestPubKeyFromManager(procResult)) {
                    readFromRmt = true;
                    break;
                }
                if (!needRetry
                        || ++retryCount >= mgrConfig.getMetaQueryMaxRetryIfFail()
                        || shutDown.get()) {
                    break;
                }
                // sleep then retry
                ProxyUtils.sleepSomeTime(mgrConfig.getMetaQueryWaitMsIfFail());
            }
        }
        if (shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (readFromRmt && procResult.isSuccess()) {
            writeCachePubKeyEntryFile((EncryptConfigEntry) procResult.getRetData());
        }
        return procResult.isSuccess();
    }

    @Override
    public void run() {
        logger.info("ConfigManager({}) thread start, groupId={}",
                this.callerId, mgrConfig.getInlongGroupId());
        long curTime;
        ProcessResult procResult = new ProcessResult();
        while (!shutDown.get()) {
            // update proxy nodes meta configures
            curTime = System.currentTimeMillis();
            updateMetaInfoFromRemote(procResult);
            if (configHolder != null && configHolder.getMetricHolder() != null) {
                configHolder.getMetricHolder().addMetaSyncMetric(
                        procResult.getErrCode(), System.currentTimeMillis() - curTime);
            }
            if (shutDown.get()) {
                break;
            }
            // sleep some time
            ProxyUtils.sleepSomeTime(mgrConfig.getMgrMetaSyncInrMs() + random.nextInt(100) * 100);
        }
        logger.info("ConfigManager({}) worker existed, groupId={}",
                this.callerId, this.mgrConfig.getInlongGroupId());
    }

    private boolean updateMetaInfoFromRemote(ProcessResult procResult) {
        // update proxy nodes meta configures
        if (!doProxyEntryQueryWork(procResult)) {
            return procResult.isSuccess();
        }
        if (mgrConfig.isEnableReportEncrypt()) {
            if (!doEncryptConfigEntryQueryWork(procResult)) {
                return procResult.isSuccess();
            }
        }
        return procResult.setSuccess();
    }

    public ProxyConfigEntry getProxyConfigEntry() {
        return this.proxyConfigEntry;
    }

    public EncryptConfigEntry getUserEncryptConfigEntry() {
        return userEncryptConfigEntry;
    }
    /**
     * request proxyHost list from manager, update ClientMgr.proxyHostList and channels
     *
     * @throws Exception
     */
    public boolean doProxyEntryQueryWork(ProcessResult procResult) {
        /* Request the configuration from manager. */
        if (localMd5 == null) {
            localMd5 = calcHostInfoMd5(proxyInfoList);
        }
        ProxyConfigEntry rmtProxyConfigEntry = null;
        if (mgrConfig.isOnlyUseLocalProxyConfig()) {
            if (!getLocalProxyListFromFile(this.localProxyConfigStoreFile, procResult)) {
                return false;
            }
            rmtProxyConfigEntry = (ProxyConfigEntry) procResult.getRetData();
        } else {
            int retryCnt = 0;
            while (!shutDown.get()) {
                if (requestProxyEntryQuietly(procResult)) {
                    break;
                }
                if (++retryCnt >= this.mgrConfig.getMetaSyncMaxRetryIfFail() || shutDown.get()) {
                    break;
                }
                // sleep then retry.
                ProxyUtils.sleepSomeTime(mgrConfig.getMetaSyncWaitMsIfFail());
            }
            if (shutDown.get()) {
                return procResult.setFailResult(ErrorCode.SDK_CLOSED);
            }
            if (procResult.isSuccess()) {
                rmtProxyConfigEntry = (ProxyConfigEntry) procResult.getRetData();
                tryToWriteCacheProxyEntry(rmtProxyConfigEntry);
            }
            /* We should exit if no local IP list and can't request it from TDManager. */
            if (localMd5 == null && rmtProxyConfigEntry == null) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) connect manager({}) failure, get cached configure, groupId={}",
                            this.callerId, this.proxyConfigVisitUrl, this.mgrConfig.getInlongGroupId());
                }
                if (tryToReadCacheProxyEntry(procResult)) {
                    rmtProxyConfigEntry = (ProxyConfigEntry) procResult.getRetData();
                }
            }
            if (localMd5 != null && rmtProxyConfigEntry == null && proxyInfoList != null) {
                if (exptCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) connect manager({}) failure, using the last configure, groupId={}",
                            this.callerId, this.proxyConfigVisitUrl, this.mgrConfig.getInlongGroupId());
                }
            }
        }
        if (localMd5 == null && rmtProxyConfigEntry == null && proxyInfoList == null) {
            if (exptCounter.shouldPrint()) {
                if (mgrConfig.isOnlyUseLocalProxyConfig()) {
                    logger.warn("ConfigManager({}) continue fetch proxy meta failure, localFile={}, groupId={}",
                            this.callerId, this.localProxyConfigStoreFile, this.mgrConfig.getInlongGroupId());
                } else {
                    logger.warn("ConfigManager({}) continue fetch proxy meta failure, manager={}, groupId={}",
                            this.callerId, this.proxyConfigVisitUrl, this.mgrConfig.getInlongGroupId());
                }
            }
            return procResult.isSuccess();
        }
        compareAndUpdateProxyList(rmtProxyConfigEntry);
        return procResult.setSuccess();
    }

    public boolean doEncryptConfigEntryQueryWork(ProcessResult procResult) {
        int retryCount = 0;
        while (!shutDown.get()) {
            if (requestPubKeyFromManager(procResult)) {
                break;
            }
            if (++retryCount >= this.mgrConfig.getMetaSyncMaxRetryIfFail() || shutDown.get()) {
                break;
            }
            // sleep then retry
            ProxyUtils.sleepSomeTime(mgrConfig.getMetaSyncWaitMsIfFail());
        }
        if (shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        if (!procResult.isSuccess()) {
            if (exptCounter.shouldPrint()) {
                if (this.userEncryptConfigEntry == null) {
                    logger.warn("ConfigManager({}) continue fetch encrypt meta failure, manager={}, username={}",
                            this.callerId, this.encryptConfigVisitUrl, mgrConfig.getRptUserName());
                } else {
                    logger.warn("ConfigManager({}) fetch encrypt failure, manager={}, use the last pubKey, username={}",
                            this.callerId, this.encryptConfigVisitUrl, mgrConfig.getRptUserName());
                }
            }
            return procResult.isSuccess();
        }
        EncryptConfigEntry rmtEncryptEntry =
                (EncryptConfigEntry) procResult.getRetData();
        updateEncryptConfigEntry(rmtEncryptEntry);
        writeCachePubKeyEntryFile(rmtEncryptEntry);
        return procResult.setSuccess();
    }

    public boolean getLocalProxyListFromFile(String filePath, ProcessResult procResult) {
        String strRet;
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            strRet = new String(fileBytes);
        } catch (Throwable ex) {
            return procResult.setFailResult(ErrorCode.READ_LOCAL_FILE_FAILURE,
                    "Read local configure failure from "
                            + filePath + ", reason is " + ex.getMessage());
        }
        if (StringUtils.isBlank(strRet)) {
            return procResult.setFailResult(ErrorCode.BLANK_FILE_CONTENT,
                    "Blank configure local file from " + filePath);
        }
        return getProxyConfigEntry(false, strRet, procResult);
    }

    private boolean requestProxyEntryQuietly(ProcessResult procResult) {
        // check cache failure
        String qryResult = getManagerQryResultInFailStatus(true);
        if (qryResult != null) {
            return procResult.setFailResult(ErrorCode.FREQUENT_RMT_FAILURE_VISIT,
                    "Query fail(" + qryResult + ") just now, retry later!");
        }
        // request meta info from manager
        List<BasicNameValuePair> params = buildProxyNodeQueryParams();
        logger.debug("ConfigManager({}) request configure to manager({}), param={}",
                this.callerId, this.proxyConfigVisitUrl, params);

        if (!requestConfiguration(true, this.proxyConfigVisitUrl, params, procResult)) {
            return false;
        }
        String content = (String) procResult.getRetData();
        // parse result
        logger.debug("ConfigManager({}) received configure, from manager({}), groupId={}, result={}",
                callerId, proxyConfigVisitUrl, mgrConfig.getInlongGroupId(), content);
        try {
            if (getProxyConfigEntry(true, content, procResult)) {
                rmvManagerQryFailStatus(true);
            } else {
                bookManagerQryFailStatus(true, procResult.getErrMsg());
            }
            return procResult.isSuccess();
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) parse exception, from manager({}), groupId={}, result={}",
                        callerId, proxyConfigVisitUrl, mgrConfig.getInlongGroupId(), content, ex);
            }
            bookManagerQryFailStatus(true, ex.getMessage());
            return procResult.setFailResult(
                    ErrorCode.PARSE_PROXY_META_EXCEPTION, ex.getMessage());
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
                        || (System.currentTimeMillis() - lstUpdateTime) < mgrConfig.getForceReChooseInrMs())) {
            return;
        }
        int newSwitchStat;
        List<HostInfo> newProxyNodeList;
        if (proxyEntry == null || proxyEntry.isNodesEmpty()) {
            newSwitchStat = oldStat;
            newProxyNodeList = new ArrayList<>(proxyInfoList.size());
            newProxyNodeList.addAll(proxyInfoList);
        } else {
            this.proxyConfigEntry = proxyEntry;
            configHolder.updateAllowedMaxPkgLength(proxyEntry.getMaxPacketLength());
            newSwitchStat = proxyEntry.getSwitchStat();
            newProxyNodeList = new ArrayList<>(proxyEntry.getSize());
            for (Map.Entry<String, HostInfo> entry : proxyEntry.getHostMap().entrySet()) {
                newProxyNodeList.add(entry.getValue());
            }
        }
        String newMd5 = calcHostInfoMd5(newProxyNodeList);
        String oldMd5 = calcHostInfoMd5(proxyInfoList);
        boolean nodeChanged = newMd5 != null && !newMd5.equals(oldMd5);
        if (nodeChanged || newSwitchStat != oldStat
                || (System.currentTimeMillis() - lstUpdateTime) >= mgrConfig.getForceReChooseInrMs()) {
            proxyInfoList = newProxyNodeList;
            configHolder.updateProxyNodes(nodeChanged, proxyInfoList);
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
                        this.callerId, this.mgrConfig.getInlongGroupId(),
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
    private boolean tryToReadCacheProxyEntry(ProcessResult procResult) {
        fileRw.readLock().lock();
        try {
            File file = new File(this.proxyConfigCacheFile);
            if (file.exists()) {
                long diffTime = System.currentTimeMillis() - file.lastModified();
                if (mgrConfig.getMetaCacheExpiredMs() > 0
                        && diffTime < mgrConfig.getMetaCacheExpiredMs()) {
                    JsonReader reader = new JsonReader(new FileReader(this.proxyConfigCacheFile));
                    ProxyConfigEntry proxyConfigEntry = gson.fromJson(reader, ProxyConfigEntry.class);
                    return procResult.setSuccess(proxyConfigEntry);
                }
                return procResult.setFailResult(ErrorCode.LOCAL_FILE_EXPIRED);
            } else {
                return procResult.setFailResult(ErrorCode.LOCAL_FILE_NOT_EXIST);
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) read cache file({}) exception, groupId={}",
                        this.callerId, this.proxyConfigCacheFile, this.mgrConfig.getInlongGroupId(), ex);
            }
            return procResult.setFailResult(
                    ErrorCode.READ_LOCAL_FILE_FAILURE, "read cache configure failure:" + ex.getMessage());
        } finally {
            fileRw.readLock().unlock();
        }
    }

    private boolean requestPubKeyFromManager(ProcessResult procResult) {
        // check cache failure
        String qryResult = getManagerQryResultInFailStatus(false);
        if (qryResult != null) {
            procResult.setFailResult(ErrorCode.FREQUENT_RMT_FAILURE_VISIT,
                    "Query fail(" + qryResult + ") just now, retry later!");
        }
        // request meta info from manager
        List<BasicNameValuePair> params = buildPubKeyQueryParams();
        logger.debug("ConfigManager({}) request pubkey to manager({}), param={}",
                this.callerId, this.encryptConfigVisitUrl, params);
        if (!requestConfiguration(false, this.encryptConfigVisitUrl, params, procResult)) {
            return false;
        }
        String content = (String) procResult.getRetData();
        logger.debug("ConfigManager({}) received pubkey from manager({}), result={}",
                this.callerId, this.encryptConfigVisitUrl, content);
        String errorMsg;
        JsonObject pubKeyConf;
        try {
            pubKeyConf = JsonParser.parseString(content).getAsJsonObject();
        } catch (Throwable ex) {
            if (parseCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) parse failure, userName={}, config={}!",
                        this.callerId, this.mgrConfig.getRptUserName(), content);
            }
            errorMsg = "parse pubkey failure:" + ex.getMessage();
            bookManagerQryFailStatus(false, errorMsg);
            return procResult.setFailResult(
                    ErrorCode.PARSE_RMT_CONTENT_FAILURE, errorMsg);
        }
        if (pubKeyConf == null) {
            errorMsg = "No public key information";
            bookManagerQryFailStatus(false, errorMsg);
            return procResult.setFailResult(ErrorCode.PARSE_RMT_CONTENT_IS_NULL);
        }
        try {
            if (!pubKeyConf.has("resultCode")) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) config failure: resultCode field not exist, userName={}, config={}!",
                            this.callerId, this.mgrConfig.getRptUserName(), content);
                }
                throw new Exception("resultCode field not exist");
            }
            int resultCode = pubKeyConf.get("resultCode").getAsInt();
            if (resultCode != 0) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) config failure: resultCode != 0, userName={}, config={}!",
                            this.callerId, this.mgrConfig.getRptUserName(), content);
                }
                throw new Exception("resultCode != 0!");
            }
            if (!pubKeyConf.has("resultData")) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) config failure: resultData field not exist, userName={}, config={}!",
                            this.callerId, this.mgrConfig.getRptUserName(), content);
                }
                throw new Exception("resultData field not exist");
            }
            JsonObject resultData = pubKeyConf.get("resultData").getAsJsonObject();
            if (resultData != null) {
                String publicKey = resultData.get("publicKey").getAsString();
                if (StringUtils.isBlank(publicKey)) {
                    if (parseCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) config failure: publicKey is blank, userName={}, config={}!",
                                this.callerId, this.mgrConfig.getRptUserName(), content);
                    }
                    throw new Exception("publicKey is blank!");
                }
                String username = resultData.get("username").getAsString();
                if (StringUtils.isBlank(username)) {
                    if (parseCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) config failure: username is blank, userName={}, config={}!",
                                this.callerId, this.mgrConfig.getRptUserName(), content);
                    }
                    throw new Exception("username is blank!");
                }
                String versionStr = resultData.get("version").getAsString();
                if (StringUtils.isBlank(versionStr)) {
                    if (parseCounter.shouldPrint()) {
                        logger.warn("ConfigManager({}) config failure: version is blank, userName={}, config={}!",
                                this.callerId, this.mgrConfig.getRptUserName(), content);
                    }
                    throw new Exception("version is blank!");
                }
                rmvManagerQryFailStatus(false);
                return procResult.setSuccess(new EncryptConfigEntry(username, versionStr, publicKey));
            }
            throw new Exception("resultData value is null!");
        } catch (Throwable ex) {
            bookManagerQryFailStatus(false, ex.getMessage());
            return procResult.setFailResult(ErrorCode.PARSE_ENCRYPT_META_EXCEPTION, ex.getMessage());
        }
    }

    private void updateEncryptConfigEntry(EncryptConfigEntry newEncryptEntry) {
        newEncryptEntry.getRsaEncryptedKey();
        this.userEncryptConfigEntry = newEncryptEntry;
    }

    private boolean readCachedPubKeyEntry(ProcessResult procResult) {
        ObjectInputStream is;
        FileInputStream fis = null;
        EncryptConfigEntry entry;
        fileRw.readLock().lock();
        try {
            File file = new File(this.encryptConfigCacheFile);
            if (file.exists()) {
                long diffTime = System.currentTimeMillis() - file.lastModified();
                if (mgrConfig.getMetaCacheExpiredMs() > 0
                        && diffTime < mgrConfig.getMetaCacheExpiredMs()) {
                    fis = new FileInputStream(file);
                    is = new ObjectInputStream(fis);
                    entry = (EncryptConfigEntry) is.readObject();
                    // is.close();
                    fis.close();
                    return procResult.setSuccess(entry);
                }
                return procResult.setFailResult(ErrorCode.LOCAL_FILE_EXPIRED);
            } else {
                return procResult.setFailResult(ErrorCode.LOCAL_FILE_NOT_EXIST);
            }
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) read({}) file exception, userName={}",
                        callerId, encryptConfigCacheFile, mgrConfig.getRptUserName(), ex);
            }
            return procResult.setFailResult(
                    ErrorCode.READ_LOCAL_FILE_FAILURE, "read PubKeyEntry file failure:" + ex.getMessage());
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
                logger.warn("ConfigManager({}) write file({}) exception, userName={}, content={}",
                        callerId, encryptConfigCacheFile, mgrConfig.getRptUserName(), entry.toString(), ex);
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
    private boolean requestConfiguration(
            boolean queryProxyInfo, String url, List<BasicNameValuePair> params, ProcessResult procResult) {
        HttpParams myParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(myParams, mgrConfig.getMgrConnTimeoutMs());
        HttpConnectionParams.setSoTimeout(myParams, mgrConfig.getMgrSocketTimeoutMs());
        CloseableHttpClient httpClient;
        // build http(s) client
        try {
            if (this.mgrConfig.isVisitMgrByHttps()) {
                httpClient = getCloseableHttpClient(params);
            } else {
                httpClient = new DefaultHttpClient(myParams);
            }
        } catch (Throwable eHttp) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) create Http(s) client failure, url={}, params={}",
                        this.callerId, url, params, eHttp);
            }
            return procResult.setFailResult(
                    ErrorCode.BUILD_HTTP_CLIENT_EXCEPTION, eHttp.getMessage());
        }
        // post request and get response
        HttpPost httpPost = null;
        try {
            String errMsg;
            httpPost = new HttpPost(url);
            this.addAuthorizationInfo(httpPost);
            UrlEncodedFormEntity urlEncodedFormEntity =
                    new UrlEncodedFormEntity(params, StandardCharsets.UTF_8);
            httpPost.setEntity(urlEncodedFormEntity);
            HttpResponse response = httpClient.execute(httpPost);
            String returnStr = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                errMsg = response.getStatusLine().getStatusCode() + ":" + returnStr;
                if (response.getStatusLine().getStatusCode() >= 500) {
                    bookManagerQryFailStatus(queryProxyInfo, errMsg);
                }
                return procResult.setFailResult(ErrorCode.RMT_RETURN_FAILURE, errMsg);
            }
            if (StringUtils.isBlank(returnStr)) {
                errMsg = "server return blank entity!";
                bookManagerQryFailStatus(queryProxyInfo, errMsg);
                return procResult.setFailResult(ErrorCode.RMT_RETURN_BLANK_CONTENT, errMsg);
            }
            return procResult.setSuccess(returnStr);
        } catch (Throwable ex) {
            if (exptCounter.shouldPrint()) {
                logger.warn("ConfigManager({}) connect manager({}) exception, params={}",
                        this.callerId, url, params, ex);
            }
            return procResult.setFailResult(ErrorCode.HTTP_VISIT_EXCEPTION, ex.getMessage());
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
                .setSocketTimeout(mgrConfig.getMgrSocketTimeoutMs())
                .setConnectTimeout(mgrConfig.getMgrConnTimeoutMs()).build();
        SSLContext sslContext = SSLContexts.custom().build();
        SSLConnectionSocketFactory sslSf = new SSLConnectionSocketFactory(sslContext,
                new String[]{mgrConfig.getTlsVersion()}, null,
                SSLConnectionSocketFactory.getDefaultHostnameVerifier());
        httpClient = HttpClients.custom().setDefaultHeaders(headers).setDefaultRequestConfig(requestConfig)
                .setSSLSocketFactory(sslSf).build();
        return httpClient;
    }

    private void storeAndBuildMetaConfigure(ProxyClientConfig config) {
        this.mgrConfig = config;
        this.proxyConfigEntry = null;
        this.proxyInfoList.clear();
        this.oldStat = 0;
        this.localMd5 = null;
        this.lstUpdateTime = 0;
        this.userEncryptConfigEntry = null;
        StringBuilder strBuff = new StringBuilder(512);
        this.proxyConfigVisitUrl = strBuff
                .append(mgrConfig.isVisitMgrByHttps() ? SdkConsts.PREFIX_HTTPS : SdkConsts.PREFIX_HTTP)
                .append(mgrConfig.getManagerIP()).append(":").append(mgrConfig.getManagerPort())
                .append(SdkConsts.MANAGER_DATAPROXY_API).append(mgrConfig.getInlongGroupId())
                .toString();
        strBuff.delete(0, strBuff.length());
        this.proxyQueryFailKey = strBuff
                .append("proxy:").append(mgrConfig.getInlongGroupId())
                .append("#").append(mgrConfig.getRegionName())
                .append("#").append(mgrConfig.getDataRptProtocol()).toString();
        strBuff.delete(0, strBuff.length());
        this.localProxyConfigStoreFile = strBuff
                .append(mgrConfig.getMetaStoreBasePath())
                .append(SdkConsts.META_STORE_SUB_DIR)
                .append(mgrConfig.getInlongGroupId())
                .append(SdkConsts.LOCAL_DP_CONFIG_FILE_SUFFIX)
                .toString();
        strBuff.delete(0, strBuff.length());
        this.proxyConfigCacheFile = strBuff
                .append(mgrConfig.getMetaStoreBasePath())
                .append(SdkConsts.META_STORE_SUB_DIR)
                .append(mgrConfig.getInlongGroupId())
                .append(SdkConsts.REMOTE_DP_CACHE_FILE_SUFFIX)
                .toString();
        strBuff.delete(0, strBuff.length());
        this.encryptConfigVisitUrl = mgrConfig.getRptRsaPubKeyUrl();
        this.encryptQueryFailKey = strBuff
                .append("encrypt:").append(mgrConfig.getRptUserName()).toString();
        strBuff.delete(0, strBuff.length());
        this.encryptConfigCacheFile = strBuff
                .append(mgrConfig.getMetaStoreBasePath())
                .append(SdkConsts.META_STORE_SUB_DIR)
                .append(mgrConfig.getRptUserName())
                .append(SdkConsts.REMOTE_ENCRYPT_CACHE_FILE_SUFFIX)
                .toString();
        strBuff.delete(0, strBuff.length());
    }

    private void addAuthorizationInfo(HttpPost httpPost) {
        httpPost.addHeader(BasicAuth.BASIC_AUTH_HEADER,
                BasicAuth.genBasicAuthCredential(mgrConfig.getMgrAuthSecretId(),
                        mgrConfig.getMgrAuthSecretKey()));
    }

    private List<BasicNameValuePair> buildProxyNodeQueryParams() {
        ArrayList<BasicNameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("ip", ProxyUtils.getLocalIp()));
        params.add(new BasicNameValuePair("protocolType", mgrConfig.getDataRptProtocol()));
        return params;
    }

    private List<BasicNameValuePair> buildPubKeyQueryParams() {
        List<BasicNameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("operation", "query"));
        params.add(new BasicNameValuePair("username", mgrConfig.getRptUserName()));
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
        if (mgrConfig.getMetaQryFailCacheExpiredMs() <= 0) {
            return null;
        }
        Tuple2<AtomicLong, String> queryResult;
        if (proxyQry) {
            queryResult = fetchFailProxyMap.get(proxyQueryFailKey);
        } else {
            queryResult = fetchFailEncryptMap.get(encryptQueryFailKey);
        }
        if (queryResult != null
                && (System.currentTimeMillis() - queryResult.getF0().get() < mgrConfig
                        .getMetaQryFailCacheExpiredMs())) {
            return queryResult.getF1();
        }
        return null;
    }

    protected boolean getProxyConfigEntry(boolean fromManager, String strRet, ProcessResult procResult) {
        DataProxyNodeResponse proxyNodeConfig;
        if (fromManager) {
            ProxyClusterConfig clusterConfig;
            try {
                clusterConfig = gson.fromJson(strRet, ProxyClusterConfig.class);
            } catch (Throwable ex) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) parse exception, groupId={}, config={}",
                            this.callerId, mgrConfig.getInlongGroupId(), strRet, ex);
                }
                return procResult.setFailResult(
                        ErrorCode.PARSE_RMT_CONTENT_FAILURE, "parse failure:" + ex.getMessage());
            }
            if (clusterConfig == null) {
                return procResult.setFailResult(ErrorCode.PARSE_RMT_CONTENT_IS_NULL);
            }
            if (!clusterConfig.isSuccess()) {
                return procResult.setFailResult(
                        ErrorCode.RMT_RETURN_ERROR, clusterConfig.getErrMsg());
            }
            if (clusterConfig.getData() == null) {
                return procResult.setFailResult(ErrorCode.META_FIELD_DATA_IS_NULL);
            }
            proxyNodeConfig = clusterConfig.getData();
        } else {
            try {
                proxyNodeConfig = gson.fromJson(strRet, DataProxyNodeResponse.class);
            } catch (Throwable ex) {
                if (parseCounter.shouldPrint()) {
                    logger.warn("ConfigManager({}) parse local file exception, groupId={}, config={}",
                            this.callerId, mgrConfig.getInlongGroupId(), strRet, ex);
                }
                return procResult.setFailResult(
                        ErrorCode.PARSE_FILE_CONTENT_FAILURE, "parse failure:" + ex.getMessage());
            }
            if (proxyNodeConfig == null) {
                return procResult.setFailResult(ErrorCode.PARSE_FILE_CONTENT_IS_NULL);
            }
        }
        // parse nodeList
        List<DataProxyNodeInfo> nodeList = proxyNodeConfig.getNodeList();
        if (CollectionUtils.isEmpty(nodeList)) {
            return procResult.setFailResult(ErrorCode.META_NODE_LIST_IS_EMPTY);
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
                            mgrConfig.getInlongGroupId(), proxy.getId(), proxy.getIp(), proxy.getPort());
                }
                continue;
            }
            tmpHostInfo = new HostInfo(proxy.getIp(), proxy.getPort());
            hostMap.put(tmpHostInfo.getReferenceName(), tmpHostInfo);
        }
        if (hostMap.isEmpty()) {
            return procResult.setFailResult(ErrorCode.NODE_LIST_RECORD_INVALID);
        }
        // parse clusterId
        int clusterId = -1;
        if (ObjectUtils.isNotEmpty(proxyNodeConfig.getClusterId())) {
            clusterId = proxyNodeConfig.getClusterId();
        }
        // parse load
        int load = SdkConsts.LOAD_THRESHOLD;
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
        proxyEntry.setGroupId(mgrConfig.getInlongGroupId());
        proxyEntry.setInterVisit(isIntranet);
        proxyEntry.setHostMap(hostMap);
        proxyEntry.setSwitchStat(isSwitch);
        proxyEntry.setLoad(load);
        proxyEntry.setMaxPacketLength(
                proxyNodeConfig.getMaxPacketLength() != null ? proxyNodeConfig.getMaxPacketLength() : -1);
        return procResult.setSuccess(proxyEntry);
    }
}
