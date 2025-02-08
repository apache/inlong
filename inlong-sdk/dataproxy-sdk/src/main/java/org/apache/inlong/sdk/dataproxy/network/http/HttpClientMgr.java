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

package org.apache.inlong.sdk.dataproxy.network.http;

import org.apache.inlong.common.enums.DataProxyErrCode;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.sdk.dataproxy.common.ErrorCode;
import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.config.HostInfo;
import org.apache.inlong.sdk.dataproxy.network.ClientMgr;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpEventInfo;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.utils.HttpUtils;
import org.apache.inlong.sdk.dataproxy.utils.LogCounter;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP Client Manager class
 *
 * Used to manage HTTP clients, including periodically selecting proxy nodes,
 *  finding available nodes when reporting messages, maintaining inflight message
 *  sending status, finding responses to corresponding requests, etc.
 */
public class HttpClientMgr implements ClientMgr {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientMgr.class);
    private static final LogCounter updConExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter sendMsgExptCnt = new LogCounter(10, 100000, 60 * 1000L);
    private static final LogCounter asyncSendExptCnt = new LogCounter(10, 100000, 60 * 1000L);

    private final BaseSender sender;
    private final HttpMsgSenderConfig httpConfig;
    private CloseableHttpClient httpClient;
    private final LinkedBlockingQueue<HttpAsyncObj> messageCache;
    private final Semaphore asyncIdleCellCnt;
    private final ExecutorService workerServices = Executors.newCachedThreadPool();
    private final AtomicBoolean shutDown = new AtomicBoolean(false);
    // meta info
    private ConcurrentHashMap<String, HostInfo> usingNodeMaps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> connFailNodeMap = new ConcurrentHashMap<>();
    // current using nodes
    private List<String> activeNodes = new ArrayList<>();
    private volatile long lastUpdateTime = -1;
    // node select index
    private final AtomicInteger reqSendIndex = new AtomicInteger(0);

    public HttpClientMgr(BaseSender sender, HttpMsgSenderConfig httpConfig) {
        this.sender = sender;
        this.httpConfig = httpConfig;
        this.messageCache = new LinkedBlockingQueue<>(httpConfig.getHttpAsyncRptCacheSize());
        this.asyncIdleCellCnt = new Semaphore(httpConfig.getHttpAsyncRptCacheSize(), true);
    }

    @Override
    public boolean start(ProcessResult procResult) {
        // build http client
        if (!HttpUtils.constructHttpClient(httpConfig.isRptDataByHttps(),
                httpConfig.getHttpSocketTimeoutMs(), httpConfig.getHttpConTimeoutMs(),
                httpConfig.getTlsVersion(), procResult)) {
            return false;
        }
        this.httpClient = (CloseableHttpClient) procResult.getRetData();
        // build async report workers
        for (int i = 0; i < httpConfig.getHttpAsyncRptWorkerNum(); i++) {
            workerServices.execute(new HttpAsyncReportWorker(i));
        }
        logger.info("ClientMgr({}) started!", this.sender.getSenderId());
        return procResult.setSuccess();
    }

    /**
     * close resources
     */
    @Override
    public void stop() {
        if (!this.shutDown.compareAndSet(false, true)) {
            return;
        }
        int remainCnt = 0;
        long stopTime = System.currentTimeMillis();
        logger.info("ClientMgr({}) is closing...", this.sender.getSenderId());
        if (!messageCache.isEmpty()) {
            if (httpConfig.isDiscardHttpCacheWhenClosing()) {
                messageCache.clear();
            } else {
                long startTime = System.currentTimeMillis();
                while (!messageCache.isEmpty()) {
                    if (System.currentTimeMillis() - startTime >= httpConfig.getHttpCloseWaitPeriodMs()) {
                        break;
                    }
                    ProxyUtils.sleepSomeTime(100L);
                }
                remainCnt = messageCache.size();
                messageCache.clear();
            }
        }
        workerServices.shutdown();
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (Throwable ignore) {
                //
            }
        }
        logger.info("ClientMgr({}) stopped, remain ({}) messages discarded, cost {} ms!",
                this.sender.getSenderId(), remainCnt, System.currentTimeMillis() - stopTime);
    }

    @Override
    public int getInflightMsgCnt() {
        return this.messageCache.size();
    }

    @Override
    public int getActiveNodeCnt() {
        return activeNodes.size();
    }

    @Override
    public void updateProxyInfoList(boolean nodeChanged, ConcurrentHashMap<String, HostInfo> hostInfoMap) {
        if (hostInfoMap.isEmpty() || this.shutDown.get()) {
            return;
        }
        long curTime = System.currentTimeMillis();
        try {
            // shuffle candidate nodes
            List<HostInfo> candidateNodes = new ArrayList<>(hostInfoMap.size());
            candidateNodes.addAll(hostInfoMap.values());
            Collections.sort(candidateNodes);
            Collections.shuffle(candidateNodes);
            int curTotalCnt = candidateNodes.size();
            int needActiveCnt = Math.min(httpConfig.getAliveConnections(), curTotalCnt);
            // build next step nodes
            Long lstFailTime;
            int maxCycleCnt = 3;
            this.connFailNodeMap.clear();
            List<String> realHosts = new ArrayList<>();
            ConcurrentHashMap<String, HostInfo> tmpNodeMaps = new ConcurrentHashMap<>();
            do {
                int selectCnt = 0;
                long selectTime = System.currentTimeMillis();
                for (HostInfo hostInfo : candidateNodes) {
                    if (realHosts.contains(hostInfo.getReferenceName())) {
                        continue;
                    }
                    lstFailTime = this.connFailNodeMap.get(hostInfo.getReferenceName());
                    if (lstFailTime != null
                            && selectTime - lstFailTime <= httpConfig.getHttpNodeReuseWaitIfFailMs()) {
                        continue;
                    }
                    tmpNodeMaps.put(hostInfo.getReferenceName(), hostInfo);
                    realHosts.add(hostInfo.getReferenceName());
                    if (lstFailTime != null) {
                        this.connFailNodeMap.remove(hostInfo.getReferenceName());
                    }
                    if (++selectCnt >= needActiveCnt) {
                        break;
                    }
                }
                if (!realHosts.isEmpty()) {
                    break;
                }
                ProxyUtils.sleepSomeTime(1000L);
            } while (--maxCycleCnt > 0);
            // update active nodes
            if (realHosts.isEmpty()) {
                if (nodeChanged) {
                    logger.error("ClientMgr({}) changed nodes, but all nodes failure, nodes={}, failNodes={}!",
                            this.sender.getSenderId(), candidateNodes, connFailNodeMap);
                } else {
                    logger.error("ClientMgr({}) re-choose nodes, but all nodes failure, nodes={}, failNodes={}!",
                            this.sender.getSenderId(), candidateNodes, connFailNodeMap);
                }
            } else {
                this.lastUpdateTime = System.currentTimeMillis();
                this.usingNodeMaps = tmpNodeMaps;
                this.activeNodes = realHosts;
                if (nodeChanged) {
                    logger.info("ClientMgr({}) changed nodes, wast {}ms, nodeCnt=(r:{}-a:{}), actives={}, fail={}",
                            this.sender.getSenderId(), (System.currentTimeMillis() - curTime),
                            needActiveCnt, realHosts.size(), realHosts, connFailNodeMap.keySet());
                } else {
                    logger.info("ClientMgr({}) re-choose nodes, wast {}ms, nodeCnt=(r:{}-a:{}), actives={}, fail={}",
                            this.sender.getSenderId(), (System.currentTimeMillis() - curTime),
                            needActiveCnt, realHosts.size(), realHosts, connFailNodeMap.keySet());
                }
            }
        } catch (Throwable ex) {
            if (updConExptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) update nodes throw exception",
                        this.sender.getSenderId(), ex);
            }
        }
    }

    public boolean asyncSendMessage(HttpAsyncObj asyncObj, ProcessResult procResult) {
        if (this.shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        List<String> curNodes = this.activeNodes;
        if (curNodes.isEmpty()) {
            return procResult.setFailResult(ErrorCode.EMPTY_ACTIVE_NODE_SET);
        }
        if (!this.asyncIdleCellCnt.tryAcquire()) {
            return procResult.setFailResult(ErrorCode.HTTP_ASYNC_POOL_FULL);
        }
        boolean released = false;
        try {
            if (!this.messageCache.offer(asyncObj)) {
                this.asyncIdleCellCnt.release();
                released = true;
                return procResult.setFailResult(ErrorCode.HTTP_ASYNC_OFFER_FAIL);
            }
            return procResult.setSuccess();
        } catch (Throwable ex) {
            if (!released) {
                this.asyncIdleCellCnt.release();
            }
            if (asyncSendExptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) async offer event exception", this.sender.getSenderId(), ex);
            }
            return procResult.setFailResult(ErrorCode.HTTP_ASYNC_OFFER_EXCEPTION, ex.getMessage());
        }
    }

    /**
     * send message to remote nodes
     */
    public boolean sendMessage(HttpEventInfo httpEvent, ProcessResult procResult) {
        if (this.shutDown.get()) {
            return procResult.setFailResult(ErrorCode.SDK_CLOSED);
        }
        List<String> curNodes = this.activeNodes;
        int curNodeSize = curNodes.size();
        if (curNodeSize == 0) {
            return procResult.setFailResult(ErrorCode.EMPTY_ACTIVE_NODE_SET);
        }
        String curNode;
        HostInfo hostInfo;
        Long lstFailTime;
        int nullNodeCnt = 0;
        HostInfo back1thNode = null;
        long nodeSelectTime = System.currentTimeMillis();
        int startPos = reqSendIndex.getAndIncrement();
        for (int index = 0; index < curNodeSize; index++) {
            curNode = curNodes.get(Math.abs(startPos++) % curNodeSize);
            hostInfo = usingNodeMaps.get(curNode);
            if (hostInfo == null) {
                nullNodeCnt++;
                continue;
            }
            lstFailTime = connFailNodeMap.get(hostInfo.getReferenceName());
            if (lstFailTime != null) {
                if (nodeSelectTime - lstFailTime <= httpConfig.getHttpNodeReuseWaitIfFailMs()) {
                    back1thNode = hostInfo;
                    continue;
                }
                connFailNodeMap.remove(hostInfo.getReferenceName(), lstFailTime);
            }
            return innSendMsgByHttp(httpEvent, hostInfo, procResult);
        }
        if (nullNodeCnt == curNodeSize) {
            return procResult.setFailResult(ErrorCode.EMPTY_ACTIVE_NODE_SET);
        }
        if (back1thNode != null) {
            return innSendMsgByHttp(httpEvent, back1thNode, procResult);
        }
        return procResult.setFailResult(ErrorCode.NO_VALID_REMOTE_NODE);
    }

    /**
     * send request to DataProxy over http
     */
    private boolean innSendMsgByHttp(HttpEventInfo httpEvent, HostInfo hostInfo, ProcessResult procResult) {
        String rmtRptUrl = (httpConfig.isRptDataByHttps() ? SdkConsts.PREFIX_HTTPS : SdkConsts.PREFIX_HTTP)
                + hostInfo.getReferenceName()
                + SdkConsts.DATAPROXY_REPORT_METHOD;
        if (!buildFormUrlPost(rmtRptUrl, httpEvent, procResult)) {
            return false;
        }
        HttpPost httpPost = (HttpPost) procResult.getRetData();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            String returnStr = EntityUtils.toString(response.getEntity());
            int returnCode = response.getStatusLine().getStatusCode();
            if (HttpStatus.SC_OK != returnCode) {
                if (sendMsgExptCnt.shouldPrint()) {
                    logger.warn("ClientMgr({}) report event failure, errCode={}, returnStr={}",
                            this.sender.getSenderId(), returnCode, returnStr);
                }
                if (response.getStatusLine().getStatusCode() >= 500) {
                    this.connFailNodeMap.put(hostInfo.getReferenceName(), System.currentTimeMillis());
                }
                return procResult.setFailResult(ErrorCode.RMT_RETURN_FAILURE,
                        response.getStatusLine().getStatusCode() + ":" + returnStr);
            }
            if (StringUtils.isBlank(returnStr)) {
                return procResult.setFailResult(ErrorCode.RMT_RETURN_BLANK_CONTENT);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("success to report event, url={}, result={}",
                        rmtRptUrl, returnStr);
            }
            JsonObject jsonResponse = JsonParser.parseString(returnStr).getAsJsonObject();
            JsonElement codeElement = jsonResponse.get("code");
            JsonElement msgElement = jsonResponse.get("msg");
            if (codeElement != null) {
                int errCode = codeElement.getAsInt();
                if (errCode == DataProxyErrCode.SUCCESS.getErrCode()) {
                    return procResult.setSuccess();
                } else {
                    return procResult.setFailResult(ErrorCode.DP_RETURN_FAILURE,
                            errCode + ":" + (msgElement != null ? msgElement.getAsString() : ""));
                }
            }
            return procResult.setFailResult(ErrorCode.DP_RETURN_UNKNOWN_ERROR, returnStr);
        } catch (Throwable ex) {
            if (sendMsgExptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) report event exception, url={}",
                        this.sender.getSenderId(), rmtRptUrl, ex);
            }
            return procResult.setFailResult(ErrorCode.HTTP_VISIT_DP_EXCEPTION, ex.getMessage());
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            if (response != null) {
                try {
                    response.close();
                } catch (Throwable ex) {
                    if (sendMsgExptCnt.shouldPrint()) {
                        logger.warn("ClientMgr({}) close response exception, url={}",
                                this.sender.getSenderId(), rmtRptUrl, ex);
                    }
                }
            }
        }
    }

    private boolean buildFormUrlPost(
            String rmtRptUrl, HttpEventInfo httpEvent, ProcessResult procResult) {
        ArrayList<BasicNameValuePair> contents = new ArrayList<>();
        try {
            HttpPost httpPost = new HttpPost(rmtRptUrl);
            httpPost.setHeader(HttpHeaders.CONNECTION,
                    HttpHeaderValues.CLOSE.toString());
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE,
                    HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED.toString());
            contents.add(new BasicNameValuePair(AttributeConstants.GROUP_ID,
                    httpEvent.getGroupId()));
            contents.add(new BasicNameValuePair(AttributeConstants.STREAM_ID,
                    httpEvent.getStreamId()));
            contents.add(new BasicNameValuePair(AttributeConstants.DATA_TIME,
                    String.valueOf(httpEvent.getDtMs())));
            contents.add(new BasicNameValuePair(SdkConsts.KEY_HTTP_FIELD_BODY,
                    StringUtils.join(httpEvent.getBodyList(), httpConfig.getHttpEventsSeparator())));
            contents.add(new BasicNameValuePair(AttributeConstants.MESSAGE_COUNT,
                    String.valueOf(httpEvent.getMsgCnt())));
            if (!httpConfig.isSepEventByLF()) {
                contents.add(new BasicNameValuePair(SdkConsts.KEY_HTTP_FIELD_DELIMITER,
                        httpConfig.getHttpEventsSeparator()));
            }
            String encodedContents = URLEncodedUtils.format(contents, StandardCharsets.UTF_8);
            httpPost.setEntity(new StringEntity(encodedContents));
            if (logger.isDebugEnabled()) {
                logger.debug("begin to post request to {}, encoded content is: {}",
                        rmtRptUrl, encodedContents);
            }
            return procResult.setSuccess(httpPost);
        } catch (Throwable ex) {
            if (sendMsgExptCnt.shouldPrint()) {
                logger.warn("ClientMgr({}) build form-url content failure, content={}",
                        this.sender.getSenderId(), contents, ex);
            }
            return procResult.setFailResult(ErrorCode.BUILD_FORM_CONTENT_EXCEPTION, ex.getMessage());
        }
    }

    /**
     * check cache runner
     */
    private class HttpAsyncReportWorker implements Runnable {

        private final String workerId;

        public HttpAsyncReportWorker(int workerId) {
            this.workerId = sender.getSenderId() + "-" + workerId;
        }

        @Override
        public void run() {
            long curTime = 0;
            HttpAsyncObj asyncObj;
            ProcessResult procResult = new ProcessResult();
            logger.info("HttpAsyncReportWorker({}) started", this.workerId);
            // if not shutdown or queue is not empty
            while (!shutDown.get() || !messageCache.isEmpty()) {
                while (!messageCache.isEmpty()) {
                    asyncObj = messageCache.poll();
                    if (asyncObj == null) {
                        continue;
                    }
                    try {
                        sendMessage(asyncObj.getHttpEvent(), procResult);
                        curTime = System.currentTimeMillis();
                        asyncObj.getCallback().onMessageAck(procResult);
                    } catch (Throwable ex) {
                        if (asyncSendExptCnt.shouldPrint()) {
                            logger.error("HttpAsync({}) report event exception", workerId, ex);
                        }
                    } finally {
                        asyncIdleCellCnt.release();
                        if (procResult.isSuccess()) {
                            sender.getMetricHolder().addCallbackSucMetric(asyncObj.getHttpEvent().getGroupId(),
                                    asyncObj.getHttpEvent().getStreamId(), asyncObj.getHttpEvent().getMsgCnt(),
                                    (curTime - asyncObj.getRptMs()), (System.currentTimeMillis() - curTime));
                        } else {
                            sender.getMetricHolder().addCallbackFailMetric(procResult.getErrCode(),
                                    asyncObj.getHttpEvent().getGroupId(), asyncObj.getHttpEvent().getStreamId(),
                                    asyncObj.getHttpEvent().getMsgCnt(), (System.currentTimeMillis() - curTime));
                        }
                    }
                }
                ProxyUtils.sleepSomeTime(httpConfig.getHttpAsyncWorkerIdleWaitMs());
            }
            logger.info("HttpAsyncReportWorker({}) stopped", this.workerId);
        }
    }
}