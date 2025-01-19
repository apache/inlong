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

package org.apache.inlong.sdk.dataproxy.http;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.sdk.dataproxy.common.HttpContentType;
import org.apache.inlong.sdk.dataproxy.common.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.common.ReportProtocol;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * HTTP Message Sender configuration:
 *
 * Used to define the setting related to the sender reporting data using the HTTP protocol, including
 * whether to use HTTPS for reporting, the separator character between message bodies when
 *  reporting multiple messages, HTTP timeout setting, message queue during HTTP asynchronous sending,
 *  and the number of sending threads, etc.
 */
public class HttpMsgSenderConfig extends ProxyClientConfig implements Cloneable {

    // whether report data over https
    private boolean rptDataByHttps = false;
    // report data content type
    private HttpContentType httpContentType = HttpContentType.APPLICATION_X_WWW_FORM_URLENCODED;
    // http events separator
    private String httpEventsSeparator = AttributeConstants.LINE_FEED_SEP;
    // whether separate event by line feed
    private boolean sepEventByLF = true;
    // connect timeout in milliseconds
    private int httpConTimeoutMs = SdkConsts.VAL_DEF_HTTP_CONNECT_TIMEOUT_MS;
    // socket timeout in milliseconds
    private int httpSocketTimeoutMs = SdkConsts.VAL_DEF_NODE_SOCKET_TIMEOUT_MS;
    // discard cache msg when closing
    private boolean discardHttpCacheWhenClosing = false;
    // http close wait period ms
    private long httpCloseWaitPeriodMs = SdkConsts.VAL_DEF_HTTP_SDK_CLOSE_WAIT_MS;
    // node reuse freezing time
    private long httpNodeReuseWaitIfFailMs = SdkConsts.VAL_DEF_HTTP_REUSE_FAIL_WAIT_MS;
    // message cache size for async report data.
    private int httpAsyncRptCacheSize = SdkConsts.VAL_DEF_HTTP_ASYNC_RPT_CACHE_SIZE;
    // thread number for async report data.
    private int httpAsyncRptWorkerNum = SdkConsts.VAL_DEF_HTTP_ASYNC_RPT_WORKER_NUM;
    // interval for async worker in microseconds.
    private int httpAsyncWorkerIdleWaitMs = SdkConsts.VAL_DEF_HTTP_ASYNC_WORKER_IDLE_WAIT_MS;

    public HttpMsgSenderConfig(boolean visitMgrByHttps,
            String managerIP, int managerPort, String groupId) throws ProxySdkException {
        super(visitMgrByHttps, managerIP, managerPort, groupId, ReportProtocol.HTTP, null);
    }

    public HttpMsgSenderConfig(String managerAddress, String groupId) throws ProxySdkException {
        super(managerAddress, groupId, ReportProtocol.HTTP, null);
    }

    public HttpMsgSenderConfig(boolean visitMgrByHttps, String managerIP, int managerPort,
            String groupId, String mgrAuthSecretId, String mgrAuthSecretKey) throws ProxySdkException {
        super(visitMgrByHttps, managerIP, managerPort, groupId, ReportProtocol.HTTP, null);
        this.setMgrAuthzInfo(true, mgrAuthSecretId, mgrAuthSecretKey);
    }

    public HttpMsgSenderConfig(String managerAddress,
            String groupId, String mgrAuthSecretId, String mgrAuthSecretKey) throws ProxySdkException {
        super(managerAddress, groupId, ReportProtocol.HTTP, null);
        this.setMgrAuthzInfo(true, mgrAuthSecretId, mgrAuthSecretKey);
    }

    public boolean isRptDataByHttps() {
        return rptDataByHttps;
    }

    public void setRptDataByHttps(boolean rptDataByHttps) {
        this.rptDataByHttps = rptDataByHttps;
    }

    public HttpContentType getContentType() {
        return httpContentType;
    }

    public void setContentType(HttpContentType httpContentType) {
        if (httpContentType == null) {
            return;
        }
        this.httpContentType = httpContentType;
    }

    public String getHttpEventsSeparator() {
        return httpEventsSeparator;
    }

    public boolean isSepEventByLF() {
        return sepEventByLF;
    }

    public void setHttpEventsSeparator(String httpEventsSeparator) {
        if (StringUtils.isBlank(httpEventsSeparator)) {
            throw new IllegalArgumentException("eventSeparator cannot be blank");
        }
        String tmpValue = httpEventsSeparator.trim();
        if (tmpValue.length() != 1) {
            throw new IllegalArgumentException("eventSeparator must be a single character");
        }
        this.httpEventsSeparator = tmpValue;
        this.sepEventByLF = this.httpEventsSeparator.equals(AttributeConstants.LINE_FEED_SEP);
    }

    public int getHttpConTimeoutMs() {
        return httpConTimeoutMs;
    }

    public void setHttpConTimeoutMs(int httpConTimeoutMs) {
        this.httpConTimeoutMs = Math.max(
                SdkConsts.VAL_MIN_CONNECT_TIMEOUT_MS, httpConTimeoutMs);
    }

    public int getHttpSocketTimeoutMs() {
        return httpSocketTimeoutMs;
    }

    public void setHttpSocketTimeoutMs(int httpSocketTimeoutMs) {
        this.httpSocketTimeoutMs =
                Math.min(SdkConsts.VAL_MAX_SOCKET_TIMEOUT_MS,
                        Math.max(SdkConsts.VAL_MIN_SOCKET_TIMEOUT_MS, httpSocketTimeoutMs));
    }

    public boolean isDiscardHttpCacheWhenClosing() {
        return discardHttpCacheWhenClosing;
    }

    public void setDiscardHttpCacheWhenClosing(boolean discardHttpCacheWhenClosing) {
        this.discardHttpCacheWhenClosing = discardHttpCacheWhenClosing;
    }

    public long getHttpCloseWaitPeriodMs() {
        return httpCloseWaitPeriodMs;
    }

    public void setHttpCloseWaitPeriodMs(long httpCloseWaitPeriodMs) {
        this.httpCloseWaitPeriodMs = httpCloseWaitPeriodMs;
        this.httpCloseWaitPeriodMs =
                Math.min(SdkConsts.VAL_MAX_HTTP_SDK_CLOSE_WAIT_MS,
                        Math.max(SdkConsts.VAL_MIN_HTTP_SDK_CLOSE_WAIT_MS, httpCloseWaitPeriodMs));
    }

    public long getHttpNodeReuseWaitIfFailMs() {
        return httpNodeReuseWaitIfFailMs;
    }

    public void setHttpNodeReuseWaitIfFailMs(long httpNodeReuseWaitIfFailMs) {
        this.httpNodeReuseWaitIfFailMs =
                Math.min(SdkConsts.VAL_MAX_HTTP_REUSE_FAIL_WAIT_MS,
                        Math.max(SdkConsts.VAL_MIN_HTTP_REUSE_FAIL_WAIT_MS, httpNodeReuseWaitIfFailMs));
    }

    public int getHttpAsyncRptCacheSize() {
        return httpAsyncRptCacheSize;
    }

    public int getHttpAsyncRptWorkerNum() {
        return httpAsyncRptWorkerNum;
    }

    public void setHttpAsyncRptPoolConfig(int httpAsyncRptCacheSize, int httpAsyncRptWorkerNum) {
        this.httpAsyncRptCacheSize =
                Math.max(SdkConsts.VAL_MIN_HTTP_ASYNC_RPT_CACHE_SIZE, httpAsyncRptCacheSize);
        this.httpAsyncRptWorkerNum =
                Math.max(SdkConsts.VAL_MIN_HTTP_ASYNC_RPT_WORKER_NUM, httpAsyncRptWorkerNum);
    }

    public int getHttpAsyncWorkerIdleWaitMs() {
        return httpAsyncWorkerIdleWaitMs;
    }

    public void setHttpAsyncWorkerIdleWaitMs(int httpAsyncWorkerIdleWaitMs) {
        this.httpAsyncWorkerIdleWaitMs =
                Math.min(SdkConsts.VAL_MAX_HTTP_ASYNC_WORKER_IDLE_WAIT_MS,
                        Math.max(SdkConsts.VAL_MIN_HTTP_ASYNC_WORKER_IDLE_WAIT_MS, httpAsyncWorkerIdleWaitMs));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        HttpMsgSenderConfig that = (HttpMsgSenderConfig) o;
        return rptDataByHttps == that.rptDataByHttps
                && sepEventByLF == that.sepEventByLF
                && httpConTimeoutMs == that.httpConTimeoutMs
                && httpSocketTimeoutMs == that.httpSocketTimeoutMs
                && discardHttpCacheWhenClosing == that.discardHttpCacheWhenClosing
                && httpCloseWaitPeriodMs == that.httpCloseWaitPeriodMs
                && httpNodeReuseWaitIfFailMs == that.httpNodeReuseWaitIfFailMs
                && httpAsyncRptCacheSize == that.httpAsyncRptCacheSize
                && httpAsyncRptWorkerNum == that.httpAsyncRptWorkerNum
                && httpAsyncWorkerIdleWaitMs == that.httpAsyncWorkerIdleWaitMs
                && httpContentType == that.httpContentType
                && Objects.equals(httpEventsSeparator, that.httpEventsSeparator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rptDataByHttps, httpContentType,
                httpEventsSeparator, sepEventByLF, httpConTimeoutMs, httpSocketTimeoutMs,
                discardHttpCacheWhenClosing, httpCloseWaitPeriodMs, httpNodeReuseWaitIfFailMs,
                httpAsyncRptCacheSize, httpAsyncRptWorkerNum, httpAsyncWorkerIdleWaitMs);
    }

    @Override
    public HttpMsgSenderConfig clone() {
        try {
            HttpMsgSenderConfig copy = (HttpMsgSenderConfig) super.clone();
            if (copy != null) {
                copy.httpContentType = this.httpContentType;
            }
            return copy;
        } catch (Throwable ex) {
            logger.warn("Failed to clone HttpMsgSenderConfig", ex);
            return null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder strBuff =
                new StringBuilder("HttpMsgSenderConfig{rptDataByHttps=").append(rptDataByHttps)
                        .append(", httpContentType=").append(httpContentType)
                        .append(", httpEventsSeparator='").append(httpEventsSeparator)
                        .append("', sepEventByLF=").append(sepEventByLF)
                        .append(", httpConTimeoutMs=").append(httpConTimeoutMs)
                        .append(", httpSocketTimeoutMs=").append(httpSocketTimeoutMs)
                        .append(", discardHttpCacheWhenClosing=").append(discardHttpCacheWhenClosing)
                        .append(", httpCloseWaitPeriodMs=").append(httpCloseWaitPeriodMs)
                        .append(", httpNodeReuseWaitIfFailMs=").append(httpNodeReuseWaitIfFailMs)
                        .append(", httpAsyncRptCacheSize=").append(httpAsyncRptCacheSize)
                        .append(", httpAsyncRptWorkerNum=").append(httpAsyncRptWorkerNum)
                        .append(", httpAsyncWorkerIdleWaitMs=").append(httpAsyncWorkerIdleWaitMs);
        return super.getSetting(strBuff);
    }
}
