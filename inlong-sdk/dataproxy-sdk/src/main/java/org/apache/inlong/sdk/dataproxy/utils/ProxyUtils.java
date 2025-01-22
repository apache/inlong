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

package org.apache.inlong.sdk.dataproxy.utils;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.common.msg.MsgType;
import org.apache.inlong.sdk.dataproxy.common.SdkConsts;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ProxyUtils {

    public static final String KEY_FILE_STATUS_CHECK = "_file_status_check";
    public static final String KEY_SECRET_ID = "_secretId";
    public static final String KEY_SIGNATURE = "_signature";
    public static final String KEY_TIME_STAMP = "_timeStamp";
    public static final String KEY_NONCE = "_nonce";
    public static final String KEY_USERNAME = "_userName";
    public static final String KEY_CLIENT_IP = "_clientIP";
    public static final String KEY_ENCY_VERSION = "_encyVersion";
    public static final String KEY_ENCY_AES_KEY = "_encyAesKey";
    private static final Logger logger = LoggerFactory.getLogger(ProxyUtils.class);
    private static final LogCounter exceptCounter = new LogCounter(10, 200000, 60 * 1000L);

    private static final int TIME_LENGTH = 13;
    public static final Set<String> SdkReservedWords = new HashSet<>();
    public static final Set<MsgType> SdkAllowedMsgType = new HashSet<>();
    private static String localHost;
    private static String sdkVersion;
    private static Integer sdkProcessId;

    static {
        getLocalIp();
        getProcessPid();
        getJarVersion();
        Collections.addAll(SdkReservedWords,
                AttributeConstants.GROUP_ID, AttributeConstants.STREAM_ID,
                AttributeConstants.DATA_TIME, AttributeConstants.MSG_UUID,
                AttributeConstants.COMPRESS_TYPE, AttributeConstants.MESSAGE_COUNT,
                AttributeConstants.MESSAGE_TYPE, AttributeConstants.METHOD,
                AttributeConstants.SEQUENCE_ID, AttributeConstants.TIME_STAMP,
                AttributeConstants.NODE_IP, AttributeConstants.MESSAGE_ID,
                AttributeConstants.MESSAGE_IS_ACK, AttributeConstants.MESSAGE_PROXY_SEND,
                AttributeConstants.MESSAGE_PROCESS_ERRCODE, AttributeConstants.MESSAGE_PROCESS_ERRMSG,
                AttributeConstants.MSG_RPT_TIME, AttributeConstants.PROXY_SDK_VERSION,
                KEY_FILE_STATUS_CHECK, KEY_SECRET_ID, KEY_SIGNATURE, KEY_TIME_STAMP,
                KEY_NONCE, KEY_USERNAME, KEY_CLIENT_IP, KEY_ENCY_VERSION, KEY_ENCY_AES_KEY);
        /*
         * Collections.addAll(SdkReservedWords, "groupId", "streamId", "dt", "msgUUID", "cp", "cnt", "mt", "m", "sid",
         * "t", "NodeIP", "messageId", "isAck", "proxySend", "errCode", "errMsg", "rtms", "sdkVersion",
         * "_file_status_check", "_secretId", "_signature", "_timeStamp", "_nonce", "_userName", "_clientIP",
         * "_encyVersion", "_encyAesKey");
         */

        Collections.addAll(SdkAllowedMsgType,
                MsgType.MSG_ACK_SERVICE, MsgType.MSG_MULTI_BODY, MsgType.MSG_BIN_MULTI_BODY);
    }

    public static String getLocalIp() {
        if (localHost != null) {
            return localHost;
        }
        String ip = "127.0.0.1";
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        } catch (Throwable ex) {
            if (exceptCounter.shouldPrint()) {
                logger.error("DataProxy-SDK get local IP failure", ex);
            }
        }
        localHost = ip;
        return ip;
    }

    public static String getJarVersion() {
        if (sdkVersion != null) {
            return sdkVersion;
        }
        Properties properties = new Properties();
        try (InputStream is = ProxyUtils.class.getResourceAsStream("/git.properties")) {
            properties.load(is);
            sdkVersion = properties.getProperty("git.build.version");
        } catch (Throwable ex) {
            if (exceptCounter.shouldPrint()) {
                logger.error("DataProxy-SDK get version failure", ex);
            }
        }
        return sdkVersion;
    }

    public static Integer getProcessPid() {
        if (sdkProcessId != null) {
            return sdkProcessId;
        }
        try {
            String processName = ManagementFactory.getRuntimeMXBean().getName();
            sdkProcessId = Integer.parseInt(processName.split("@")[0]);
        } catch (Throwable ex) {
            if (exceptCounter.shouldPrint()) {
                logger.error("DataProxy-SDK get process ID failure", ex);
            }
        }
        return sdkProcessId;
    }

    public static boolean sleepSomeTime(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }

    public static String buildClusterIdKey(String protocol, String regionName, Integer clusterId) {
        return clusterId + ":" + regionName + ":" + protocol;
    }

    public static String buildGroupIdConfigKey(String protocol, String regionName, String groupId) {
        return protocol + ":" + regionName + ":" + groupId;
    }

    public static boolean isAttrKeysValid(Map<String, String> attrsMap) {
        if (attrsMap == null || attrsMap.size() == 0) {
            return false;
        }
        for (String key : attrsMap.keySet()) {
            if (SdkReservedWords.contains(key)) {
                logger.error("the attributes is invalid ,please check ! {}", key);
                return false;
            }
        }
        return true;
    }

    public static boolean isDtValid(long dt) {
        if (String.valueOf(dt).length() != TIME_LENGTH) {
            logger.error("dt {} is error", dt);
            return false;
        }
        return true;
    }

    /**
     * check body valid
     *
     * @param body
     * @return
     */
    public static boolean isBodyValid(byte[] body) {
        if (body == null || body.length == 0) {
            logger.error("body is error {}", body);
            return false;
        }
        return true;
    }

    /**
     * check body valid
     *
     * @param bodyList
     * @return
     */
    public static boolean isBodyValid(List<byte[]> bodyList) {
        if (bodyList == null || bodyList.size() == 0) {
            logger.error("body  is error");
            return false;
        }
        return true;
    }

    /**
     * Check if the body length exceeds the maximum limit, if the maximum limit is less than 0, it is not checked
     * @param body
     * @param maxLen
     * @return
     */
    public static boolean isBodyLengthValid(byte[] body, int maxLen) {
        // Not valid if the maximum limit is less than or equal to 0
        if (maxLen < 0) {
            return true;
        }
        // Reserve space for attribute
        if (body.length > maxLen - SdkConsts.RESERVED_ATTRIBUTE_LENGTH) {
            logger.debug("body length({}) > max length({}) - fixed attribute length({})",
                    body.length, maxLen, SdkConsts.RESERVED_ATTRIBUTE_LENGTH);
            return false;
        }
        return true;
    }

    /**
     * Check if the total body length exceeds the maximum limit, if the maximum limit is less than 0, it is not checked
     * @param bodyList
     * @param maxLen
     * @return
     */
    public static boolean isBodyLengthValid(List<byte[]> bodyList, int maxLen) {
        // Not valid if the maximum limit is less than or equal to 0
        if (maxLen < 0) {
            return true;
        }
        int size = 0;
        for (byte[] body : bodyList) {
            size += body.length;
        }
        // Reserve space for attribute
        if (size > maxLen - SdkConsts.RESERVED_ATTRIBUTE_LENGTH) {
            logger.debug("bodyList length({}) > max length({}) - fixed attribute length({})",
                    size, maxLen, SdkConsts.RESERVED_ATTRIBUTE_LENGTH);
            return false;
        }
        return true;
    }

    public static long covertZeroDt(long dt) {
        if (dt == 0) {
            return System.currentTimeMillis();
        }
        return dt;
    }

    /**
     * valid client config
     *
     * @param tcpConfig
     */
    public static void validClientConfig(TcpMsgSenderConfig tcpConfig) {
        if (tcpConfig.isEnableMgrAuthz()) {
            if (StringUtils.isBlank(tcpConfig.getMgrAuthSecretId())) {
                throw new IllegalArgumentException("Authentication require secretId not Blank!");
            }
            if (StringUtils.isBlank(tcpConfig.getMgrAuthSecretKey())) {
                throw new IllegalArgumentException("Authentication require secretKey not Blank!");
            }
        }
    }
}
