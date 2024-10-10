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
import org.apache.inlong.sdk.dataproxy.ProxyClientConfig;
import org.apache.inlong.sdk.dataproxy.network.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProxyUtils {

    private static final Logger logger = LoggerFactory.getLogger(ProxyUtils.class);
    private static final int TIME_LENGTH = 13;
    private static final Set<String> invalidAttr = new HashSet<>();

    static {
        Collections.addAll(invalidAttr, "groupId", "streamId", "dt", "msgUUID", "cp",
                "cnt", "mt", "m", "sid", "t", "NodeIP", "messageId", "_file_status_check", "_secretId",
                "_signature", "_timeStamp", "_nonce", "_userName", "_clientIP", "_encyVersion", "_encyAesKey",
                "proxySend", "errMsg", "errCode", AttributeConstants.MSG_RPT_TIME);
    }

    public static boolean isAttrKeysValid(Map<String, String> attrsMap) {
        if (attrsMap == null || attrsMap.size() == 0) {
            return false;
        }
        for (String key : attrsMap.keySet()) {
            if (invalidAttr.contains(key)) {
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
     * Check if the body length exceeds the maximum limit, if the maximum limit is less than or equal to 0, it is not checked
     * @param body
     * @param maxLen
     * @return
     */
    public static boolean bodyLengthCheck(byte[] body, int maxLen) {
        // Not valid if the maximum limit is less than or equal to 0
        if (maxLen <= 0) {
            return true;
        }
        if (body.length > maxLen) {
            logger.error("body length is too long, max length is {}", maxLen);
            return false;
        }
        return true;
    }

    /**
     * Check if the total body length exceeds the maximum limit, if the maximum limit is less than or equal to 0, it is not checked
     * @param bodyList
     * @param maxLen
     * @return
     */
    public static boolean bodyLengthCheck(List<byte[]> bodyList, int maxLen) {
        // Not valid if the maximum limit is less than or equal to 0
        if (maxLen <= 0) {
            return true;
        }
        int size = 0;
        for (byte[] body : bodyList) {
            size += body.length;
            if (size > maxLen) {
                logger.error("body length is too long, max length is {}", maxLen);
                return false;
            }
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
     * @param clientConfig
     */
    public static void validClientConfig(ProxyClientConfig clientConfig) {
        if (clientConfig.isNeedAuthentication()) {
            if (Utils.isBlank(clientConfig.getUserName())) {
                throw new IllegalArgumentException("Authentication require userName not Blank!");
            }
            if (Utils.isBlank(clientConfig.getSecretKey())) {
                throw new IllegalArgumentException("Authentication require secretKey not Blank!");
            }
        }
    }
}
