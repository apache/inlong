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

package org.apache.inlong.manager.pojo.util;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BaseException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MySQLSensitiveUrlUtils {

    /**
     * The sensitive param may lead the attack.
     */
    private static final Map<String, String> SENSITIVE_REPLACE_PARAM_MAP = new HashMap<String, String>() {

        {
            put("autoDeserialize", "false");
            put("allowLoadLocalInfile", "false");
            put("allowUrlInLocalInfile", "false");
        }
    };

    private static final Set<String> SENSITIVE_REMOVE_PARAM_MAP = new HashSet<String>() {

        {
            add("allowLoadLocalInfileInPath");
        }
    };

    /**
     * Filter the sensitive params for the given URL.
     *
     * @param url str may have some sensitive params
     * @return str without sensitive param
     */
    public static String filterSensitive(String url) {
        if (StringUtils.isBlank(url)) {
            return url;
        }

        try {
            String resultUrl = url;
            while (resultUrl.contains(InlongConstants.PERCENT)) {
                resultUrl = URLDecoder.decode(resultUrl, "UTF-8");
            }
            resultUrl = resultUrl.replaceAll(InlongConstants.REGEX_WHITESPACE, InlongConstants.EMPTY);

            String sensitiveKey = containSensitiveKey(resultUrl);
            while (StringUtils.isNotBlank(sensitiveKey)) {
                resultUrl = StringUtils.replaceIgnoreCase(resultUrl, sensitiveKey + InlongConstants.EQUAL + "true",
                        InlongConstants.EMPTY);
                resultUrl = StringUtils.replaceIgnoreCase(resultUrl, sensitiveKey + InlongConstants.EQUAL + "yes",
                        InlongConstants.EMPTY);
                sensitiveKey = containSensitiveKey(resultUrl);
            }
            if (resultUrl.contains(InlongConstants.QUESTION_MARK)) {
                StringBuilder builder = new StringBuilder();
                builder.append(StringUtils.substringBefore(resultUrl, InlongConstants.QUESTION_MARK));
                builder.append(InlongConstants.QUESTION_MARK);

                List<String> paramList = new ArrayList<>();
                String queryString = StringUtils.substringAfter(resultUrl, InlongConstants.QUESTION_MARK);
                if (queryString.contains(InlongConstants.SHARP)) {
                    queryString = StringUtils.substringBefore(queryString, InlongConstants.SHARP);
                }
                for (String param : queryString.split(InlongConstants.AMPERSAND)) {
                    if (StringUtils.isBlank(param)) {
                        continue;
                    }
                    String key = StringUtils.substringBefore(param, InlongConstants.EQUAL);
                    String value = StringUtils.substringAfter(param, InlongConstants.EQUAL);

                    if (SENSITIVE_REMOVE_PARAM_MAP.contains(key) || SENSITIVE_REPLACE_PARAM_MAP.containsKey(key)) {
                        continue;
                    }

                    paramList.add(key + InlongConstants.EQUAL + value);
                }
                SENSITIVE_REPLACE_PARAM_MAP.forEach((key, value) -> paramList.add(key + InlongConstants.EQUAL + value));

                String params = StringUtils.join(paramList, InlongConstants.AMPERSAND);
                builder.append(params);
                resultUrl = builder.toString();
            }

            log.info("MySQL original URL {} was replaced to {}", url, resultUrl);
            return resultUrl;
        } catch (Exception e) {
            throw new BaseException(String.format("Failed to filter MySQL sensitive URL: %s, error: %s",
                    url, e.getMessage()));
        }
    }

    public static String containSensitiveKey(String url) {
        for (String key : SENSITIVE_REPLACE_PARAM_MAP.keySet()) {
            if (url.contains(key + InlongConstants.EQUAL + "true")
                    || url.contains(key + InlongConstants.EQUAL + "yes")) {
                return key;
            }
        }
        return null;
    }
}
