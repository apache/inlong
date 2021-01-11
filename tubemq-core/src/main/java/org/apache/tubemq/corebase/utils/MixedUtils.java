/*
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

package org.apache.tubemq.corebase.utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.tubemq.corebase.TokenConstants;



public class MixedUtils {
    // java version cache
    private static String javaVersion = "";

    static {
        javaVersion = System.getProperty("java.version");
    }

    public static String getJavaVersion() {
        if (TStringUtils.isEmpty(javaVersion)) {
            return "";
        } else {
            int maxLen = Math.min(javaVersion.length(), 100);
            return javaVersion.substring(0, maxLen);
        }
    }

    /**
     * parse topic Parameter with format topic_1[,topic_2[:filterCond_2.1[;filterCond_2.2]]]
     *  topicParam->set(filterCond) map
     * @param topicParam - composite string
     * @return - map of topic->set(filterCond)
     */
    public static Map<String, TreeSet<String>> parseTopicParam(String topicParam) {
        Map<String, TreeSet<String>> topicAndFiltersMap = new HashMap<>();
        if (TStringUtils.isBlank(topicParam)) {
            return topicAndFiltersMap;
        }
        String[] topicFilterStrs = topicParam.split(TokenConstants.ARRAY_SEP);
        for (String topicFilterStr : topicFilterStrs) {
            if (TStringUtils.isBlank(topicFilterStr)) {
                continue;
            }
            String[] topicFilters = topicFilterStr.split(TokenConstants.ATTR_SEP);
            if (TStringUtils.isBlank(topicFilters[0])) {
                continue;
            }
            TreeSet<String> filterSet = new TreeSet<>();
            if (topicFilters.length > 1
                    && TStringUtils.isNotBlank(topicFilters[1])) {
                String[] filterItems = topicFilters[1].split(TokenConstants.LOG_SEG_SEP);
                for (String filterItem : filterItems) {
                    if (TStringUtils.isBlank(filterItem)) {
                        continue;
                    }
                    filterSet.add(filterItem.trim());
                }
            }
            topicAndFiltersMap.put(topicFilters[0].trim(), filterSet);
        }
        return topicAndFiltersMap;
    }

    public static byte[] buildTestData(int bodySize) {
        final byte[] transmitData =
                StringUtils.getBytesUtf8("This is a test data!");
        final ByteBuffer dataBuffer = ByteBuffer.allocate(bodySize);
        while (dataBuffer.hasRemaining()) {
            int offset = dataBuffer.arrayOffset();
            dataBuffer.put(transmitData, offset,
                    Math.min(dataBuffer.remaining(), transmitData.length));
        }
        dataBuffer.flip();
        return dataBuffer.array();
    }

    // get the middle data between min, max, and data
    public static int mid(int data, int min, int max) {
        return Math.max(min, Math.min(max, data));
    }

    public static long mid(long data, long min, long max) {
        return Math.max(min, Math.min(max, data));
    }

}
