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

package org.apache.inlong.agent.plugin.task.cos;

import org.apache.inlong.agent.plugin.utils.regex.PatternUtil;
import org.apache.inlong.agent.plugin.utils.regex.Scanner;
import org.apache.inlong.agent.plugin.utils.regex.Scanner.FinalPatternInfo;
import org.apache.inlong.agent.utils.DateTransUtils;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * This class is mainly used for scanning log file that we want to read. We use this class at
 * inlong_agent recover process, the do and redo tasks and the current log file access when we deploy a
 * new data source.
 */
public class FileScanner {

    public static final int DEFAULT_KEY_COUNT = 100;
    public static final String DEFAULT_DELIMITER = "/";
    public static final char PATH_SEP = '/';

    public static class BasicFileInfo {

        public String fileName;
        public String dataTime;

        public BasicFileInfo(String fileName, String dataTime) {
            this.fileName = fileName;
            this.dataTime = dataTime;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(FileScanner.class);

    public static List<BasicFileInfo> scanTaskBetweenTimes(COSClient cosClient, String bucketName, String originPattern,
            String cycleUnit, String timeOffset, long startTime, long endTime, boolean isRetry) {
        List<FinalPatternInfo> finalPatternInfos = Scanner.getFinalPatternInfos(originPattern, cycleUnit, timeOffset,
                startTime, endTime, isRetry);
        List<BasicFileInfo> infos = new ArrayList<>();
        for (FinalPatternInfo finalPatternInfo : finalPatternInfos) {
            String prefix = PatternUtil.getBeforeFirstWildcard(finalPatternInfo.finalPattern);
            Pattern pattern = Pattern.compile(finalPatternInfo.finalPattern,
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
            List<BasicFileInfo> fileInfos = scanTaskInOneCycle(cosClient, bucketName, pattern, prefix,
                    finalPatternInfo.dataTime, cycleUnit);
            infos.addAll(fileInfos);
        }
        return infos;
    }

    public static List<BasicFileInfo> scanTaskInOneCycle(COSClient cosClient, String bucketName, Pattern pattern,
            String prefix, Long dataTime, String cycleUnit) {
        List<BasicFileInfo> infos = new ArrayList<>();
        ObjectListing objectListing;
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        do {
            try {
                listObjectsRequest.setBucketName(bucketName);
                listObjectsRequest.setPrefix(prefix);
                listObjectsRequest.setDelimiter(DEFAULT_DELIMITER);
                listObjectsRequest.setMaxKeys(DEFAULT_KEY_COUNT);
                objectListing = cosClient.listObjects(listObjectsRequest);
            } catch (CosServiceException e) {
                logger.error("scanTaskInOneCycle finalPattern {} CosServiceException", pattern.pattern(), e);
                return infos;
            } catch (CosClientException e) {
                logger.error("scanTaskInOneCycle finalPattern {} CosClientException", pattern.pattern(), e);
                return infos;
            }
            List<String> commonPrefixes = objectListing.getCommonPrefixes();
            int depth;
            Pattern patternByDepth;
            if (!commonPrefixes.isEmpty()) {
                depth = countCharacterOccurrences(commonPrefixes.get(0), PATH_SEP);
                String nthOccurrenceSubstring = findNthOccurrenceSubstring(pattern.pattern(), PATH_SEP, depth);
                if (nthOccurrenceSubstring != null) {
                    patternByDepth = Pattern.compile(nthOccurrenceSubstring,
                            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
                    for (String commonPrefix : commonPrefixes) {
                        Matcher matcher = patternByDepth.matcher(commonPrefix);
                        if (matcher.matches()) {
                            infos.addAll(scanTaskInOneCycle(cosClient, bucketName, pattern, commonPrefix, dataTime,
                                    cycleUnit));
                        }
                    }
                }
            }
            List<COSObjectSummary> cosObjectSummaries = objectListing.getObjectSummaries();
            for (COSObjectSummary cosObjectSummary : cosObjectSummaries) {
                String key = cosObjectSummary.getKey();
                Matcher matcher = pattern.matcher(key);
                if (matcher.lookingAt()) {
                    long fileSize = cosObjectSummary.getSize();
                    String storageClasses = cosObjectSummary.getStorageClass();
                    infos.add(new BasicFileInfo(key,
                            DateTransUtils.millSecConvertToTimeStr(dataTime, cycleUnit)));
                    String strDataTime = DateTransUtils.millSecConvertToTimeStr(dataTime, cycleUnit);
                    logger.info("list key {} dataTime {} size {}, storageClasses {}", key, strDataTime, fileSize,
                            storageClasses);
                }
            }
            String nextMarker = objectListing.getNextMarker();
            listObjectsRequest.setMarker(nextMarker);
        } while (objectListing.isTruncated());
        return infos;
    }

    public static int countCharacterOccurrences(String input, char targetChar) {
        if (input == null) {
            throw new IllegalArgumentException("Input string cannot be null");
        }
        int count = 0;

        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == targetChar) {
                count++;
            }
        }
        return count;
    }

    public static String findNthOccurrenceSubstring(String input, char targetChar, int n) {
        int endIndex = findNthOccurrence(input, targetChar, n);
        if (endIndex != -1) {
            return input.substring(0, endIndex + 1);
        } else {
            return null;
        }
    }

    public static int findNthOccurrence(String input, char targetChar, int n) {
        int currentIndex = -1;
        for (int i = 0; i < n; i++) {
            currentIndex = input.indexOf(targetChar, currentIndex + 1);
            if (currentIndex == -1) {
                return -1;
            }
        }
        return currentIndex;
    }
}
