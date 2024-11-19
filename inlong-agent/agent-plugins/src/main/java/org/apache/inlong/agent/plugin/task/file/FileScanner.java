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

package org.apache.inlong.agent.plugin.task.file;

import org.apache.inlong.agent.plugin.utils.regex.PatternUtil;
import org.apache.inlong.agent.plugin.utils.regex.Scanner;
import org.apache.inlong.agent.plugin.utils.regex.Scanner.FinalPatternInfo;
import org.apache.inlong.agent.utils.DateTransUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_FILE_MAX_NUM;

/*
 * This class is mainly used for scanning log file that we want to read. We use this class at
 * inlong_agent recover process, the do and redo tasks and the current log file access when we deploy a
 * new data source.
 */
public class FileScanner {

    public static class BasicFileInfo {

        public String fileName;
        public String dataTime;

        public BasicFileInfo(String fileName, String dataTime) {
            this.fileName = fileName;
            this.dataTime = dataTime;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(FileScanner.class);

    public static List<BasicFileInfo> scanTaskBetweenTimes(String originPattern, String cycleUnit, String timeOffset,
            long startTime, long endTime, boolean isRetry) {
        List<BasicFileInfo> infos = new ArrayList<>();
        List<FinalPatternInfo> finalPatternInfos = Scanner.getFinalPatternInfos(originPattern, cycleUnit, timeOffset,
                startTime, endTime, isRetry);
        for (FinalPatternInfo finalPatternInfo : finalPatternInfos) {
            ArrayList<String> allPaths = PatternUtil.cutDirectoryByWildcard(finalPatternInfo.finalPattern);
            String firstDir = allPaths.get(0);
            String secondDir = allPaths.get(0) + File.separator + allPaths.get(1);
            ArrayList<String> fileList = getUpdatedOrNewFiles(firstDir, secondDir, finalPatternInfo.finalPattern, 3,
                    DEFAULT_FILE_MAX_NUM);
            for (String file : fileList) {
                // TODO the time is not YYYYMMDDHH
                String dataTime = DateTransUtils.millSecConvertToTimeStr(finalPatternInfo.dataTime, cycleUnit);
                BasicFileInfo info = new BasicFileInfo(file, dataTime);
                logger.info("scan new task fileName {} ,dataTime {}", file, dataTime);
                infos.add(info);
            }
        }
        return infos;
    }

    private static ArrayList<String> getUpdatedOrNewFiles(String firstDir, String secondDir,
            String fileName, long depth, int maxFileNum) {
        ArrayList<String> ret = new ArrayList<String>();
        ArrayList<File> readyFiles = new ArrayList<File>();
        if (!new File(firstDir).isDirectory()) {
            return ret;
        }
        for (File pathname : Files.find(firstDir).yieldFilesAndDirectories()
                .recursive().withDepth((int) depth).withDirNameRegex(secondDir)
                .withFileNameRegex(fileName)) {
            if (readyFiles.size() >= maxFileNum) {
                break;
            }
            readyFiles.add(pathname);
        }
        // sort by last-modified time (older -> newer)
        Collections.sort(readyFiles, new FileTimeComparator());
        for (File f : readyFiles) {
            // System.out.println(f.getAbsolutePath());
            ret.add(f.getAbsolutePath());
        }
        return ret;
    }
}
