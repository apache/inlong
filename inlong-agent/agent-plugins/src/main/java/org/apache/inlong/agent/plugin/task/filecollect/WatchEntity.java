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

package org.apache.inlong.agent.plugin.task.filecollect;

import org.apache.inlong.agent.plugin.utils.file.DateUtils;
import org.apache.inlong.agent.plugin.utils.file.FilePathUtil;
import org.apache.inlong.agent.plugin.utils.file.NewDateUtils;
import org.apache.inlong.agent.plugin.utils.file.NonRegexPatternPosition;
import org.apache.inlong.agent.plugin.utils.file.PathDateExpression;
import org.apache.inlong.agent.utils.DateTransUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WatchEntity {

    private static final Logger logger = LoggerFactory.getLogger(WatchEntity.class);
    private WatchService watchService;
    private final String basicStaticPath;
    private final String originPattern;
    private final String regexPattern;
    private final Pattern pattern;
    private final PathDateExpression dateExpression;
    private final String originPatternWithoutFileName;
    private final Pattern patternWithoutFileName;
    private final boolean containRegexPattern;
    private final Map<WatchKey, Path> keys = new ConcurrentHashMap<WatchKey, Path>();
    private final Map<String, WatchKey> pathToKeys = new ConcurrentHashMap<String, WatchKey>();
    private final String dirSeparator = System.getProperty("file.separator");
    private String cycleUnit;
    private String timeOffset;

    public WatchEntity(WatchService watchService,
            String originPattern,
            String cycleUnit,
            String timeOffset) {
        this.watchService = watchService;
        this.originPattern = originPattern;
        ArrayList<String> directoryLayers = FilePathUtil.getDirectoryLayers(originPattern);
        this.basicStaticPath = directoryLayers.get(0);
        this.regexPattern = NewDateUtils.replaceDateExpressionWithRegex(originPattern);
        pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        ArrayList<String> directories = FilePathUtil.cutDirectory(originPattern);
        this.originPatternWithoutFileName = directories.get(0);
        this.patternWithoutFileName = Pattern
                .compile(NewDateUtils.replaceDateExpressionWithRegex(originPatternWithoutFileName),
                        Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        /*
         * Get the longest data regex from the data name, it's used if we want to get out the data time from the file
         * name.
         */
        this.dateExpression = DateUtils.extractLongestTimeRegexWithPrefixOrSuffix(originPattern);
        this.containRegexPattern = isPathContainRegexPattern();
        this.cycleUnit = cycleUnit;
        this.timeOffset = timeOffset;
        logger.info("add a new watchEntity {}", this);
    }

    @Override
    public String toString() {
        return "WatchEntity [parentPathName=" + basicStaticPath
                + ", readFilePattern=" + regexPattern
                + ", dateExpression=" + dateExpression + ", totalDirPattern="
                + originPatternWithoutFileName + ", containRegexPattern="
                + containRegexPattern + ", totalDirRegexPattern="
                + patternWithoutFileName + ", keys=" + keys + ", pathToKeys=" + pathToKeys
                + ", watchService=" + watchService + "]";
    }

    private boolean isPathContainRegexPattern() {
        if (originPatternWithoutFileName.contains("YYYY") || originPatternWithoutFileName.contains("MM")
                || originPatternWithoutFileName.contains("DD") || originPatternWithoutFileName.contains("hh")) {
            return true;
        }

        return false;
    }

    public boolean isContainRegexPattern() {
        return containRegexPattern;
    }

    private int calcPathDepth(String rootDir, String dirName) {
        // rootDir
        return 0;
    }

    private void register(Path dir) throws IOException {

        if (dir == null) {
            return;
        }

        String dirName = dir.toAbsolutePath().toString();
        logger.info(dirName);
        Matcher matcher = patternWithoutFileName.matcher(dirName);
        String rootDir = Paths.get(basicStaticPath).toAbsolutePath().toString();
        Paths.get(basicStaticPath).toAbsolutePath().getNameCount();

        // must use suffeix match
        // consider /data/YYYYMMDD/abc/YYYYMMDDhh.*.txt this case
        if (!pathToKeys.containsKey(dirName) && (matcher.matches() || rootDir.equals(dirName))) {
            WatchKey key = dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            keys.put(key, dir);
            pathToKeys.put(dirName, key);

            logger.info("Register a new directory: " + dir.toAbsolutePath().toString());
        }
    }

    public void registerRecursively() throws IOException {
        // register root dir
        Path rootPath = Paths.get(basicStaticPath);
        String rootDirName = rootPath.toAbsolutePath().toString();
        if (!pathToKeys.containsKey(rootDirName)) {
            WatchKey key = rootPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            keys.put(key, rootPath);
            pathToKeys.put(rootDirName, key);
            logger.info("Register a new directory: " + rootDirName);
        }
        registerRecursively(rootPath.toFile(), rootPath.toAbsolutePath().toString().length() + 1);
    }

    public void registerRecursively(Path dir) throws IOException {
        Path rootPath = dir;
        String rootDirName = rootPath.toAbsolutePath().toString();
        int beginIndex = rootDirName.lastIndexOf(dirSeparator) + 1;
        if (beginIndex == 0) {
            return;
        }
        int index = originPatternWithoutFileName.indexOf(dirSeparator, beginIndex + 1);
        Pattern pattern = getPattern(index);
        logger.info("beginIndex {} ,index {} ,dirPattern {}",
                new Object[]{beginIndex, index, pattern.pattern()});
        if (!pathToKeys.containsKey(rootDirName) && match(pattern, rootDirName)) {
            WatchKey key = rootPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            keys.put(key, rootPath);
            pathToKeys.put(rootDirName, key);
            logger.info("Register a new directory: " + rootDirName);
        } else {
            return;
        }

        logger.info("rootPath len {}", rootPath.toAbsolutePath().toString().length());

        registerRecursively(rootPath.toFile(), rootPath.toAbsolutePath().toString().length() + 1);
    }

    public void registerRecursively(File dir, int beginIndex) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        int index = originPatternWithoutFileName.indexOf(dirSeparator, beginIndex);
        Pattern pattern = getPattern(index);
        logger.info("beginIndex {} ,index {} ,dirPattern {}",
                new Object[]{beginIndex, index, pattern.pattern()});
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                String dirName = files[i].toString();
                Path dirPath = Paths.get(dirName);
                if (!pathToKeys.containsKey(dirName) && match(pattern, dirName)) {
                    try {
                        WatchKey key = dirPath
                                .register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                        keys.put(key, dirPath);
                        pathToKeys.put(dirName, key);
                        logger.info("Register a new directory: " + dirName);
                    } catch (IOException e) {
                        /**
                         * catch error，ignore the child directory that can not register
                         */
                        logger.error("Register directory {} error, skip it. ", dirName, e);
                        continue;
                    }
                    registerRecursively(files[i].getAbsoluteFile(),
                            files[i].getAbsolutePath().length() + 1);
                }
            }
        }
    }

    private Pattern getPattern(int index) {
        String dirPattern = "";
        if (index == -1) {
            dirPattern = originPatternWithoutFileName;
        } else {
            dirPattern = originPatternWithoutFileName.substring(0, index);
        }
        Pattern pattern = Pattern.compile(NewDateUtils.replaceDateExpressionWithRegex(dirPattern),
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        return pattern;
    }

    private boolean match(Pattern pattern, String dirName) {
        Matcher matcher = pattern.matcher(dirName);
        return matcher.matches() || matcher.lookingAt();
    }

    public Path getPath(WatchKey key) {
        return keys.get(key);
    }

    public int getTotalPathSize() {
        return keys.size();
    }

    public String getWatchPath() {
        return basicStaticPath;
    }

    public WatchService getWatchService() {
        return watchService;
    }

    public void setWatchService(WatchService watchService) {
        this.watchService = watchService;
    }

    public String getRegexPattern() {
        return regexPattern;
    }

    public PathDateExpression getDateExpression() {
        return dateExpression;
    }

    public String getLongestDatePattern() {
        return dateExpression.getLongestDatePattern();
    }

    public NonRegexPatternPosition getPatternPosition() {
        return dateExpression.getPatternPosition();
    }

    /*
     * Remove the watched path which is 3 cycle units earlier than current task data time, this is because JDK7 starts a
     * thread for each watch path, which should consume lots of memory.
     */
    public void removeUselessWatchDirectories(String curDataTime)
            throws Exception {

        logger.info("removeUselessWatchDirectories {}", curDataTime);

        /* Calculate the data time which is 3 cycle units earlier than current task data time. */
        long curDataTimeMillis = DateTransUtils.timeStrConvertTomillSec(curDataTime, cycleUnit);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(curDataTimeMillis);
        if ("D".equalsIgnoreCase(cycleUnit)) {
            calendar.add(Calendar.DAY_OF_YEAR, -3);
        } else if ("h".equalsIgnoreCase(cycleUnit)) {
            calendar.add(Calendar.HOUR_OF_DAY, -3);
        } else if ("10m".equalsIgnoreCase(cycleUnit)) {
            calendar.add(Calendar.MINUTE, -30);
        }

        /* Calculate the 3 cycle units earlier date. */
        String year = String.valueOf(calendar.get(Calendar.YEAR));
        String month = String.valueOf(calendar.get(Calendar.MONTH) + 1);
        if (month.length() < 2) {
            month = "0" + month;
        }
        String day = String.valueOf(calendar.get(Calendar.DAY_OF_MONTH));
        if (day.length() < 2) {
            day = "0" + day;
        }
        String hour = String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
        if (hour.length() < 2) {
            hour = "0" + hour;
        }
        String minute = String.valueOf(calendar.get(Calendar.MINUTE));
        if (minute.length() < 2) {
            minute = "0" + minute;
        }

        /* Replace it with the date and get a specified watch path. */
        String copyDirPattern = new String(originPatternWithoutFileName);
        copyDirPattern = copyDirPattern.replace("YYYY", year);
        copyDirPattern = copyDirPattern.replace("MM", month);
        copyDirPattern = copyDirPattern.replace("DD", day);
        copyDirPattern = copyDirPattern.replace("hh", hour);
        copyDirPattern = copyDirPattern.replace("mm", minute);

        Set<String> keys = pathToKeys.keySet();
        Set<String> tmpKeys = new HashSet<>();
        tmpKeys.addAll(keys);
        String rootDir = Paths.get(basicStaticPath).toAbsolutePath().toString();
        for (String path : tmpKeys) {
            /*
             * Remove the watch path whose path is smaller than the 3 cycle units earlier.
             */
            logger.info("[Path]{}  {}", path, copyDirPattern);
            if (path.compareTo(copyDirPattern) < 0 && !copyDirPattern.contains(path)) {
                WatchKey key = pathToKeys.get(path);
                key.cancel();

                pathToKeys.remove(path);

                logger.info("Watch path: {} is too old for data time: {}, we should remove", path,
                        curDataTime);
            }
        }
    }

    public void clearPathToKeys() {
        pathToKeys.clear();
    }

    public void clearKeys() {
        keys.clear();
    }

    public String getCycleUnit() {
        return cycleUnit;
    }

    public String getTimeOffset() {
        return timeOffset;
    }

    public String getOriginPattern() {
        return originPattern;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
