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

package org.apache.inlong.agent.plugin.utils;

import org.apache.inlong.agent.plugin.utils.regex.NewDateUtils;
import org.apache.inlong.agent.plugin.utils.regex.PatternUtil;
import org.apache.inlong.agent.utils.DateTransUtils;
import org.apache.inlong.common.metric.MetricRegister;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

public class TestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);
    private static final String RECORD = "This is the test line for file\n";

    @Test
    public void testCalcOffset() {
        Assert.assertTrue(DateTransUtils.calcOffset("-1h") == -3600 * 1000);
        Assert.assertTrue(DateTransUtils.calcOffset("1D") == 24 * 3600 * 1000);
        Assert.assertTrue(DateTransUtils.calcOffset("0") == 0);
        Assert.assertTrue(DateTransUtils.calcOffset("1") == 0);
        Assert.assertTrue(DateTransUtils.calcOffset("10") == 0);
        Assert.assertTrue(DateTransUtils.calcOffset("") == 0);
    }

    @Test
    public void testPattern() throws ParseException {
        /*
         * Condition: YYYY(?:.MM|MM)?(?:.DD|DD)?(?:.hh|hh)?(?:.mm|mm)?(?:.ss|ss)? The date expression as a whole must
         * meet this condition in order to match Need to start with YYYY, and month, day, hour, minute can only be
         * separated by one character
         */
        testReplaceDateExpression("/YYYYMMDDhhmm.log", "/202406251007.log");
        testReplaceDateExpression("/YYYY.log", "/2024.log");
        testReplaceDateExpression("/YYYYhhmm.log", "/20241007.log");
        testReplaceDateExpression("/YYYY/YYYYMMDDhhmm.log", "/2024/202406251007.log");
        testReplaceDateExpression("/YYYY/MMDD/hhmm.log", "/2024/0625/1007.log");
        testReplaceDateExpression("/data/YYYYMMDD.hh/mm.log_[0-9]+", "/data/20240625.10/07.log_[0-9]+");
        // error cases
        testReplaceDateExpression("/YYY.log", "/YYY.log");
        testReplaceDateExpression("/MMDDhhmm.log", "/MMDDhhmm.log");
        testReplaceDateExpression("/MMDD/hhmm.log", "/MMDD/hhmm.log");
        testReplaceDateExpression("/data/YYYYMMDD..hh/mm.log_[0-9]+", "/data/20240625..hh/mm.log_[0-9]+");

        /*
         * 1 cut the file name 2 cut the path contains wildcard
         */
        testCutDirectoryByWildcard("/data/123/YYYYMMDDhhmm.log",
                Arrays.asList("/data/123", "", "YYYYMMDDhhmm.log"));
        testCutDirectoryByWildcard("/data/YYYYMMDDhhmm/test.log",
                Arrays.asList("/data/YYYYMMDDhhmm", "", "test.log"));
        testCutDirectoryByWildcard("/data/YYYYMMDDhhmm*/test.log",
                Arrays.asList("/data", "YYYYMMDDhhmm*", "test.log"));
        testCutDirectoryByWildcard("/data/log_minute/minute_YYYYMMDDhh*/mm.log_[0-9]+",
                Arrays.asList("/data/log_minute", "minute_YYYYMMDDhh*", "mm.log_[0-9]+"));
        testCutDirectoryByWildcard("/data/123+/YYYYMMDDhhmm.log",
                Arrays.asList("/data", "123+", "YYYYMMDDhhmm.log"));
        testCutDirectoryByWildcard("/data/2024112610*/test.log",
                Arrays.asList("/data", "2024112610*", "test.log"));

        /*
         * 1 cut the file name 2 cut the path contains wildcard or date expression
         */
        testCutDirectoryByWildcardAndDateExpression("/data/YYYYMM/YYYaaMM/YYYYMMDDhhmm.log",
                Arrays.asList("/data", "YYYYMM/YYYaaMM", "YYYYMMDDhhmm.log"));
        testCutDirectoryByWildcardAndDateExpression("/data/YYYY/test.log",
                Arrays.asList("/data", "YYYY", "test.log"));
        testCutDirectoryByWildcardAndDateExpression("/data/123*/MMDD/test.log",
                Arrays.asList("/data", "123*/MMDD", "test.log"));
        testCutDirectoryByWildcardAndDateExpression("/data/YYYYMMDD/123*/test.log",
                Arrays.asList("/data", "YYYYMMDD/123*", "test.log"));

        /*
         * get the string before the first wildcard
         */
        testGetBeforeFirstWildcard("/data/YYYYMM/YYYaaMM/YYYYMMDDhhmm.log",
                "/data/YYYYMM/YYYaaMM/YYYYMMDDhhmm");
        testGetBeforeFirstWildcard("/data/123*/MMDD/test.log",
                "/data/123");
        testGetBeforeFirstWildcard("/data/YYYYMMDD/123*/test.log",
                "/data/YYYYMMDD/123");
        testGetBeforeFirstWildcard("test/65535_YYYYMMDD_hh00.log",
                "test/65535_YYYYMMDD_hh00");
        testGetBeforeFirstWildcard("/data/YYYYMMDD/*123/test.log",
                "/data/YYYYMMDD/");
    }

    private void testReplaceDateExpression(String src, String dst) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Long dataTime = DateTransUtils.timeStrConvertToMillSec("202406251007", "m");
        calendar.setTimeInMillis(dataTime);
        Assert.assertEquals(NewDateUtils.replaceDateExpression(calendar, src), dst);
    }

    private void testCutDirectoryByWildcard(String src, List<String> dst) {
        ArrayList<String> directories = PatternUtil.cutDirectoryByWildcard(src);
        Assert.assertEquals(directories, dst);
    }

    private void testGetBeforeFirstWildcard(String src, String dst) {
        String temp = PatternUtil.getBeforeFirstWildcard(src);
        Assert.assertEquals(dst, temp);
    }

    private void testCutDirectoryByWildcardAndDateExpression(String src, List<String> dst) {
        ArrayList<String> directoryLayers = PatternUtil.cutDirectoryByWildcardAndDateExpression(src);
        Assert.assertEquals(directoryLayers, dst);
    }

    public static void createHugeFiles(String fileName, String rootDir, String record) throws Exception {
        final Path hugeFile = Paths.get(rootDir, fileName);
        FileWriter writer = new FileWriter(hugeFile.toFile());
        for (int i = 0; i < 1024; i++) {
            writer.write(record);
        }
        writer.flush();
        writer.close();
    }

    public static void createMultipleLineFiles(String fileName, String rootDir,
            String record, int lineNum) throws Exception {
        final Path hugeFile = Paths.get(rootDir, fileName);
        List<String> beforeList = new ArrayList<>();
        for (int i = 0; i < lineNum; i++) {
            beforeList.add(String.format("%s_%d", record, i));
        }
        Files.write(hugeFile, beforeList, StandardOpenOption.CREATE);
    }

    public static void mockMetricRegister() throws Exception {
        PowerMockito.mockStatic(MetricRegister.class);
        PowerMockito.doNothing().when(MetricRegister.class, "register", any());
    }

    public static void createFile(String fileName) throws Exception {
        FileWriter writer = new FileWriter(fileName);
        for (int i = 0; i < 1; i++) {
            writer.write(RECORD);
        }
        writer.flush();
        writer.close();
    }

    public static void deleteFile(String fileName) throws Exception {
        try {
            FileUtils.delete(Paths.get(fileName).toFile());
        } catch (Exception ignored) {
            LOGGER.warn("deleteFile error ", ignored);
        }
    }

    public static void write(String fileName, StringBuffer records) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter writer = new FileWriter(file, true);
        writer.write(records.toString());
        writer.flush();
        writer.close();
    }
}
