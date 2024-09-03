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

package org.apache.inlong.manager.service.cmd.shell;

import org.apache.inlong.manager.common.consts.InlongConstants;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class ShellExecutorImpl implements ShellExecutor {

    private static final String[] EXCEPTION_REG = new String[]{"(.*)Caused by: (.*)Exception(.*)",
            "(.*)java.net.UnknownHostException(.*)",
            "(.*)Copy failed: java.io.IOException: Job failed!(.*)"};
    private ShellTracker tracker;

    public ShellExecutorImpl(ShellTracker tracker) {
        this.tracker = tracker;
    }

    private static long getPid(Process process) {
        try {
            Field f = process.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            return f.getLong(process);
        } catch (Exception e) {
            log.error("get pid failed", e);
            return -1;
        }
    }

    private static String[] merge(String shellPath, String[] paths) {
        List<String> cmds = new ArrayList<String>();
        cmds.add(shellPath);
        cmds.addAll(Arrays.asList(paths));
        return cmds.toArray(new String[0]);
    }

    private static String arrayToString(Object[] array, String split) {
        if (array == null || array.length == 0) {
            return InlongConstants.BLANK;
        }
        StringBuilder str = new StringBuilder();
        for (int i = 0, length = array.length; i < length; i++) {
            if (i != 0) {
                str.append(split);
            }
            str.append(array[i]);
        }
        return str.toString();
    }

    private static boolean HasException(String str) {
        for (String reg : EXCEPTION_REG) {
            Pattern pattern = Pattern.compile(reg);
            Matcher matcher = pattern.matcher(str);
            if (matcher.find()) {
                return true;
            }
        }
        return false;
    }

    public void syncExec(String shellPath, String... params) {
        List<String> result = new ArrayList<String>();
        String[] cmds = merge(shellPath, params);
        try {
            Process ps = Runtime.getRuntime().exec(cmds);
            long pid = getPid(ps);
            tracker.setProcessId(pid);
            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            String line;
            boolean hasException = false;
            while ((line = br.readLine()) != null) {
                if (HasException(line)) {
                    hasException = true;
                }
                result.add(line);
                tracker.setRunResult(arrayToString(result.toArray(), InlongConstants.NEW_LINE));
                tracker.lineChange(line);
            }
            if (hasException) {
                tracker.lineChange("Java exception exist in output");
                tracker.setCode(-1);
                return;
            }
            ps.waitFor();
            int exitValue = ps.exitValue();
            if (exitValue != 0) {
                tracker.setCode(exitValue);
            }
        } catch (Exception e) {
            log.error("sync exec shell failed", e);
            result.add(e.getMessage());
            tracker.setRunResult(arrayToString(result.toArray(), InlongConstants.NEW_LINE));
            tracker.lineChange(e.getMessage());
            tracker.setCode(-1);
        }
    }
}
