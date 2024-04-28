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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
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
            long procHandle = f.getLong(process);
            return procHandle;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private static String[] merge(String shellPath, String[] paths) {
        List<String> cmds = new ArrayList<String>();
        cmds.add(shellPath);
        for (String path : paths) {
            if (StringUtils.isBlank(path)) {
                continue;
            }
            cmds.add(path);
        }
        String[] strings = new String[cmds.size()];
        cmds.toArray(strings);
        return strings;
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

    public void asyncExec(String shellPath, String... params) {
        AsyncShellRunnable asyncShell = new AsyncShellRunnable(shellPath, this.tracker, params);
        Thread thread = new Thread(asyncShell);
        thread.start();
    }

    public void syncExec(String shellPath, String... params) {
        List<String> result = new ArrayList<String>();
        String[] cmds = merge(shellPath, params);
        try {
            tracker.start();
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
                tracker.fail(-1);
                return;
            }
            ps.waitFor();
            int exitValue = ps.exitValue();
            if (exitValue != 0) {
                tracker.fail(exitValue);
                return;
            }
            tracker.success();
        } catch (Exception e) {
            e.printStackTrace();
            result.add(e.getMessage());
            tracker.setRunResult(arrayToString(result.toArray(), InlongConstants.NEW_LINE));
            tracker.lineChange(e.getMessage());
            tracker.fail(-1);
        }
    }

    public void syncScriptExec(String script, String[] envConfig) {
        List<String> result = new ArrayList<String>();
        try {
            tracker.start();
            Process ps = Runtime.getRuntime().exec("bash +x " + script, envConfig);
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
                tracker.fail(-1);
                return;
            }
            ps.waitFor();
            int exitValue = ps.exitValue();
            if (exitValue != 0) {
                tracker.fail(exitValue);
                return;
            }
            tracker.success();
        } catch (Exception e) {
            e.printStackTrace();
            result.add(e.getMessage());
            tracker.setRunResult(arrayToString(result.toArray(), InlongConstants.NEW_LINE));
            tracker.lineChange(e.getMessage());
            tracker.fail(-1);
        }
    }

    public static class AsyncShellRunnable implements Runnable {

        private String shellPath;
        private String[] params;
        private List<String> result = new ArrayList<String>();
        private ShellTracker tracker;

        public AsyncShellRunnable(String shellPath, ShellTracker tracker, String... params) {
            this.shellPath = shellPath;
            this.params = params;
            this.tracker = tracker;
        }

        public void run() {
            String[] cmds = merge(shellPath, params);
            try {
                tracker.start();
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
                    tracker.fail(-1);
                    return;
                }
                ps.waitFor();
                int exitValue = ps.exitValue();
                if (exitValue != 0) {
                    tracker.fail(exitValue);
                    return;
                }
                tracker.success();
            } catch (Exception e) {
                e.printStackTrace();
                result.add(e.getMessage());
                tracker.setRunResult(arrayToString(result.toArray(), InlongConstants.NEW_LINE));
                tracker.lineChange(e.getMessage());
                tracker.fail(-1);
            }
        }
    }
}
