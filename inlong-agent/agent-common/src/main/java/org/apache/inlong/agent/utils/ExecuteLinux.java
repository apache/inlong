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

package org.apache.inlong.agent.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility for executing local OS commands.
 *
 * <p>{@link #exeCmd(String[])}, {@link #exeCmd(List, File, long)} and
 * {@link #exePipedCmd(List, File, long)} are the recommended argv-array / piped entry points.
 * They use {@link ProcessBuilder} directly and never go through {@code /bin/sh -c}, so shell
 * metacharacters ({@code ;}, {@code |}, {@code &&}, backticks, {@code $(...)}) are never
 * interpreted.</p>
 *
 * <p>{@link #exeCmd(String)} is a legacy string-based fallback that runs commands through
 * {@code /bin/sh -c}; it is {@link Deprecated} and must only be used with strings already
 * validated by {@code ModuleCommandValidator}.</p>
 *
 * <p>Piping is emulated by chaining several {@link ProcessBuilder} instances via background
 * threads that copy stdout to the next process's stdin, so no shell is required.</p>
 */
public class ExecuteLinux {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteLinux.class);

    /** Default command execution timeout in milliseconds. */
    public static final long DEFAULT_TIMEOUT_MS = 60_000L;

    /** Maximum number of bytes of stderr kept in log output (truncated beyond this). */
    private static final int STDERR_LOG_MAX_BYTES = 1024;

    private ExecuteLinux() {
    }

    /**
     * Execute a command using an argv array (no shell), inheriting the JVM working directory
     * and using {@link #DEFAULT_TIMEOUT_MS} as the timeout.
     *
     * @param cmdArgs argv, {@code cmdArgs[0]} is the executable name.
     * @return stdout of the child process; {@code null} on error or timeout.
     */
    public static String exeCmd(String[] cmdArgs) {
        if (cmdArgs == null || cmdArgs.length == 0) {
            logger.error("exeCmd(String[]) called with empty cmdArgs");
            return null;
        }
        return exeCmd(Arrays.asList(cmdArgs), null, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Execute a command using an argv list (no shell), with an optional working directory and
     * timeout. Every element in {@code cmdArgs} is passed to the child as a literal argv
     * entry; shell metacharacters are never interpreted. If the child does not finish within
     * {@code timeoutMs} it is force-killed via {@link Process#destroyForcibly()}.
     *
     * @param cmdArgs   argv list, index 0 is the executable name.
     * @param workDir   working directory of the child; {@code null} means inherit.
     * @param timeoutMs timeout in milliseconds; a value &lt;= 0 means {@link #DEFAULT_TIMEOUT_MS}.
     * @return stdout of the child; {@code null} on non-zero exit, error or timeout.
     */
    public static String exeCmd(List<String> cmdArgs, File workDir, long timeoutMs) {
        if (cmdArgs == null || cmdArgs.isEmpty()) {
            logger.error("exeCmd(List) called with empty cmdArgs");
            return null;
        }
        long timeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
        Process ps = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(cmdArgs);
            if (workDir != null) {
                pb.directory(workDir);
            }
            pb.redirectErrorStream(false);
            long startNs = System.nanoTime();
            ps = pb.start();

            // Drain stdout/stderr on background threads to prevent the child from blocking
            // when either pipe buffer fills up.
            StreamGobbler stdoutGobbler = new StreamGobbler(ps.getInputStream(), "stdout-" + cmdArgs.get(0));
            StreamGobbler stderrGobbler = new StreamGobbler(ps.getErrorStream(), "stderr-" + cmdArgs.get(0));
            stdoutGobbler.start();
            stderrGobbler.start();

            boolean finished = ps.waitFor(timeout, TimeUnit.MILLISECONDS);
            if (!finished) {
                ps.destroyForcibly();
                ps.waitFor(1, TimeUnit.SECONDS);
                logger.error("exeCmd timeout after {} ms, cmd={}", timeout, cmdArgs);
                stdoutGobbler.join(1000);
                stderrGobbler.join(1000);
                closeQuietly(ps);
                return null;
            }
            stdoutGobbler.join(1000);
            stderrGobbler.join(1000);

            int exitCode = ps.exitValue();
            long costMs = (System.nanoTime() - startNs) / 1_000_000L;
            String stdout = stdoutGobbler.getResult();
            String stderr = stderrGobbler.getResult();

            if (exitCode == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("exeCmd success cmd={} exit=0 costMs={}", cmdArgs, costMs);
                }
                return stdout;
            } else {
                logger.error("exeCmd non-zero exit cmd={} exitCode={} costMs={} stderr={}",
                        cmdArgs, exitCode, costMs, truncate(stderr, STDERR_LOG_MAX_BYTES));
                return null;
            }
        } catch (Exception e) {
            logger.error("exeCmd error cmd={}", cmdArgs, e);
            return null;
        } finally {
            if (ps != null) {
                closeQuietly(ps);
            }
        }
    }

    /**
     * Emulate a shell pipe {@code |} by chaining several {@link ProcessBuilder} instances on
     * the Java side, without going through {@code /bin/sh -c}. Each segment is started as its
     * own child; a background thread copies the stdout of every upstream child into the
     * stdin of the next downstream child; the stdout of the last segment is returned.
     *
     * @param argvSegments argv of each pipe segment, e.g.
     *                     {@code [["ps","aux"], ["grep","java"], ["awk","{print $2}"]]}.
     * @param workDir      shared working directory for every segment; {@code null} inherits.
     * @param timeoutMs    total timeout for the whole pipeline in milliseconds; &lt;= 0 means default.
     * @return stdout of the last segment; {@code null} on error or timeout.
     */
    public static String exePipedCmd(List<String[]> argvSegments, File workDir, long timeoutMs) {
        if (argvSegments == null || argvSegments.isEmpty()) {
            logger.error("exePipedCmd called with empty argvSegments");
            return null;
        }
        if (argvSegments.size() == 1) {
            return exeCmd(Arrays.asList(argvSegments.get(0)), workDir, timeoutMs);
        }
        long timeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
        List<Process> processes = new ArrayList<>(argvSegments.size());
        List<Thread> pumpers = new ArrayList<>(argvSegments.size() - 1);
        List<StreamGobbler> stderrGobblers = new ArrayList<>(argvSegments.size());
        StreamGobbler tailStdoutGobbler = null;
        try {
            for (int i = 0; i < argvSegments.size(); i++) {
                String[] argv = argvSegments.get(i);
                if (argv == null || argv.length == 0) {
                    logger.error("exePipedCmd segment[{}] is empty", i);
                    return null;
                }
                ProcessBuilder pb = new ProcessBuilder(argv);
                if (workDir != null) {
                    pb.directory(workDir);
                }
                pb.redirectErrorStream(false);
                Process p = pb.start();
                processes.add(p);
                stderrGobblers.add(new StreamGobbler(p.getErrorStream(), "stderr-piped-" + argv[0]));
                stderrGobblers.get(i).start();
            }
            // Chain adjacent segments: upstream stdout -> downstream stdin.
            for (int i = 0; i < processes.size() - 1; i++) {
                Process upstream = processes.get(i);
                Process downstream = processes.get(i + 1);
                Thread pump = new Thread(new StreamPump(upstream.getInputStream(), downstream.getOutputStream()),
                        "pipe-pump-" + i);
                pump.setDaemon(true);
                pump.start();
                pumpers.add(pump);
            }
            Process tail = processes.get(processes.size() - 1);
            tailStdoutGobbler = new StreamGobbler(tail.getInputStream(), "stdout-piped-tail");
            tailStdoutGobbler.start();

            long deadline = System.currentTimeMillis() + timeout;
            for (Process p : processes) {
                long remain = deadline - System.currentTimeMillis();
                if (remain <= 0) {
                    logger.error("exePipedCmd timeout, argvSegments={}", argvSegmentsToString(argvSegments));
                    return null;
                }
                if (!p.waitFor(remain, TimeUnit.MILLISECONDS)) {
                    logger.error("exePipedCmd timeout at one segment, argvSegments={}",
                            argvSegmentsToString(argvSegments));
                    return null;
                }
            }
            for (Thread pump : pumpers) {
                pump.join(1000);
            }
            tailStdoutGobbler.join(1000);
            for (StreamGobbler g : stderrGobblers) {
                g.join(1000);
            }

            int tailExit = tail.exitValue();
            if (tailExit != 0) {
                logger.error("exePipedCmd tail non-zero exit={} stderr={}", tailExit,
                        truncate(stderrGobblers.get(stderrGobblers.size() - 1).getResult(), STDERR_LOG_MAX_BYTES));
                return null;
            }
            return tailStdoutGobbler.getResult();
        } catch (Exception e) {
            logger.error("exePipedCmd error argvSegments={}", argvSegmentsToString(argvSegments), e);
            return null;
        } finally {
            // Force-kill every child on any error/timeout path so we never leak zombies or fds.
            for (Process p : processes) {
                try {
                    if (p != null && p.isAlive()) {
                        p.destroyForcibly();
                    }
                } catch (Exception ignore) {
                }
                closeQuietly(p);
            }
        }
    }

    /**
     * Legacy string-based command entry point that runs the given command through
     * {@code /bin/sh -c}. <b>Vulnerable to shell injection.</b> Kept only as a fallback for
     * strings already validated by {@code ModuleCommandValidator}. New business code must
     * use {@link #exeCmd(String[])} or {@link #exePipedCmd(List, File, long)} instead.
     *
     * @param commandStr a command string that has already been validated.
     * @return stdout of the child process; {@code null} on error.
     * @deprecated use {@link #exeCmd(String[])} or {@link #exePipedCmd(List, File, long)}.
     */
    @Deprecated
    public static String exeCmd(String commandStr) {
        String result = null;
        try {
            String[] cmd = new String[]{"/bin/sh", "-c", commandStr};
            Process ps = Runtime.getRuntime().exec(cmd);

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream(),
                    StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            result = sb.toString();

        } catch (Exception e) {
            logger.error("execute linux cmd error: ", e);
        }

        return result;
    }

    // -------------------------------------------------------------------------
    // internal helpers
    // -------------------------------------------------------------------------

    private static void closeQuietly(Process ps) {
        if (ps == null) {
            return;
        }
        closeQuietly(ps.getInputStream());
        closeQuietly(ps.getErrorStream());
        closeQuietly(ps.getOutputStream());
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException ignore) {
        }
    }

    private static String truncate(String s, int maxBytes) {
        if (s == null) {
            return "";
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return s;
        }
        return new String(bytes, 0, maxBytes, StandardCharsets.UTF_8) + "...(truncated)";
    }

    private static String argvSegmentsToString(List<String[]> segs) {
        List<List<String>> readable = new ArrayList<>(segs.size());
        for (String[] seg : segs) {
            readable.add(seg == null ? Collections.<String>emptyList() : Arrays.asList(seg));
        }
        return readable.toString();
    }

    /** Daemon thread that fully drains an {@link InputStream} into memory. */
    private static final class StreamGobbler extends Thread {

        private final InputStream in;
        private final AtomicReference<String> resultRef = new AtomicReference<>("");

        StreamGobbler(InputStream in, String threadName) {
            super(threadName);
            this.in = in;
            setDaemon(true);
        }

        @Override
        public void run() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            try {
                while ((n = in.read(buf)) != -1) {
                    baos.write(buf, 0, n);
                }
                resultRef.set(new String(baos.toByteArray(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                // Reading may throw once the child has been destroyed; treat as end of stream.
                resultRef.set(new String(baos.toByteArray(), StandardCharsets.UTF_8));
            } finally {
                closeQuietly(in);
            }
        }

        String getResult() {
            return resultRef.get();
        }
    }

    /** Daemon thread that copies stdout of an upstream process into stdin of a downstream one. */
    private static final class StreamPump implements Runnable {

        private final InputStream in;
        private final OutputStream out;

        StreamPump(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public void run() {
            byte[] buf = new byte[4096];
            int n;
            try {
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                }
                out.flush();
            } catch (IOException ignore) {
                // Downstream process may have exited; treat as end of pipe.
            } finally {
                closeQuietly(in);
                closeQuietly(out);
            }
        }
    }
}