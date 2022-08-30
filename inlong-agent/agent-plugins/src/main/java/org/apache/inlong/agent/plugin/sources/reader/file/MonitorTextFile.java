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

package org.apache.inlong.agent.plugin.sources.reader.file;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * monitor files
 */
public final class MonitorTextFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitorTextFile.class);
    private static MonitorTextFile monitorTextFile;
    private static Long INTERVAL = 2000L;
    // monitor thread pool
    private final ThreadPoolExecutor runningPool = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("monitor file"));

    private MonitorTextFile() {

    }

    public static synchronized MonitorTextFile getInstance() {
        if (null == monitorTextFile) {
            monitorTextFile = new MonitorTextFile();
        }
        return monitorTextFile;
    }

    public void monitor(FileReaderOperator fileReaderOperator, TextFileReader textFileReader) {
        MonitorEvent monitorEvent = new MonitorEvent(fileReaderOperator, textFileReader);
        runningPool.execute(monitorEvent);
    }

    private class MonitorEvent implements Runnable {

        private final FileReaderOperator fileReaderOperator;
        private final TextFileReader textFileReader;
        /**
         * the last modify time of the file
         */
        private BasicFileAttributes attributesBefore;

        public MonitorEvent(FileReaderOperator fileReaderOperator, TextFileReader textFileReader) {
            this.fileReaderOperator = fileReaderOperator;
            this.textFileReader = textFileReader;
            try {
                this.attributesBefore = Files
                        .readAttributes(fileReaderOperator.file.toPath(), BasicFileAttributes.class);
            } catch (IOException e) {
                LOGGER.error("get {} last modify time error:", fileReaderOperator.file.getName(), e);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    listen();
                    TimeUnit.MILLISECONDS.sleep(INTERVAL);
                } catch (Exception e) {
                    LOGGER.error("monitor {} error:", this.fileReaderOperator.file.getName(), e);
                }
            }
        }

        private void listen() throws IOException {
            BasicFileAttributes attributesAfter = Files
                    .readAttributes(this.fileReaderOperator.file.toPath(), BasicFileAttributes.class);
            if (attributesBefore.lastModifiedTime().compareTo(attributesAfter.lastModifiedTime()) < 0
                    && !this.fileReaderOperator.iterator.hasNext()) {
                this.textFileReader.getData();
                this.fileReaderOperator.iterator = fileReaderOperator.stream.iterator();
                this.attributesBefore = attributesAfter;
            }
        }
    }
}
