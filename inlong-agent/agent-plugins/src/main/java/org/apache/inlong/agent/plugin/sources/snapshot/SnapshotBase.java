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

package org.apache.inlong.agent.plugin.sources.snapshot;

import org.apache.inlong.agent.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.util.Base64;

public interface SnapshotBase {
    Logger LOGGER = LoggerFactory.getLogger(SnapshotBase.class);

    Base64.Decoder DECODER = Base64.getDecoder();
    Base64.Encoder ENCODER = Base64.getEncoder();

    /**
     * buffer size used for reading and writing
     */
    int BUFFER_SIZE = 8192;
    /**
     * start offset
     */
    int START_OFFSET = 0;


    /**
     * get snapshot of the job
     * @return
     */
    String getSnapshot();

    /**
     * close resources
     */
    void close();

    /**
     * Load the file contents from the specified path
     *
     * @param file file
     * @return file content
     */
    default byte[] load(File file) {
        try {
            if (!file.exists()) {
                // if parentDir not exist, create first
                File parentDir = file.getParentFile();
                if (parentDir == null) {
                    LOGGER.info("no parent dir, file:{}", file.getAbsolutePath());
                    return new byte[0];
                }
                if (!parentDir.exists()) {
                    boolean success = parentDir.mkdirs();
                    LOGGER.info("create dir {} result {}", parentDir, success);
                }
                file.createNewFile();
            }
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream inputStream = new BufferedInputStream(fis);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int len;
            byte[] buf = new byte[BUFFER_SIZE];
            while ((len = inputStream.read(buf)) != -1) {
                outputStream.write(buf, START_OFFSET, len);
            }
            inputStream.close();
            outputStream.close();
            return outputStream.toByteArray();
        } catch (Throwable ex) {
            LOGGER.error("load binlog offset error", ex);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            return null;
        }
    }

    /**
     * offset persist
     *
     * @param snapshot Contents of the file to be written back
     * @param destFile Target file
     */
    default void save(String snapshot, File destFile) {
        byte[] bytes = DECODER.decode(snapshot);
        if (bytes.length != 0) {
            //offset = bytes;
            try (OutputStream output = Files.newOutputStream(destFile.toPath())) {
                output.write(bytes);
            } catch (Throwable e) {
                LOGGER.error("save offset to file error", e);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
            }
        }
    }

}
