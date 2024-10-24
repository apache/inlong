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

package org.apache.inlong.audit.send;

import org.apache.inlong.audit.protocol.AuditApi;
import org.apache.inlong.audit.util.AuditConfig;
import org.apache.inlong.audit.util.AuditData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Audit sender manager
 */
public class SenderManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderManager.class);
    private static final int SEND_INTERVAL_MS = 100;
    private final ConcurrentHashMap<Long, AuditData> failedDataMap = new ConcurrentHashMap<>();
    private AuditConfig auditConfig;
    private Socket socket = new Socket();
    private static final int PACKAGE_HEADER_LEN = 4;
    private static final int MAX_RESPONSE_LENGTH = 32 * 1024;
    private static final AtomicLong globalAuditMemory = new AtomicLong(0);
    private static long maxGlobalAuditMemory = 200 * 1024 * 1024;

    public static void setMaxGlobalAuditMemory(long maxGlobalAuditMemory) {
        SenderManager.maxGlobalAuditMemory = maxGlobalAuditMemory;
    }

    public SenderManager(AuditConfig config) {
        auditConfig = config;
    }

    public void closeSocket() {
        if (socket.isClosed()) {
            LOGGER.info("Audit socket is already closed");
            return;
        }
        try {
            socket.close();
            LOGGER.info("Audit socket closed successfully");
        } catch (IOException exception) {
            LOGGER.error("Error closing audit socket", exception);
        }
    }

    public boolean checkSocket() {
        if (socket.isClosed() || !socket.isConnected()) {
            try {
                InetSocketAddress inetSocketAddress = ProxyManager.getInstance().getInetSocketAddress();
                if (inetSocketAddress == null) {
                    LOGGER.error("Audit proxy address is null!");
                    return false;
                }
                reconnect(inetSocketAddress, auditConfig.getSocketTimeout());
            } catch (IOException exception) {
                LOGGER.error("Connect to audit proxy {} has exception!", socket.getInetAddress(), exception);
                return false;
            }
        }
        return socket.isConnected();
    }

    private void reconnect(InetSocketAddress inetSocketAddress, int timeout)
            throws IOException {
        socket = new Socket();
        socket.connect(inetSocketAddress, timeout);
        socket.setSoTimeout(timeout);
    }

    /**
     * Send data with command
     */
    public boolean send(AuditApi.BaseCommand baseCommand, AuditApi.AuditRequest auditRequest) {
        AuditData data = new AuditData(baseCommand, auditRequest);
        for (int retry = 0; retry < auditConfig.getRetryTimes(); retry++) {
            if (sendData(data.getDataByte())) {
                return true;
            }
            LOGGER.warn("Failed to send data on attempt {}. Retrying...", retry + 1);
            sleep();
        }

        LOGGER.error("Failed to send data after {} attempts. Storing data for later retry.",
                auditConfig.getRetryTimes());
        failedDataMap.putIfAbsent(baseCommand.getAuditRequest().getRequestId(), data);
        return false;
    }

    private void readFully(InputStream is, byte[] buffer, int len) throws IOException {
        int bytesRead;
        int totalBytesRead = 0;
        while (totalBytesRead < len
                && (bytesRead = is.read(buffer, totalBytesRead, len - totalBytesRead)) != -1) {
            totalBytesRead += bytesRead;
        }
    }

    /**
     * Send data byte array
     */
    private boolean sendData(byte[] data) {
        if (data == null || data.length == 0) {
            LOGGER.warn("Send data is empty!");
            return false;
        }
        if (!checkSocket()) {
            return false;
        }
        try {
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();

            outputStream.write(data);

            byte[] header = new byte[PACKAGE_HEADER_LEN];
            readFully(inputStream, header, PACKAGE_HEADER_LEN);

            int bodyLen = ((header[0] & 0xFF) << 24) |
                    ((header[1] & 0xFF) << 16) |
                    ((header[2] & 0xFF) << 8) |
                    (header[3] & 0xFF);

            if (bodyLen > MAX_RESPONSE_LENGTH) {
                closeSocket();
                return false;
            }

            byte[] body = new byte[bodyLen];
            readFully(inputStream, body, bodyLen);

            AuditApi.BaseCommand reply = AuditApi.BaseCommand.parseFrom(body);
            return AuditApi.AuditReply.RSP_CODE.SUCCESS.equals(reply.getAuditReply().getRspCode());
        } catch (IOException exception) {
            closeSocket();
            LOGGER.error("Send audit data to proxy has exception!", exception);
            return false;
        }
    }

    /**
     * Clean up the backlog of unsent message packets
     */
    public void checkFailedData() {
        LOGGER.info("Audit failed cache size: {}", failedDataMap.size());

        Iterator<Map.Entry<Long, AuditData>> iterator = failedDataMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, AuditData> entry = iterator.next();
            if (sendData(entry.getValue().getDataByte())) {
                iterator.remove();
                sleep();
            }
        }
        if (failedDataMap.isEmpty()) {
            checkAuditFile();
        }

        long failedDataSize = getFailedDataSize();
        globalAuditMemory.addAndGet(failedDataSize);

        if (failedDataMap.size() > auditConfig.getMaxCacheRow()
                || globalAuditMemory.get() > maxGlobalAuditMemory) {
            LOGGER.warn("Failed cache [size: {}, threshold {}], [count {}, threshold: {}]",
                    globalAuditMemory.get(), maxGlobalAuditMemory,
                    failedDataMap.size(), auditConfig.getMaxCacheRow());

            writeLocalFile();

            failedDataMap.clear();
        }

        globalAuditMemory.addAndGet(-failedDataSize);
    }

    /**
     * write local file
     */
    private void writeLocalFile() {
        if (!checkFilePath()) {
            LOGGER.error("{} is not exist!", auditConfig.getFilePath());
            return;
        }

        File file = new File(auditConfig.getDisasterFile());

        try {
            if (file.exists()) {
                if (file.length() > auditConfig.getMaxFileSize()) {
                    if (!file.delete()) {
                        LOGGER.error("Failed to delete file: {}", file.getAbsolutePath());
                        return;
                    }
                    LOGGER.info("Deleted file due to exceeding max file size: {}", file.getAbsolutePath());
                }
            } else {
                if (!file.createNewFile()) {
                    LOGGER.error("Failed to create file: {}", file.getAbsolutePath());
                    return;
                }
                LOGGER.info("Created file: {}", file.getAbsolutePath());
            }

            try (FileOutputStream fos = new FileOutputStream(file);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(fos)) {

                objectOutputStream.writeObject(failedDataMap);
            }
        } catch (IOException e) {
            LOGGER.error("Error writing to local file: {}", e.getMessage(), e);
        }
    }

    /**
     * check file path
     */
    private boolean checkFilePath() {
        File file = new File(auditConfig.getFilePath());
        if (!file.exists()) {
            if (!file.mkdirs()) {
                LOGGER.error("Create file {} failed!", auditConfig.getFilePath());
                return false;
            }
            LOGGER.info("Create file {} success", auditConfig.getFilePath());
        }
        return true;
    }

    /**
     * check audit file
     */
    private void checkAuditFile() {
        File file = new File(auditConfig.getDisasterFile());
        if (!file.exists()) {
            return;
        }

        try (FileInputStream inputStream = new FileInputStream(file);
                ObjectInputStream objectStream = new ObjectInputStream(inputStream)) {

            ConcurrentHashMap<Long, AuditData> fileData = (ConcurrentHashMap<Long, AuditData>) objectStream
                    .readObject();

            for (Map.Entry<Long, AuditData> entry : fileData.entrySet()) {
                if (!sendData(entry.getValue().getDataByte())) {
                    LOGGER.error("Local file recovery failed: {}", entry.getValue());
                }
                sleep();
            }
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("check audit file error:{}", e.getMessage(), e);
        } finally {
            if (!file.delete()) {
                LOGGER.error("Failed to delete file: {}", file.getAbsolutePath());
            }
        }
    }

    /**
     * get data map size
     */
    public int getDataMapSize() {
        return this.failedDataMap.size();
    }

    /**
     * sleep SEND_INTERVAL_MS
     */
    private void sleep() {
        try {
            Thread.sleep(SEND_INTERVAL_MS);
        } catch (Throwable ex) {
            LOGGER.error("sleep error:{}", ex.getMessage(), ex);
        }
    }

    /***
     * set audit config
     */
    public void setAuditConfig(AuditConfig config) {
        auditConfig = config;
    }

    private long getFailedDataSize() {
        long dataSize = 0;
        for (AuditData auditData : failedDataMap.values()) {
            dataSize += auditData.getDataByte().length;
        }
        return dataSize;
    }
}
