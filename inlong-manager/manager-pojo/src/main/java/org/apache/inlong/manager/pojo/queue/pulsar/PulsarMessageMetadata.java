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

package org.apache.inlong.manager.pojo.queue.pulsar;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarMessageMetadata {

    private int payloadSize;
    private String partitionKey;
    private boolean compactedOut;
    private long eventTime;
    private boolean partitionKeyB64Encoded;
    private byte[] orderingKey;
    private long sequenceId;
    private boolean nullValue;
    private boolean nullPartitionKey;
    private Map<String, String> properties;
    private long publishTime;
    private long deliverAtTime;
    private int markerType;
    private long txnidLeastBits;
    private long txnidMostBits;
    private long highestSequenceId;
    private String uuid;
    private int numChunksFromMsg;
    private int totalChunkMsgSize;
    private int chunkId;
    private String producerName;
    private String replicatedFrom;
    private int uncompressedSize;
    private int numMessagesInBatch;
    private String encryptionAlgo;
    private String compression;
    private byte[] encryptionParam;
    private byte[] schemaVersion;
    private List<String> replicateTos;
}
