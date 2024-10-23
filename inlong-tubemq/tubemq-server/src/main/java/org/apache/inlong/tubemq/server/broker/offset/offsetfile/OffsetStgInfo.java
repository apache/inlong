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

package org.apache.inlong.tubemq.server.broker.offset.offsetfile;

import org.apache.inlong.tubemq.corebase.TokenConstants;
import org.apache.inlong.tubemq.server.broker.offset.OffsetStorageInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetStgInfo {

    private long lstCommitTime;
    private final ConcurrentHashMap<String, PartStgInfo> partOffsetInfo = new ConcurrentHashMap<>();

    public OffsetStgInfo() {
    }

    public boolean updOffsetInfo(String topic, int partId, long lstTerm,
            long msgId, long offset, long lstUpdateTime) {
        boolean isAdded = false;
        String key = buildOffsetKey(topic, partId);
        PartStgInfo partInfo = partOffsetInfo.get(key);
        if (partInfo == null) {
            PartStgInfo tmpPartInfo = new PartStgInfo(topic, partId);
            partInfo = partOffsetInfo.putIfAbsent(key, tmpPartInfo);
            if (partInfo == null) {
                isAdded = true;
                partInfo = tmpPartInfo;
            }
        }
        partInfo.updateOffset(lstTerm, msgId, isAdded,
                offset, lstUpdateTime, offset, lstUpdateTime);
        this.lstCommitTime = lstUpdateTime;
        return isAdded;
    }

    public boolean updOffsetInfo(OffsetStorageInfo info) {
        boolean isAdded = false;
        String key = buildOffsetKey(info.getTopic(), info.getPartitionId());
        PartStgInfo partInfo = this.partOffsetInfo.get(key);
        if (partInfo == null) {
            PartStgInfo tmpPartInfo = new PartStgInfo(info.getTopic(), info.getPartitionId());
            partInfo = this.partOffsetInfo.putIfAbsent(key, tmpPartInfo);
            if (partInfo == null) {
                isAdded = true;
                partInfo = tmpPartInfo;
            }
        }
        partInfo.updateOffset(info.getLstRstTerm(), info.getMessageId(), isAdded,
                info.getFirstOffset(), info.getCreateTime(), info.getOffset(), info.getLstUpdateTime());
        this.lstCommitTime = info.getLstUpdateTime();
        return isAdded;
    }

    public boolean resetOffsetInfo(String topic, int partId, long resetTerm, long offset, long lstUpdateTime) {
        boolean isAdded = false;
        String key = buildOffsetKey(topic, partId);
        PartStgInfo partInfo = this.partOffsetInfo.get(key);
        if (partInfo == null) {
            PartStgInfo tmpPartInfo = new PartStgInfo(topic, partId);
            partInfo = this.partOffsetInfo.putIfAbsent(key, tmpPartInfo);
            if (partInfo == null) {
                isAdded = true;
                partInfo = tmpPartInfo;
            }
        }
        partInfo.resetOffset(resetTerm, isAdded, offset, lstUpdateTime);
        this.lstCommitTime = lstUpdateTime;
        return isAdded;
    }

    public long getLstCommitTime() {
        return lstCommitTime;
    }

    public boolean isOffsetStgInfoEmpty() {
        return partOffsetInfo.isEmpty();
    }

    public boolean includedTopic(String topic) {
        if (partOffsetInfo.isEmpty()) {
            return false;
        }
        for (PartStgInfo partStgInfo : partOffsetInfo.values()) {
            if (partStgInfo == null) {
                continue;
            }
            if (topic.equals(partStgInfo.getTopic())) {
                return true;
            }
        }
        return false;
    }

    public PartStgInfo getPartOffsetInfo(String topic, int partId) {
        return partOffsetInfo.get(buildOffsetKey(topic, partId));
    }

    public ConcurrentHashMap<String, PartStgInfo> getPartOffsetInfo() {
        return partOffsetInfo;
    }

    public Map<Integer, Long> getPartOffsetInfo(String topic, Set<Integer> partIds) {
        Map<Integer, Long> offsetMap = new HashMap<>(partIds.size());
        for (Integer partId : partIds) {
            PartStgInfo partInfo = partOffsetInfo.get(buildOffsetKey(topic, partId));
            if (partInfo == null) {
                offsetMap.put(partId, null);
            } else {
                offsetMap.put(partId, partInfo.getLstOffset());
            }
        }
        return offsetMap;
    }

    public boolean rmvPartOffsetInfo(String topic, Set<Integer> partIds) {
        for (Integer partId : partIds) {
            partOffsetInfo.remove(buildOffsetKey(topic, partId));
        }
        return partOffsetInfo.isEmpty();
    }

    public boolean rmvPartOffsetInfo(Set<String> rmvTopics) {
        Set<String> rmvItems = new HashSet<>();
        for (Map.Entry<String, PartStgInfo> entry : partOffsetInfo.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (rmvTopics.contains(entry.getValue().getTopic())) {
                rmvItems.add(buildOffsetKey(entry.getValue().getTopic(), entry.getValue().getPartId()));
            }
        }
        for (String rmvItem : rmvItems) {
            partOffsetInfo.remove(rmvItem);
        }
        return partOffsetInfo.isEmpty();
    }

    @Override
    public String toString() {
        return "OffsetStgInfo{" +
                "lstCommitTime=" + lstCommitTime +
                ", partOffsetInfo=" + partOffsetInfo +
                '}';
    }

    public static String buildOffsetKey(String topic, int partId) {
        return topic + TokenConstants.HYPHEN + partId;
    }
}
