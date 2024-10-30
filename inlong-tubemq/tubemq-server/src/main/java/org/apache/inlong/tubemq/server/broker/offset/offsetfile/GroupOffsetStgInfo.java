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

import org.apache.inlong.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.inlong.tubemq.server.broker.offset.OffsetStorageInfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class GroupOffsetStgInfo {

    private final AtomicLong lstStoreTime = new AtomicLong(0);
    private final int brokerId;
    private final ConcurrentHashMap<String, OffsetStgInfo> groupOffsetStgInfo = new ConcurrentHashMap<>();

    public GroupOffsetStgInfo(int brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isStgInfoEmpty() {
        return groupOffsetStgInfo.isEmpty();
    }

    public boolean storeOffsetStgInfo(String group, Collection<OffsetStorageInfo> offsetInfoList,
            ConcurrentHashMap<String, ConcurrentHashSet<String>> groupTopicsInfo) {
        OffsetStgInfo curOffsetStgInfo = groupOffsetStgInfo.get(group);
        if (curOffsetStgInfo == null) {
            OffsetStgInfo tmpOffsetStgInfo = new OffsetStgInfo();
            curOffsetStgInfo = groupOffsetStgInfo.putIfAbsent(group, tmpOffsetStgInfo);
            if (curOffsetStgInfo == null) {
                curOffsetStgInfo = tmpOffsetStgInfo;
            }
        }
        boolean updated = false;
        ConcurrentHashSet<String> tmpSet;
        ConcurrentHashSet<String> topicSet;
        for (OffsetStorageInfo info : offsetInfoList) {
            if (info == null || !info.isModified()) {
                continue;
            }
            if (curOffsetStgInfo.updOffsetInfo(info)) {
                topicSet = groupTopicsInfo.get(group);
                if (topicSet == null) {
                    tmpSet = new ConcurrentHashSet<>();
                    topicSet = groupTopicsInfo.putIfAbsent(group, tmpSet);
                    if (topicSet == null) {
                        topicSet = tmpSet;
                    }
                }
                topicSet.add(info.getTopic());
            }
            updated = true;
            info.setModified(false);
        }
        return updated;
    }

    public boolean addOffsetStgInfo(String group, String topic, int partId, long offset, long msgId) {
        OffsetStgInfo curOffsetStgInfo = groupOffsetStgInfo.get(group);
        if (curOffsetStgInfo == null) {
            OffsetStgInfo tmpOffsetStgInfo = new OffsetStgInfo();
            curOffsetStgInfo = groupOffsetStgInfo.putIfAbsent(group, tmpOffsetStgInfo);
            if (curOffsetStgInfo == null) {
                curOffsetStgInfo = tmpOffsetStgInfo;
            }
        }
        return curOffsetStgInfo.updOffsetInfo(topic, partId, 0, msgId, offset, System.currentTimeMillis());
    }

    public Map<Integer, Long> queryGroupOffsetInfo(String group, String topic, Set<Integer> partIds) {
        OffsetStgInfo offsetStgInfo = groupOffsetStgInfo.get(group);
        if (offsetStgInfo == null) {
            Map<Integer, Long> offsetMap = new HashMap<>(partIds.size());
            for (Integer partId : partIds) {
                offsetMap.put(partId, null);
            }
            return offsetMap;
        }
        return offsetStgInfo.getPartOffsetInfo(topic, partIds);
    }

    public boolean rmvGroupOffsetInfo(String group, Map<String, Set<Integer>> topicParts) {
        OffsetStgInfo offsetStgInfo = groupOffsetStgInfo.get(group);
        if (offsetStgInfo == null) {
            return false;
        }
        for (Map.Entry<String, Set<Integer>> entry : topicParts.entrySet()) {
            if (entry == null
                    || entry.getKey() == null
                    || entry.getValue() == null
                    || entry.getValue().isEmpty()) {
                continue;
            }
            offsetStgInfo.rmvPartOffsetInfo(entry.getKey(), entry.getValue());
        }
        if (offsetStgInfo.isOffsetStgInfoEmpty()) {
            groupOffsetStgInfo.remove(group);
        }
        return true;
    }

    public void rmvGroupOffsetInfo(String group, Set<String> rmvTopics) {
        OffsetStgInfo offsetStgInfo = groupOffsetStgInfo.get(group);
        if (offsetStgInfo == null) {
            return;
        }
        if (offsetStgInfo.rmvPartOffsetInfo(rmvTopics)) {
            groupOffsetStgInfo.remove(group);
        }
    }

    public ConcurrentHashMap<String, OffsetStgInfo> getGroupOffsetStgInfo() {
        return groupOffsetStgInfo;
    }

    public boolean includedTopic(String group, String topic) {
        OffsetStgInfo offsetStgInfo = groupOffsetStgInfo.get(group);
        if (offsetStgInfo == null) {
            return false;
        }
        return offsetStgInfo.includedTopic(topic);
    }

    public Map<String, PartStgInfo> getOffsetStgInfos(String group) {
        OffsetStgInfo offsetStgInfo = groupOffsetStgInfo.get(group);
        if (offsetStgInfo == null) {
            return null;
        }
        return offsetStgInfo.getPartOffsetInfo();
    }

    public PartStgInfo getOffsetStgInfo(String group, String topic, int partId) {
        OffsetStgInfo offsetStgInfo = groupOffsetStgInfo.get(group);
        if (offsetStgInfo == null) {
            return null;
        }
        return offsetStgInfo.getPartOffsetInfo(topic, partId);
    }

    public Set<String> rmvExpiredGroupOffsetInfo(long chkTime, long expireDurMs) {
        Set<String> rmvGroups = new HashSet<>();
        for (Map.Entry<String, OffsetStgInfo> entry : groupOffsetStgInfo.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (chkTime - entry.getValue().getLstCommitTime() > expireDurMs) {
                rmvGroups.add(entry.getKey());
            }
        }
        if (!rmvGroups.isEmpty()) {
            for (String group : rmvGroups) {
                if (group == null) {
                    continue;
                }
                groupOffsetStgInfo.remove(group);
            }
        }
        return rmvGroups;
    }

    public void setLstStoreTime() {
        lstStoreTime.set(System.currentTimeMillis());
    }

    public void clear() {
        this.groupOffsetStgInfo.clear();
    }

    @Override
    public String toString() {
        return "GroupOffsetStgInfo{" +
                "lstStoreTime=" + lstStoreTime +
                ", brokerId=" + brokerId +
                ", groupOffsetStgInfo=" + groupOffsetStgInfo +
                '}';
    }
}
