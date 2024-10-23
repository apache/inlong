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

package org.apache.inlong.tubemq.server.broker.offset;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public interface OffsetStorage {

    void close();

    ConcurrentHashMap<String, OffsetStorageInfo> loadGroupStgInfo(String group);

    OffsetStorageInfo loadOffset(String group, String topic, int partitionId);

    boolean commitOffset(String group, Collection<OffsetStorageInfo> offsetInfoList, boolean isFailRetry);

    Map<String, Set<String>> queryGroupTopicInfo(Set<String> groups);

    Map<Integer, Long> queryGroupOffsetInfo(String group, String topic,
            Set<Integer> partitionIds);

    void deleteGroupOffsetInfo(Map<String, Map<String, Set<Integer>>> groupTopicPartMap);

    Set<String> cleanExpiredGroupInfo(long checkTime, long expiredDurMs);

    Set<String> cleanRmvTopicInfo(Set<String> rmvTopics);
}
