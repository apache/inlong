/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.broker.offset;

import java.util.Map;
import java.util.Set;
import org.apache.tubemq.server.broker.msgstore.MessageStore;
import org.apache.tubemq.server.broker.msgstore.MessageStoreManager;
import org.apache.tubemq.server.common.offsetstorage.OffsetStorageInfo;


/***
 * Offset manager service interface.
 */
public interface OffsetService {

    void close(long waitTimeMs);

    OffsetStorageInfo loadOffset(final MessageStore store, final String group,
                                 final String topic, int partitionId,
                                 int readStatus, long reqOffset,
                                 final StringBuilder sb);

    long getOffset(final MessageStore msgStore, final String group,
                   final String topic, int partitionId,
                   boolean isManCommit, boolean lastConsumed,
                   final StringBuilder sb);

    long getOffset(String group, String topic, int partitionId);

    void bookOffset(final String group, final String topic, int partitionId,
                    int readDalt, boolean isManCommit, boolean isMsgEmpty,
                    final StringBuilder sb);

    long commitOffset(final String group, final String topic,
                      int partitionId, boolean isConsumed);

    long resetOffset(final MessageStore store, final String group, final String topic,
                     int partitionId, long reSetOffset, final String modifier);

    long getTmpOffset(final String group, final String topic, int partitionId);

    Set<String> getBookedGroups();

    Set<String> getInMemoryGroups();

    Set<String> getUnusedGroupInfo();

    Set<String> getGroupSubInfo(String group);

    Map<String, Map<Integer, Long>> queryGroupOffset(
            String group, Map<String, Set<Integer>> topicPartMap);

    boolean modifyGroupOffset(MessageStoreManager storeManager, Set<String> groups,
                              Map<String, Map<Integer, Long>> topicPartOffsetMap,
                              String modifier);
}
