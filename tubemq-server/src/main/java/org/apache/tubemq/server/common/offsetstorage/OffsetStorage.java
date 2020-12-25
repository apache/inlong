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

package org.apache.tubemq.server.common.offsetstorage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;


public interface OffsetStorage {

    void close();

    OffsetStorageInfo loadOffset(final String group,
                                 final String topic, int partitionId);

    void commitOffset(final String group,
                      final Collection<OffsetStorageInfo> offsetInfoList,
                      boolean isFailRetry);

    Map<String, Map<String, Set<String>>> getZkGroupTopicBrokerInfos();

    Map<String, Set<String>> getZkLocalGroupTopicInfos();

    Map<Integer, Long> queryGroupOffsetInfo(String group, String topic,
                                            Set<Integer> partitionIds);
}
