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

package org.apache.inlong.manager.service.sort.util;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.sort.protocol.node.LoadNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;

import java.util.List;
import java.util.stream.Collectors;

public class LoadNodeUtils {

    public static List<LoadNode> createLoadNodes(List<SinkResponse> sinkResponses) {
        if (CollectionUtils.isEmpty(sinkResponses)) {
            return Lists.newArrayList();
        }
        return sinkResponses.stream().map(sourceResponse -> createLoadNode(sourceResponse))
                .collect(Collectors.toList());
    }

    public static LoadNode createLoadNode(SinkResponse sinkResponse) {
        SinkType sinkType = SinkType.forType(sinkResponse.getSinkType());
        switch (sinkType) {
            case KAFKA:
                return createLoadNode((KafkaSinkResponse) sinkResponse);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sinkType=%s to create loadNode", sinkType));
        }
    }

    public static KafkaLoadNode createLoadNode(KafkaSinkResponse kafkaSinkResponse) {
        //todo
        return null;
    }
}
