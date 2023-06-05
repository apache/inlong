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

package org.apache.inlong.manager.pojo.sort.node;

import org.apache.inlong.manager.pojo.sort.util.FieldInfoUtils;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.pojo.stream.StreamNode;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.ExtractNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Interface of the extract node provider
 */
public interface ExtractNodeProvider extends NodeProvider {

    /**
     * Determines whether the current instance matches the specified type.
     *
     * @param sourceType the specified source type
     * @return whether the current instance matches the specified type
     */
    Boolean accept(String sourceType);

    /**
     * Create extract node by stream node info
     *
     * @param nodeInfo stream node info
     * @return extract node
     */
    ExtractNode createNode(StreamNode nodeInfo);

    /**
     * Parse FieldInfos
     *
     * @param streamFields The stream fields
     * @param nodeId The node id
     * @return FieldInfo list
     */
    default List<FieldInfo> parseFieldInfos(List<StreamField> streamFields, String nodeId) {
        // Filter constant fields
        return streamFields.stream().filter(s -> Objects.isNull(s.getFieldValue()))
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, nodeId))
                .collect(Collectors.toList());
    }
}
