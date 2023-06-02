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

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Abstract Extract Node Factory
 */
@Slf4j
public abstract class ExtractNodeFactory implements AbstractNodeFactory {

    /**
     * Create extract node by stream node info
     *
     * @param nodeInfo stream node info
     * @return extract node
     */
    public abstract ExtractNode createNode(StreamNode nodeInfo);

    /**
     * Parse FieldInfos
     *
     * @param streamFields The stream fields
     * @param nodeId The node id
     * @return FieldInfo list
     */
    protected static List<FieldInfo> parseFieldInfos(List<StreamField> streamFields, String nodeId) {
        // Filter constant fields
        return streamFields.stream().filter(s -> Objects.isNull(s.getFieldValue()))
                .map(streamFieldInfo -> FieldInfoUtils.parseStreamFieldInfo(streamFieldInfo, nodeId))
                .collect(Collectors.toList());
    }
}
