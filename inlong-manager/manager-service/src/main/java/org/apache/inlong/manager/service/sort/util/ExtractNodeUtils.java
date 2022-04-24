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
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;

import java.util.List;
import java.util.stream.Collectors;

public class ExtractNodeUtils {

    public static List<ExtractNode> createExtractNodes(List<SourceResponse> sourceResponses) {
        if (CollectionUtils.isEmpty(sourceResponses)) {
            return Lists.newArrayList();
        }
        return sourceResponses.stream().map(sourceResponse -> createExtractNode(sourceResponse))
                .collect(Collectors.toList());
    }

    public static ExtractNode createExtractNode(SourceResponse sourceResponse) {
        SourceType sourceType = SourceType.forType(sourceResponse.getSourceType());
        switch (sourceType) {
            case BINLOG:
                return createExtractNode((BinlogSourceResponse) sourceResponse);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported sourceType=%s to create extractNode", sourceType));
        }
    }

    public static MySqlExtractNode createExtractNode(BinlogSourceResponse binlogSourceResponse) {
        String id = binlogSourceResponse.getSourceName();
        String name = binlogSourceResponse.getSourceName();
        String database = binlogSourceResponse.getDatabaseWhiteList();
        //todo
        return null;
    }
}
