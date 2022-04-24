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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.util.StreamParseUtils;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class NodeRelationShipUtils {

    public static List<NodeRelationShip> createNodeRelationShipsForStream(InlongStreamInfo streamInfo) {
        String tempView = streamInfo.getTempView();
        if (StringUtils.isEmpty(tempView)) {
            log.warn("StreamNodeRelationShip is empty for Stream={}", streamInfo);
            return Lists.newArrayList();
        }
        StreamPipeline pipeline = StreamParseUtils.parseStreamPipeline(streamInfo.getTempView(),
                streamInfo.getInlongStreamId());
        //todo join Node
        List<NodeRelationShip> nodeRelationShips = pipeline.getPipeline().stream()
                .map(streamNodeRelationShip -> new NodeRelationShip(
                        Lists.newArrayList(streamNodeRelationShip.getInputNodes()),
                        Lists.newArrayList(streamNodeRelationShip.getOutputNodes())))
                .collect(
                        Collectors.toList());
        return nodeRelationShips;
    }

}
