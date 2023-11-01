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

package org.apache.inlong.manager.service.resource.sink;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.sink.SinkInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class AbstractStandaloneSinkResourceOperator implements SinkResourceOperator {

    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;
    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private InlongGroupEntityMapper groupEntityMapper;

    private static final String SORT_PREFIX = "SORT_";

    private Random rand = new Random();

    @VisibleForTesting
    protected void assignCluster(SinkInfo sinkInfo) {
        if (StringUtils.isNotBlank(sinkInfo.getInlongClusterName())) {
            return;
        }

        String targetCluster = assignOneCluster(sinkInfo);
        Preconditions.expectNotBlank(targetCluster,
                String.format("find no proper cluster assign to group=%s, stream=%s, sink type=%s, data node=%s ",
                        sinkInfo.getInlongGroupId(), sinkInfo.getInlongStreamId(), sinkInfo.getSinkType(),
                        sinkInfo.getDataNodeName()));

        StreamSinkEntity sink = sinkEntityMapper.selectByPrimaryKey(sinkInfo.getId());
        sink.setInlongClusterName(targetCluster);
        sinkEntityMapper.updateByIdSelective(sink);
    }

    private String assignOneCluster(SinkInfo sinkInfo) {
        return StringUtils
                .firstNonBlank(assignFromExist(sinkInfo.getDataNodeName()),
                        assignFromRelated(sinkInfo.getSinkType(), sinkInfo.getInlongGroupId()));
    }

    private String assignFromExist(String dataNodeName) {
        return sinkEntityMapper.selectAssignedCluster(dataNodeName);
    }

    private String assignFromRelated(String sinkType, String groupId) {
        InlongGroupEntity group = groupEntityMapper.selectByGroupId(groupId);
        String sortClusterType = SORT_PREFIX.concat(sinkType);
        List<InlongClusterEntity> clusters = clusterEntityMapper
                .selectSortClusterByType(sortClusterType).stream()
                .filter(cluster -> checkCluster(cluster.getClusterTags(), group.getInlongClusterTag()))
                .collect(Collectors.toList());

        return CollectionUtils.isEmpty(clusters) ? null : clusters.get(rand.nextInt(clusters.size())).getName();

    }

    private boolean checkCluster(String clusterTags, String targetTag) {
        return StringUtils.isBlank(clusterTags)
                || Sets.newHashSet(clusterTags.split(InlongConstants.COMMA)).contains(targetTag);
    }

}
