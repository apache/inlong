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

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Random;

@Slf4j
public abstract class AbstractStandaloneSinkResourceOperator implements SinkResourceOperator {

    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;
    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private InlongGroupEntityMapper groupEntityMapper;
    @Autowired
    private StreamSinkService sinkService;

    private Random rand = new Random();

    @VisibleForTesting
    protected void assignCluster(SinkInfo sinkInfo) {
        try {
            if (StringUtils.isBlank(sinkInfo.getSinkType())) {
                throw new IllegalArgumentException(ErrorCodeEnum.SINK_TYPE_IS_NULL.getMessage());
            }

            if (StringUtils.isNotBlank(sinkInfo.getInlongClusterName())) {
                String info = "no need to auto-assign cluster since the cluster has already assigned";
                sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), info);
                return;
            }

            String targetCluster = assignOneCluster(sinkInfo);
            Preconditions.expectNotBlank(targetCluster,
                    String.format("find no proper cluster assign to group=%s, stream=%s, sink type=%s, data node=%s ",
                            sinkInfo.getInlongGroupId(), sinkInfo.getInlongStreamId(), sinkInfo.getSinkType(),
                            sinkInfo.getDataNodeName()));

            StreamSinkEntity sink = sinkEntityMapper.selectByPrimaryKey(sinkInfo.getId());
            sink.setInlongClusterName(targetCluster);
            sink.setStatus(SinkStatus.CONFIG_SUCCESSFUL.getCode());
            sinkEntityMapper.updateByIdSelective(sink);
        } catch (Throwable e) {
            String errMsg = "assign standalone cluster failed: " + e.getMessage();
            log.error(errMsg, e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }
    }

    protected void checkTaskAndConsumerGroup(SinkInfo sinkInfo) {
        if (!StringUtils.isAnyBlank(sinkInfo.getSortConsumerGroup(), sinkInfo.getSortTaskName())) {
            return;
        }
        StreamSinkEntity sink = sinkEntityMapper.selectByPrimaryKey(sinkInfo.getId());
        if (StringUtils.isBlank(sink.getSortConsumerGroup())) {
            sink.setSortConsumerGroup(sink.getDataNodeName());
        }
        if (StringUtils.isBlank(sink.getSortTaskName())) {
            sink.setSortTaskName(sink.getDataNodeName());
        }
        sinkEntityMapper.updateByIdSelective(sink);
    }

    private String assignOneCluster(SinkInfo sinkInfo) {
        InlongGroupEntity group = groupEntityMapper.selectByGroupId(sinkInfo.getInlongGroupId());
        return StringUtils
                .firstNonBlank(assignFromExist(sinkInfo.getDataNodeName(), group.getInlongClusterTag()),
                        assignFromRelated(sinkInfo.getSinkType(), group));
    }

    private String assignFromExist(String dataNodeName, String clusterTag) {
        return sinkEntityMapper.selectAssignedCluster(dataNodeName, clusterTag);
    }

    private String assignFromRelated(String sinkType, InlongGroupEntity group) {
        String sortClusterType = SinkType.relatedSortClusterType(sinkType);
        if (StringUtils.isBlank(sortClusterType)) {
            log.error("find no relate sort cluster type for sink type={}", sinkType);
            return null;
        }

        // if some clusters have the same tag
        List<InlongClusterEntity> clusters =
                clusterEntityMapper.selectByKey(group.getInlongClusterTag(), null, sortClusterType);

        return CollectionUtils.isEmpty(clusters) ? null : clusters.get(rand.nextInt(clusters.size())).getName();

    }

}
