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

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.cluster.sort.cls.SortClsClusterRequest;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.cluster.InlongClusterService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class StandaloneAutoAssignTest extends ServiceBaseTest {

    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private TestStandaloneSinkResourceOperator testResourceOperator;

    @Test
    public void testAutoAssign() {

        String group = "autoGroup";
        String stream = "autoStream";
        InlongGroupInfo groupInfo = this.createInlongGroup(group, MQType.PULSAR);
        InlongStreamInfo streamInfo = this.createStreamInfo(groupInfo, stream);
        Integer id = saveClsSink(groupInfo.getInlongGroupId(), streamInfo.getInlongStreamId());

        String clusterName = "clsCluster";
        saveStandaloneCluster(groupInfo.getInlongClusterTag(), clusterName);

        List<SinkInfo> sinkInfos = sinkEntityMapper.selectAllConfig(groupInfo.getInlongGroupId(), null);
        Assertions.assertEquals(1, sinkInfos.size());
        SinkInfo clsSinkInfo = sinkInfos.get(0);

        testResourceOperator.assignCluster(clsSinkInfo);

        StreamSinkEntity newClsEntity = sinkEntityMapper.selectByPrimaryKey(id);
        Assertions.assertEquals(clusterName, newClsEntity.getInlongClusterName());

    }

    public Integer saveClsSink(String groupId, String streamId) {
        StreamSinkEntity clsSinkEntity = new StreamSinkEntity();
        clsSinkEntity.setDataNodeName("testNode");
        clsSinkEntity.setSinkType(SinkType.CLS);
        clsSinkEntity.setSinkName("testClsSink");
        clsSinkEntity.setInlongGroupId(groupId);
        clsSinkEntity.setInlongStreamId(streamId);
        clsSinkEntity.setCreator(GLOBAL_OPERATOR);
        sinkEntityMapper.insert(clsSinkEntity);
        return clsSinkEntity.getId();
    }

    public Integer saveStandaloneCluster(String clusterTag, String clusterName) {
        SortClsClusterRequest request = new SortClsClusterRequest();
        request.setClusterTags(clusterTag);
        request.setName(clusterName);
        request.setInCharges(GLOBAL_OPERATOR);
        return clusterService.save(request, GLOBAL_OPERATOR);
    }

}
