/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.manager.plugin.sort;

import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.thirdparty.sort.SourceInfoGenerator;
import org.apache.inlong.manager.service.sort.util.SourceInfoUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@ConditionalOnProperty(name = "type", prefix = "inlong.sort.sourceinfo", havingValue = "default")
@Component
public class SourceInfoGeneratorImpl implements SourceInfoGenerator {
    @Override
    public SourceInfo createSourceInfo(
            PulsarClusterInfo pulsarCluster,
            String masterAddress,
            ClusterBean clusterBean,
            InlongGroupInfo groupInfo,
            InlongStreamInfo streamInfo,
            SourceResponse sourceResponse,
            List<FieldInfo> sourceFields,
            Map<String, Object> properties) {
        return SourceInfoUtils.createSourceInfo(pulsarCluster, masterAddress,
                clusterBean, groupInfo, streamInfo, sourceResponse, sourceFields);
    }

    @Override
    public boolean isBinlogAllMigration(SourceResponse sourceResponse) {
        return SourceInfoUtils.isBinlogAllMigration(sourceResponse);
    }
}
