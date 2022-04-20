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

package org.apache.inlong.manager.common.thirdparty.sort;

import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;

import java.util.List;
import java.util.Map;

/**
 * Interface of source info generator.
 * Use to generate user-defined source info.
 */
public interface SourceInfoGenerator {

    /**
     * Method to create customized source info.
     *
     * @param pulsarCluster pulsarCluster
     * @param masterAddress pulsarCluster
     * @param clusterBean clusterBean
     * @param groupInfo groupInfo
     * @param streamInfo streamInfo
     * @param sourceResponse sourceResponse
     * @param sourceFields sourceFields
     * @return Customized source info
     */
    SourceInfo createSourceInfo(
            PulsarClusterInfo pulsarCluster,
            String masterAddress,
            ClusterBean clusterBean,
            InlongGroupInfo groupInfo,
            InlongStreamInfo streamInfo,
            SourceResponse sourceResponse,
            List<FieldInfo> sourceFields,
            Map<String, Object> properties);

    /**
     * Whether the source is all binlog migration.
     */
    boolean isBinlogAllMigration(SourceResponse sourceResponse);
}
