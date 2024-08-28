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

package org.apache.inlong.manager.service.cluster.node;

import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeRequest;

public interface InlongClusterNodeInstallOperator {

    /**
     * Determines whether the current instance matches the specified type.
     *
     * @param clusterType cluster type
     */
    Boolean accept(String clusterType);

    String getClusterNodeType();

    /**
     * Installing cluster nodes.
     *
     * @param clusterNodeRequest cluster request
     * @param operator operator
     */
    boolean install(ClusterNodeRequest clusterNodeRequest, String operator);

    /**
     * ReInstalling cluster nodes.
     *
     * @param clusterNodeRequest cluster request
     * @param operator operator
     */
    boolean reInstall(ClusterNodeRequest clusterNodeRequest, String operator);

    /**
     * Uninstalling cluster nodes.
     *
     * @param clusterNodeEntity cluster entity
     * @param operator operator
     */
    boolean unload(InlongClusterNodeEntity clusterNodeEntity, String operator);

}
