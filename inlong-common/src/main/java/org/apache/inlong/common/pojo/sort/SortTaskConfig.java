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

package org.apache.inlong.common.pojo.sort;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.inlong.common.pojo.sort.node.NodeConfig;
import org.apache.inlong.common.util.SortConfigUtil;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SortTaskConfig implements Serializable {

    private String sortTaskName;
    private List<SortClusterConfig> clusters;
    private NodeConfig nodeConfig;

    public static List<SortTaskConfig> batchCheckDelete(List<SortTaskConfig> last, List<SortTaskConfig> current) {
        return SortConfigUtil.batchCheckDeleteRecursive(last, current,
                SortTaskConfig::getSortTaskName, SortTaskConfig::checkDelete);
    }

    public static List<SortTaskConfig> batchCheckUpdate(List<SortTaskConfig> last, List<SortTaskConfig> current) {
        return SortConfigUtil.batchCheckUpdateRecursive(last, current,
                SortTaskConfig::getSortTaskName, SortTaskConfig::checkUpdate);
    }

    public static List<SortTaskConfig> batchCheckNew(List<SortTaskConfig> last, List<SortTaskConfig> current) {
        return SortConfigUtil.batchCheckNewRecursive(last, current,
                SortTaskConfig::getSortTaskName, SortTaskConfig::checkNew);
    }

    public static List<SortTaskConfig> batchCheckLatest(List<SortTaskConfig> latest, List<SortTaskConfig> current) {
        return SortConfigUtil.batchCheckLatestRecursive(latest, current,
                SortTaskConfig::getSortTaskName, SortTaskConfig::checkLatest);
    }

    public static SortTaskConfig checkDelete(SortTaskConfig last, SortTaskConfig current) {
        return check(last, current, SortClusterConfig::batchCheckDelete,
                (lastNode, currentNode) -> lastNode);
    }

    public static SortTaskConfig checkUpdate(SortTaskConfig last, SortTaskConfig current) {
        return check(last, current, SortClusterConfig::batchCheckUpdate,
                (lastNode, currentNode) -> {
                    if (lastNode == null || currentNode == null) {
                        return null;
                    }
                    return lastNode.getVersion() < currentNode.getVersion() ? null : currentNode;
                });
    }

    public static SortTaskConfig checkNew(SortTaskConfig last, SortTaskConfig current) {
        return check(last, current, SortClusterConfig::batchCheckNew,
                (lastNode, currentNode) -> {
                    if (lastNode == null || currentNode == null) {
                        return null;
                    }
                    return lastNode.getVersion() >= currentNode.getVersion() ? lastNode : currentNode;
                });
    }

    public static SortTaskConfig checkLatest(SortTaskConfig last, SortTaskConfig current) {
        return check(last, current, SortClusterConfig::batchCheckLatest,
                (lastNode, currentNode) -> {
                    if (lastNode == null || currentNode == null) {
                        return null;
                    }
                    return lastNode.getVersion() >= currentNode.getVersion() ? lastNode : currentNode;
                });
    }

    public static SortTaskConfig check(
            SortTaskConfig last, SortTaskConfig current,
            BiFunction<List<SortClusterConfig>, List<SortClusterConfig>, List<SortClusterConfig>> clusterCheckFunction,
            BiFunction<NodeConfig, NodeConfig, NodeConfig> nodeCheckFunction) {

        List<SortClusterConfig> checkCluster = clusterCheckFunction.apply(last.getClusters(), current.getClusters());

        NodeConfig checkNode = nodeCheckFunction.apply(last.getNodeConfig(), current.getNodeConfig());

        if (CollectionUtils.isEmpty(checkCluster) && checkNode == null) {
            return null;
        }

        return SortTaskConfig
                .builder()
                .sortTaskName(last.getSortTaskName())
                .clusters(checkCluster)
                .nodeConfig(checkNode)
                .build();
    }
}
