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
public class TaskConfig implements Serializable {

    private String sortTaskName;
    private List<ClusterTagConfig> clusterTagConfigs;
    private NodeConfig nodeConfig;

    public static List<TaskConfig> batchCheckDelete(List<TaskConfig> last, List<TaskConfig> current) {
        return SortConfigUtil.batchCheckDeleteRecursive(last, current,
                TaskConfig::getSortTaskName, TaskConfig::checkDelete);
    }

    public static List<TaskConfig> batchCheckUpdate(List<TaskConfig> last, List<TaskConfig> current) {
        return SortConfigUtil.batchCheckUpdateRecursive(last, current,
                TaskConfig::getSortTaskName, TaskConfig::checkUpdate);
    }

    public static List<TaskConfig> batchCheckNew(List<TaskConfig> last, List<TaskConfig> current) {
        return SortConfigUtil.batchCheckNewRecursive(last, current,
                TaskConfig::getSortTaskName, TaskConfig::checkNew);
    }

    public static List<TaskConfig> batchCheckLatest(List<TaskConfig> latest, List<TaskConfig> current) {
        return SortConfigUtil.batchCheckLatestRecursive(latest, current,
                TaskConfig::getSortTaskName, TaskConfig::checkLatest);
    }

    public static TaskConfig checkDelete(TaskConfig last, TaskConfig current) {
        return check(last, current, ClusterTagConfig::batchCheckDelete,
                (lastNode, currentNode) -> lastNode);
    }

    public static TaskConfig checkUpdate(TaskConfig last, TaskConfig current) {
        return check(last, current, ClusterTagConfig::batchCheckUpdate,
                (lastNode, currentNode) -> {
                    if (lastNode == null || currentNode == null) {
                        return null;
                    }
                    return lastNode.getVersion() < currentNode.getVersion() ? null : currentNode;
                });
    }

    public static TaskConfig checkNew(TaskConfig last, TaskConfig current) {
        return check(last, current, ClusterTagConfig::batchCheckNew,
                (lastNode, currentNode) -> {
                    if (lastNode == null || currentNode == null) {
                        return null;
                    }
                    return lastNode.getVersion() >= currentNode.getVersion() ? lastNode : currentNode;
                });
    }

    public static TaskConfig checkLatest(TaskConfig last, TaskConfig current) {
        return check(last, current, ClusterTagConfig::batchCheckLatest,
                (lastNode, currentNode) -> {
                    if (lastNode == null || currentNode == null) {
                        return null;
                    }
                    return lastNode.getVersion() >= currentNode.getVersion() ? lastNode : currentNode;
                });
    }

    public static TaskConfig check(
            TaskConfig last, TaskConfig current,
            BiFunction<List<ClusterTagConfig>, List<ClusterTagConfig>, List<ClusterTagConfig>> clusterCheckFunction,
            BiFunction<NodeConfig, NodeConfig, NodeConfig> nodeCheckFunction) {

        List<ClusterTagConfig> checkClusterTags =
                clusterCheckFunction.apply(last.getClusterTagConfigs(), current.getClusterTagConfigs());

        NodeConfig checkNode = nodeCheckFunction.apply(last.getNodeConfig(), current.getNodeConfig());

        if (CollectionUtils.isEmpty(checkClusterTags) && checkNode == null) {
            return null;
        }

        return TaskConfig
                .builder()
                .sortTaskName(last.getSortTaskName())
                .clusterTagConfigs(checkClusterTags)
                .nodeConfig(checkNode)
                .build();
    }
}
