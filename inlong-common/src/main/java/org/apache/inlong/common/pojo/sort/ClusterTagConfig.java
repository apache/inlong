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

import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.mq.MqClusterConfig;
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
public class ClusterTagConfig implements Serializable {

    private String clusterTag;
    private List<MqClusterConfig> mqClusterConfigs;
    private List<DataFlowConfig> dataFlowConfigs;

    public static ClusterTagConfig checkDelete(ClusterTagConfig last, ClusterTagConfig current) {
        if (CollectionUtils.isEmpty(current.getMqClusterConfigs())) {
            return last;
        }

        return check(last, current, MqClusterConfig::batchCheckLast, DataFlowConfig::batchCheckDelete);
    }

    public static ClusterTagConfig checkNew(ClusterTagConfig last, ClusterTagConfig current) {
        return check(last, current, MqClusterConfig::batchCheckLatest, DataFlowConfig::batchCheckNew);
    }

    public static ClusterTagConfig checkUpdate(ClusterTagConfig last, ClusterTagConfig current) {
        List<MqClusterConfig> updateCluster =
                MqClusterConfig.batchCheckUpdate(last.getMqClusterConfigs(), current.getMqClusterConfigs());

        List<MqClusterConfig> latestCluster =
                MqClusterConfig.batchCheckLatest(last.getMqClusterConfigs(), current.getMqClusterConfigs());

        List<MqClusterConfig> deleteCluster =
                MqClusterConfig.batchCheckDelete(last.getMqClusterConfigs(), current.getMqClusterConfigs());

        List<MqClusterConfig> newCluster =
                MqClusterConfig.batchCheckNew(last.getMqClusterConfigs(), current.getMqClusterConfigs());

        List<DataFlowConfig> updateDataflows =
                DataFlowConfig.batchCheckUpdate(last.getDataFlowConfigs(), current.getDataFlowConfigs());

        // if mq cluster update/add/delete, use latest mq and update+noupdate dataflow
        if (CollectionUtils.isNotEmpty(updateCluster)
                || CollectionUtils.isNotEmpty(deleteCluster)
                || CollectionUtils.isNotEmpty(newCluster)) {
            List<DataFlowConfig> noUpdateDataflows =
                    DataFlowConfig.batchCheckNoUpdate(last.getDataFlowConfigs(), current.getDataFlowConfigs());
            noUpdateDataflows.addAll(updateDataflows);

            return ClusterTagConfig.builder()
                    .clusterTag(last.getClusterTag())
                    .mqClusterConfigs(latestCluster)
                    .dataFlowConfigs(noUpdateDataflows)
                    .build();
        }

        // if data flow no update, return null
        if (CollectionUtils.isEmpty(updateDataflows)) {
            return null;
        }

        // if only dataflow update, use latest mq and update dataflow
        return ClusterTagConfig.builder()
                .clusterTag(last.getClusterTag())
                .mqClusterConfigs(latestCluster)
                .dataFlowConfigs(updateDataflows)
                .build();
    }

    public static ClusterTagConfig checkLatest(ClusterTagConfig last, ClusterTagConfig current) {
        if (CollectionUtils.isEmpty(current.getMqClusterConfigs())) {
            return null;
        }

        return check(last, current, MqClusterConfig::batchCheckLatest, DataFlowConfig::batchCheckLatest);
    }

    public static List<ClusterTagConfig> batchCheckDelete(
            List<ClusterTagConfig> last,
            List<ClusterTagConfig> current) {
        return SortConfigUtil.batchCheckDeleteRecursive(last, current,
                ClusterTagConfig::getClusterTag, ClusterTagConfig::checkDelete);
    }

    public static List<ClusterTagConfig> batchCheckNew(
            List<ClusterTagConfig> last,
            List<ClusterTagConfig> current) {
        return SortConfigUtil.batchCheckNewRecursive(last, current,
                ClusterTagConfig::getClusterTag, ClusterTagConfig::checkNew);
    }

    public static List<ClusterTagConfig> batchCheckUpdate(
            List<ClusterTagConfig> last,
            List<ClusterTagConfig> current) {
        return SortConfigUtil.batchCheckUpdateRecursive(last, current,
                ClusterTagConfig::getClusterTag, ClusterTagConfig::checkUpdate);
    }

    public static List<ClusterTagConfig> batchCheckLatest(
            List<ClusterTagConfig> last,
            List<ClusterTagConfig> current) {
        return SortConfigUtil.batchCheckLatestRecursive(last, current,
                ClusterTagConfig::getClusterTag, ClusterTagConfig::checkLatest);
    }

    public static ClusterTagConfig check(
            ClusterTagConfig last, ClusterTagConfig current,
            BiFunction<List<MqClusterConfig>, List<MqClusterConfig>, List<MqClusterConfig>> mqCheckFunction,
            BiFunction<List<DataFlowConfig>, List<DataFlowConfig>, List<DataFlowConfig>> flowCheckFunction) {

        List<MqClusterConfig> checkMqCluster = mqCheckFunction
                .apply(last.getMqClusterConfigs(), current.getMqClusterConfigs());
        List<DataFlowConfig> checkDataflows = flowCheckFunction
                .apply(last.getDataFlowConfigs(), current.getDataFlowConfigs());

        if (CollectionUtils.isEmpty(checkDataflows)) {
            return null;
        }

        return ClusterTagConfig.builder()
                .clusterTag(last.getClusterTag())
                .mqClusterConfigs(checkMqCluster)
                .dataFlowConfigs(checkDataflows)
                .build();
    }

}
