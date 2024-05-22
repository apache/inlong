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
public class SortClusterConfig implements Serializable {

    private String clusterTag;
    private List<MqClusterConfig> mqClusterConfigs;
    private List<DataFlowConfig> dataFlowConfigs;

    public static List<SortClusterConfig> batchCheckDelete(
            List<SortClusterConfig> last,
            List<SortClusterConfig> current) {
        return SortConfigUtil.batchCheckDeleteRecursive(last, current,
                SortClusterConfig::getClusterTag, SortClusterConfig::checkDelete);
    }

    public static List<SortClusterConfig> batchCheckUpdate(
            List<SortClusterConfig> last,
            List<SortClusterConfig> current) {
        return SortConfigUtil.batchCheckUpdateRecursive(last, current,
                SortClusterConfig::getClusterTag, SortClusterConfig::checkUpdate);
    }

    public static List<SortClusterConfig> batchCheckNoUpdate(
            List<SortClusterConfig> last,
            List<SortClusterConfig> current) {
        return SortConfigUtil.batchCheckNoUpdateRecursive(last, current,
                SortClusterConfig::getClusterTag, SortClusterConfig::checkNoUpdate);
    }

    public static List<SortClusterConfig> batchCheckNew(
            List<SortClusterConfig> last,
            List<SortClusterConfig> current) {
        return SortConfigUtil.batchCheckNewRecursive(last, current,
                SortClusterConfig::getClusterTag, SortClusterConfig::checkNew);
    }

    public static SortClusterConfig checkDelete(SortClusterConfig last, SortClusterConfig current) {
        return check(last, current, MqClusterConfig::batchCheckDelete, DataFlowConfig::batchCheckDelete);
    }

    public static SortClusterConfig checkUpdate(SortClusterConfig last, SortClusterConfig current) {
        return check(last, current, MqClusterConfig::batchCheckUpdate, DataFlowConfig::batchCheckUpdate);
    }

    public static SortClusterConfig checkNoUpdate(SortClusterConfig last, SortClusterConfig current) {
        return check(last, current, MqClusterConfig::batchCheckNoUpdate, DataFlowConfig::batchCheckNoUpdate);
    }

    public static SortClusterConfig checkNew(SortClusterConfig last, SortClusterConfig current) {
        return check(last, current, MqClusterConfig::batchCheckNew, DataFlowConfig::batchCheckNew);
    }

    public static SortClusterConfig check(
            SortClusterConfig last, SortClusterConfig current,
            BiFunction<List<MqClusterConfig>, List<MqClusterConfig>, List<MqClusterConfig>> mqCheckFunction,
            BiFunction<List<DataFlowConfig>, List<DataFlowConfig>, List<DataFlowConfig>> flowCheckFunction) {

        List<MqClusterConfig> checkCluster = mqCheckFunction
                .apply(last.getMqClusterConfigs(), current.getMqClusterConfigs());
        List<DataFlowConfig> checkDataflows = flowCheckFunction
                .apply(last.getDataFlowConfigs(), current.getDataFlowConfigs());

        if (CollectionUtils.isNotEmpty(checkCluster) && CollectionUtils.isNotEmpty(checkDataflows)) {
            return null;
        }

        return SortClusterConfig.builder()
                .clusterTag(last.getClusterTag())
                .mqClusterConfigs(checkCluster)
                .dataFlowConfigs(checkDataflows)
                .build();
    }

}
