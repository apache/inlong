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

package org.apache.inlong.common.pojo.sort.dataflow;

import org.apache.inlong.common.pojo.sort.dataflow.sink.SinkConfig;
import org.apache.inlong.common.util.SortConfigUtil;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DataFlowConfig implements Serializable {

    private String dataflowId;
    private Integer version;
    private String auditTag;
    private String inlongGroupId;
    private String inlongStreamId;
    private String transformSql;
    private SourceConfig sourceConfig;
    private SinkConfig sinkConfig;
    private Map<String, Object> properties;

    public static List<DataFlowConfig> batchCheckDelete(List<DataFlowConfig> last, List<DataFlowConfig> current) {
        return SortConfigUtil.checkDelete(last, current, DataFlowConfig::getDataflowId);
    }

    public static List<DataFlowConfig> batchCheckUpdate(List<DataFlowConfig> last, List<DataFlowConfig> current) {
        return SortConfigUtil.checkUpdate(last, current, DataFlowConfig::getDataflowId, DataFlowConfig::getVersion);
    }

    public static List<DataFlowConfig> batchCheckNoUpdate(List<DataFlowConfig> last, List<DataFlowConfig> current) {
        return SortConfigUtil.checkNoUpdate(last, current, DataFlowConfig::getDataflowId, DataFlowConfig::getVersion);
    }

    public static List<DataFlowConfig> batchCheckNew(List<DataFlowConfig> last, List<DataFlowConfig> current) {
        return SortConfigUtil.checkNew(last, current, DataFlowConfig::getDataflowId);
    }

    public static List<DataFlowConfig> batchCheckLatest(List<DataFlowConfig> last, List<DataFlowConfig> current) {
        return SortConfigUtil.checkLatest(last, current,
                DataFlowConfig::getDataflowId, DataFlowConfig::getVersion);
    }

}
