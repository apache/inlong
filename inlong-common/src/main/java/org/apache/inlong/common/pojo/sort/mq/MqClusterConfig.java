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

package org.apache.inlong.common.pojo.sort.mq;

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.util.SortConfigUtil;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.List;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PulsarClusterConfig.class, name = MQType.PULSAR),
        @JsonSubTypes.Type(value = TubeClusterConfig.class, name = MQType.TUBEMQ)
})
public abstract class MqClusterConfig implements Serializable {

    private Integer version;
    private String clusterName;

    public static List<MqClusterConfig> batchCheckDelete(List<MqClusterConfig> last, List<MqClusterConfig> current) {
        return SortConfigUtil.checkDelete(last, current, MqClusterConfig::getClusterName);
    }

    public static List<MqClusterConfig> batchCheckUpdate(List<MqClusterConfig> last, List<MqClusterConfig> current) {
        return SortConfigUtil.checkUpdate(last, current,
                MqClusterConfig::getClusterName, MqClusterConfig::getVersion);
    }

    public static List<MqClusterConfig> batchCheckNoUpdate(List<MqClusterConfig> last, List<MqClusterConfig> current) {
        return SortConfigUtil.checkNoUpdate(last, current,
                MqClusterConfig::getClusterName, MqClusterConfig::getVersion);
    }

    public static List<MqClusterConfig> batchCheckNew(List<MqClusterConfig> last, List<MqClusterConfig> current) {
        return SortConfigUtil.checkNew(last, current, MqClusterConfig::getClusterName);
    }

    public static List<MqClusterConfig> batchCheckLatest(List<MqClusterConfig> last, List<MqClusterConfig> current) {
        return SortConfigUtil.checkLatest(last, current,
                MqClusterConfig::getClusterName, MqClusterConfig::getVersion);
    }

    public static List<MqClusterConfig> batchCheckLast(List<MqClusterConfig> last, List<MqClusterConfig> current) {
        return last;
    }

}
