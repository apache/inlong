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

package org.apache.inlong.manager.service.thirdparty.sort.util;

import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;

import java.util.List;

public class SourceInfoUtils {

    public static boolean isBinlogMigrationSource(SourceResponse sourceResponse) {
        if (SourceType.BINLOG.getType().equalsIgnoreCase(sourceResponse.getSourceType())) {
            BinlogSourceResponse binlogSourceResponse = (BinlogSourceResponse) sourceResponse;
            return binlogSourceResponse.isAllMigration();
        }
        return false;
    }

    public static PulsarSourceInfo createPulsarSourceInfo(InlongGroupInfo groupInfo,
            String pulsarTopic,
            DeserializationInfo deserializationInfo,
            List<FieldInfo> fieldInfos,
            String appName,
            PulsarClusterInfo pulsarClusterInfo,
            String tenant) {
        final String namespace = groupInfo.getMqResourceObj();
        // Full name of Topic in Pulsar
        final String fullTopicName = "persistent://" + tenant + "/" + namespace + "/" + pulsarTopic;
        final String consumerGroup = appName + "_" + pulsarTopic + "_consumer_group";
        return new PulsarSourceInfo(pulsarClusterInfo.getAdminUrl(), pulsarClusterInfo.getBrokerServiceUrl(),
                fullTopicName, consumerGroup, deserializationInfo, fieldInfos.toArray(new FieldInfo[0]),
                pulsarClusterInfo.getToken());
    }
}
