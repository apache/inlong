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

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.source.PulsarSourceInfo;

public class SourceInfoUtils {

    public static PulsarSourceInfo createPulsarSourceInfo(InlongGroupInfo groupInfo, String pulsarTopic,
                                                          DeserializationInfo deserializationInfo,
                                                          List<FieldInfo> fieldInfos, String appName,String tenant,
                                                          String pulsarAdminUrl, String pulsarServiceUrl) {
        final String namespace = groupInfo.getMqResourceObj();
        // Full name of Topic in Pulsar
        final String fullTopicName = "persistent://" + tenant + "/" + namespace + "/" + pulsarTopic;
        final String consumerGroup = appName + "_" + pulsarTopic + "_consumer_group";
        String adminUrl = null;
        String serviceUrl = null;
        String authentication = null;
        if (CollectionUtils.isNotEmpty(groupInfo.getExtList())) {
            for (InlongGroupExtInfo extInfo : groupInfo.getExtList()) {
                if (InlongGroupSettings.PULSAR_SERVICE_URL.equals(extInfo.getKeyName())
                        && StringUtils.isNotEmpty(extInfo.getKeyValue())) {
                    serviceUrl = extInfo.getKeyValue();
                }
                if (InlongGroupSettings.PULSAR_AUTHENTICATION.equals(extInfo.getKeyName())
                        && StringUtils.isNotEmpty(extInfo.getKeyValue())) {
                    authentication = extInfo.getKeyValue();
                }
                if (InlongGroupSettings.PULSAR_ADMIN_URL.equals(extInfo.getKeyName())
                        && StringUtils.isNotEmpty(extInfo.getKeyValue())) {
                    adminUrl = extInfo.getKeyValue();
                }
            }
        }
        if (StringUtils.isEmpty(adminUrl)) {
            adminUrl = pulsarAdminUrl;
        }
        if (StringUtils.isEmpty(serviceUrl)) {
            serviceUrl = pulsarServiceUrl;
        }
        return new PulsarSourceInfo(adminUrl, serviceUrl, fullTopicName, consumerGroup,
                deserializationInfo, fieldInfos.toArray(new FieldInfo[0]), authentication);
    }
}
