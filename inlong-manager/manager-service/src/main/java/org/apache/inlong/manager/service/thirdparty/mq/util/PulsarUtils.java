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

package org.apache.inlong.manager.service.thirdparty.mq.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;

/**
 * Pulsar connection utils
 */
@Slf4j
public class PulsarUtils {

    private PulsarUtils() {
    }

    /**
     * Get pulsar admin info
     */
    public static PulsarAdmin getPulsarAdmin(InlongGroupRequest groupInfo, String defaultServiceUrl)
            throws PulsarClientException {
        if (CollectionUtils.isEmpty(groupInfo.getExtList())) {
            return getPulsarAdmin(defaultServiceUrl);
        }
        List<InlongGroupExtInfo> groupExtInfoList = groupInfo.getExtList();
        String pulsarServiceUrl = null;
        String pulsarAuthentication = null;
        String pulsarAuthenticationType = InlongGroupSettings.DEFAULT_PULSAR_AUTHENTICATION_TYPE;
        for (InlongGroupExtInfo extInfo : groupExtInfoList) {
            if (InlongGroupSettings.PULSAR_ADMIN_URL.equals(extInfo.getKeyName())
                    && StringUtils.isNotEmpty(extInfo.getKeyValue())) {
                pulsarServiceUrl = extInfo.getKeyValue();
            }
            if (InlongGroupSettings.PULSAR_AUTHENTICATION_TYPE.equals(extInfo.getKeyName())
                    && StringUtils.isNotEmpty(extInfo.getKeyValue())) {
                pulsarAuthenticationType = extInfo.getKeyValue();
            }
            if (InlongGroupSettings.PULSAR_AUTHENTICATION.equals(extInfo.getKeyName())
                    && StringUtils.isNotEmpty(extInfo.getKeyValue())) {
                pulsarAuthentication = extInfo.getKeyValue();
            }
        }
        if (StringUtils.isEmpty(pulsarServiceUrl) && StringUtils.isEmpty(pulsarAuthentication)) {
            return getPulsarAdmin(defaultServiceUrl);
        } else if (StringUtils.isEmpty(pulsarAuthentication)) {
            return getPulsarAdmin(pulsarServiceUrl);
        } else {
            return getPulsarAdmin(pulsarServiceUrl, pulsarAuthentication, pulsarAuthenticationType);
        }
    }

    /**
     * Obtain the PulsarAdmin client according to the service URL, and it must be closed after use
     */
    public static PulsarAdmin getPulsarAdmin(String serviceHttpUrl) throws PulsarClientException {
        return PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl).build();
    }

    public static PulsarAdmin getPulsarAdmin(String serviceHttpUrl, String authentication, String authenticationType)
            throws PulsarClientException {
        if (InlongGroupSettings.DEFAULT_PULSAR_AUTHENTICATION_TYPE.equals(authenticationType)) {
            return PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl)
                    .authentication(AuthenticationFactory.token(authentication)).build();
        } else {
            throw new IllegalArgumentException(
                    String.format("illegal authentication type for pulsar : %s", authenticationType));
        }
    }

    public static List<String> getPulsarClusters(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        return pulsarAdmin.clusters().getClusters();
    }

    public static String getServiceUrl(PulsarAdmin pulsarAdmin, String pulsarCluster) throws PulsarAdminException {
        return pulsarAdmin.clusters().getCluster(pulsarCluster).getServiceUrl();
    }

}
