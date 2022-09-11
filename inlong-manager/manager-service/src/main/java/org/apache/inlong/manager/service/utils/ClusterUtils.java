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

package org.apache.inlong.manager.service.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.pojo.cluster.tubemq.TubeClusterInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * ClusterUtils
 */
public class ClusterUtils {

    public static final Logger LOG = LoggerFactory.getLogger(ClusterUtils.class);

    public static final String KEY_ADMIN_URL = "adminUrl";
    public static final String KEY_AUTHENTICATION = "authentication";
    public static final String KEY_TENANT = "tenant";
    public static final String KEY_NAMESPACE = "namespace";
    public static final java.lang.reflect.Type HASHMAP_TYPE = new TypeToken<HashMap<String, String>>() {
    }.getType();

    /**
     * createTubeClusterInfo
     */
    public static TubeClusterInfo createTubeClusterInfo(InlongClusterEntity cluster) {
        TubeClusterInfo tubeClusterInfo = new TubeClusterInfo();
        tubeClusterInfo.setClusterTags(cluster.getClusterTags());
        tubeClusterInfo.setExtParams(cluster.getExtParams());
        tubeClusterInfo.setExtTag(cluster.getExtTag());
        tubeClusterInfo.setHeartbeat(cluster.getHeartbeat());
        tubeClusterInfo.setMasterWebUrl(cluster.getUrl());
        tubeClusterInfo.setName(cluster.getName());
        tubeClusterInfo.setToken(cluster.getToken());
        tubeClusterInfo.setType(cluster.getType());
        tubeClusterInfo.setUrl(cluster.getUrl());
        tubeClusterInfo.setVersion(cluster.getVersion());
        return tubeClusterInfo;
    }

    /**
     * createPulsarAdmin
     */
    public static PulsarAdmin createPulsarAdmin(Map<String, String> extParams) throws PulsarClientException {
        // create admin
        String adminUrl = extParams.get(KEY_ADMIN_URL);
        String authentication = extParams.get(KEY_AUTHENTICATION);
        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(adminUrl);
        if (!StringUtils.isEmpty(authentication)) {
            pulsarAdminBuilder.authentication(AuthenticationFactory.token(authentication));
        }
        PulsarAdmin admin = pulsarAdminBuilder.build();
        return admin;
    }

    /**
     * createPulsarAdmin
     */
    public static PulsarAdmin createPulsarAdmin(InlongClusterEntity cluster) throws PulsarClientException {
        // check cluster parameters
        String strExtParams = cluster.getExtParams();
        Gson gson = new Gson();
        Map<String, String> extParams = gson.fromJson(strExtParams, ClusterUtils.HASHMAP_TYPE);
        if (!extParams.containsKey(KEY_ADMIN_URL)) {
            LOG.error("Can not delete pulsar topic,adminUrl is empty,cluster:{}",
                    cluster.getName());
            return null;
        }
        // create admin
        PulsarAdmin admin = createPulsarAdmin(extParams);
        return admin;
    }
}
