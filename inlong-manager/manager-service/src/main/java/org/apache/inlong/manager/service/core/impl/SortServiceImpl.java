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

package org.apache.inlong.manager.service.core.impl;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.sdk.CacheZone;
import org.apache.inlong.common.pojo.sdk.CacheZoneConfig;
import org.apache.inlong.common.pojo.sortstandalone.SortClusterResponse;
import org.apache.inlong.common.pojo.sdk.SortSourceConfigResponse;
import org.apache.inlong.manager.service.core.SortSourceService;
import org.apache.inlong.manager.service.core.SortService;
import org.apache.inlong.manager.service.repository.SortConfigRepository;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Sort service implementation.
 */
@Service
public class SortServiceImpl implements SortService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortServiceImpl.class);

    private static final int RESPONSE_CODE_SUCCESS = 0;
    private static final int RESPONSE_CODE_NO_UPDATE = 1;
    private static final int RESPONSE_CODE_FAIL = -1;
    private static final int RESPONSE_CODE_REQ_PARAMS_ERROR = -101;


    @Autowired private SortSourceService sortSourceService;

    @Autowired private SortConfigRepository sortConfigRepository;

    @Override
    public SortClusterResponse getClusterConfig(String clusterName, String md5) {

        return sortConfigRepository.getClusterConfig(clusterName, md5);
    }

    @Override
    public SortSourceConfigResponse getSourceConfig(String clusterName, String sortTaskId, String md5) {

        // check if clusterName and sortTaskId are null.
        if (StringUtils.isBlank(clusterName) || StringUtils.isBlank(sortTaskId)) {
            String errMsg = "Blank cluster name or task id, return nothing";
            return SortSourceConfigResponse.builder()
                    .msg(errMsg)
                    .code(RESPONSE_CODE_REQ_PARAMS_ERROR)
                    .build();
        }

        // get cacheZones
        Map<String, CacheZone> cacheZones;
        try {
            cacheZones = sortSourceService.getCacheZones(clusterName, sortTaskId);
        } catch (Exception e) {
            String errMsg = "Got exception when get cache zones. " + e.getMessage();
            LOGGER.error(errMsg, e);
            return SortSourceConfigResponse.builder()
                    .msg(errMsg)
                    .code(RESPONSE_CODE_FAIL)
                    .build();
        }

        // if there is not any cacheZones;
        if (cacheZones.isEmpty()) {
            String errMsg = "There is not any cacheZones of cluster: " + clusterName
                    + " , task: " + sortTaskId
                    + " , please check the params";
            LOGGER.error(errMsg);
            return SortSourceConfigResponse.builder()
                    .msg(errMsg)
                    .code(RESPONSE_CODE_REQ_PARAMS_ERROR)
                    .build();
        }

        CacheZoneConfig data = CacheZoneConfig.builder()
                .sortClusterName(clusterName)
                .sortTaskId(sortTaskId)
                .cacheZones(cacheZones)
                .build();

        // no update
        JSONObject jo = new JSONObject(data);
        String localMd5 = DigestUtils.md5Hex(jo.toString());
        if (md5.equals(localMd5)) {
            String msg = "Same md5, no update";
            return SortSourceConfigResponse.builder()
                    .msg(msg)
                    .code(RESPONSE_CODE_NO_UPDATE)
                    .md5(md5)
                    .build();
        }

        // normal response.
        return SortSourceConfigResponse.builder()
                .msg("success")
                .code(RESPONSE_CODE_SUCCESS)
                .data(data)
                .md5(localMd5)
                .build();
    }
}
