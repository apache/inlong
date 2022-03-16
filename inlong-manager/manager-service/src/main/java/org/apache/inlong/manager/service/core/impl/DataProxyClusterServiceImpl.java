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

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import org.apache.inlong.common.pojo.dataproxy.DataProxyCluster;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyClusterSet;
import org.apache.inlong.manager.service.core.DataProxyClusterService;
import org.apache.inlong.manager.service.repository.DataProxyConfigRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * DataProxy cluster service layer implementation class
 */
@Service
@Slf4j
public class DataProxyClusterServiceImpl implements DataProxyClusterService {

    @Autowired
    private DataProxyConfigRepository proxyRepository;

    /**
     * query data proxy config by cluster id
     *
     * @return data proxy config
     */
    public String getAllConfig(String clusterName, String setName, String md5) {
        DataProxyClusterSet setObj = proxyRepository.getDataProxyClusterSet(setName);
        if (setObj == null) {
            return this.getErrorAllConfig();
        }
        String configMd5 = setObj.getMd5Map().get(clusterName);
        if (configMd5 == null) {
            return this.getErrorAllConfig();
        }
        // same config
        if (md5 != null && configMd5.equals(md5)) {
            DataProxyConfigResponse response = new DataProxyConfigResponse();
            response.setResult(true);
            response.setErrCode(DataProxyConfigResponse.NOUPDATE);
            response.setMd5(configMd5);
            response.setData(new DataProxyCluster());
            Gson gson = new Gson();
            return gson.toJson(response);
        }
        String configJson = setObj.getProxyConfigJson().get(clusterName);
        if (configJson == null) {
            return this.getErrorAllConfig();
        }
        return configJson;
    }

    /**
     * getErrorAllConfig
     */
    private String getErrorAllConfig() {
        DataProxyConfigResponse response = new DataProxyConfigResponse();
        response.setResult(false);
        response.setErrCode(DataProxyConfigResponse.REQ_PARAMS_ERROR);
        Gson gson = new Gson();
        return gson.toJson(response);
    }

}
