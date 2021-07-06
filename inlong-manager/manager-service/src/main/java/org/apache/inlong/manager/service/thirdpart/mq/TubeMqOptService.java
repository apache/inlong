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

package org.apache.inlong.manager.service.thirdpart.mq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeMqTopicRequest;
import org.apache.inlong.manager.common.pojo.tubemq.QueryTubeTopicRequest;
import org.apache.inlong.manager.common.pojo.tubemq.TubeClusterResponse;
import org.apache.inlong.manager.common.pojo.tubemq.TubeManagerResponse;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Slf4j
@Service
public class TubeMqOptService {

    private static final Gson GSON = new GsonBuilder().create(); // thread safe

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private HttpUtils httpUtils;

    public String createNewTopic(AddTubeMqTopicRequest request) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Content-Type", "application/json");
        try {
            if (CollectionUtils.isEmpty(request.getAddTopicTasks())) {
                throw new Exception("topic cannot be empty");
            }
            AddTubeMqTopicRequest.AddTopicTasksBean addTopicTasksBean = request.getAddTopicTasks().get(0);
            QueryTubeTopicRequest topicRequest = QueryTubeTopicRequest.builder()
                    .topicName(addTopicTasksBean.getTopicName()).clusterId(request.getClusterId())
                    .user(request.getUser()).build();

            String tubeManager = clusterBean.getTubeManager();
            TubeManagerResponse response = httpUtils
                    .request(tubeManager + "/v1/topic?method=queryCanWrite", HttpMethod.POST,
                            GSON.toJson(topicRequest), httpHeaders, TubeManagerResponse.class);
            if (response.getErrCode() == 101) { // topic already exists
                log.info(" create tube topic  {}  on {} ", GSON.toJson(request),
                        tubeManager + "/v1/task?method=addTopicTask");
                TubeManagerResponse createRsp = httpUtils
                        .request(tubeManager + "/v1/task?method=addTopicTask", HttpMethod.POST,
                                GSON.toJson(request), httpHeaders, TubeManagerResponse.class);
            } else {
                log.warn("topic {} exists in {} ", addTopicTasksBean.getTopicName(), tubeManager);
            }
        } catch (Exception e) {
            log.error("fail to create tube topic " + request.getAddTopicTasks().get(0).getTopicName(), e);
        }
        return "";
    }

    public String createNewConsumerGroup(AddTubeConsumeGroupRequest request) throws Exception {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Content-Type", "application/json");
        try {
            log.info(" create tube consumer group  {}  on {} ", GSON.toJson(request),
                    clusterBean.getTubeManager() + "/v1/task?method=addTopicTask");
            TubeManagerResponse rsp = httpUtils
                    .request(clusterBean.getTubeManager() + "/v1/group?method=add", HttpMethod.POST,
                            GSON.toJson(request),
                            httpHeaders, TubeManagerResponse.class);
            if (rsp.getErrCode() == -1) { // Creation failed
                throw new BusinessException(BizErrorCodeEnum.CONSUMER_GROUP_CREATE_FAILED, rsp.getErrMsg());
            }
        } catch (BusinessException e) {
            log.error(" fail to create tube consumer group  " + GSON.toJson(request), e);
            throw e;
        }
        return "";
    }

    public List<TubeClusterResponse.DataBean> queryCluster() {
        HttpHeaders httpHeaders = new HttpHeaders();
        try {
            log.info(" query tube  cluster {} ", clusterBean.getTubeManager() + "/v1/cluster");
            TubeClusterResponse rsp = httpUtils
                    .request(clusterBean.getTubeManager() + "/v1/cluster", HttpMethod.GET, null, httpHeaders,
                            TubeClusterResponse.class);
            return rsp.getData();
        } catch (Exception e) {
            log.error(" fail to  query tube  cluster ", e);
        }
        return null;
    }
}
