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

package org.apache.inlong.manager.service.thirdparty.mq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeMqTopicRequest;
import org.apache.inlong.manager.common.pojo.tubemq.QueryTubeTopicRequest;
import org.apache.inlong.manager.common.pojo.tubemq.TubeManagerResponse;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.service.CommonOperateService;
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
    private CommonOperateService commonOperateService;
    @Autowired
    private HttpUtils httpUtils;

    /**
     * Create new topic
     */
    public String createNewTopic(AddTubeMqTopicRequest request) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Content-Type", "application/json");
        try {
            if (CollectionUtils.isEmpty(request.getAddTopicTasks())) {
                throw new Exception("topic cannot be empty");
            }
            AddTubeMqTopicRequest.AddTopicTasksBean addTopicTasksBean = request.getAddTopicTasks().get(0);
            String clusterIdStr = commonOperateService.getSpecifiedParam(Constant.CLUSTER_TUBE_CLUSTER_ID);
            int clusterId = Integer.valueOf(clusterIdStr);
            QueryTubeTopicRequest topicRequest = QueryTubeTopicRequest.builder()
                    .topicName(addTopicTasksBean.getTopicName()).clusterId(clusterId)
                    .user(request.getUser()).build();

            String tubeManager = commonOperateService.getSpecifiedParam(Constant.CLUSTER_TUBE_MANAGER);
            TubeManagerResponse response = httpUtils
                    .request(tubeManager + "/v1/topic?method=queryCanWrite", HttpMethod.POST,
                            GSON.toJson(topicRequest), httpHeaders, TubeManagerResponse.class);
            if (response.getErrCode() == 101) { // topic already exists
                log.info(" create tube topic  {}  on {} ", GSON.toJson(request),
                        tubeManager + "/v1/task?method=addTopicTask");

                request.setClusterId(clusterId);
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

    /**
     * Create new consumer group
     */
    public String createNewConsumerGroup(AddTubeConsumeGroupRequest request) throws Exception {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Content-Type", "application/json");
        try {
            String tubeManager = commonOperateService.getSpecifiedParam(Constant.CLUSTER_TUBE_MANAGER);
            log.info("create tube consumer group {}  on {} ", GSON.toJson(request),
                    tubeManager + "/v1/task?method=addTopicTask");
            TubeManagerResponse rsp = httpUtils.request(tubeManager + "/v1/group?method=add",
                    HttpMethod.POST, GSON.toJson(request), httpHeaders, TubeManagerResponse.class);
            if (rsp.getErrCode() == -1) { // Creation failed
                throw new BusinessException(ErrorCodeEnum.CONSUMER_GROUP_CREATE_FAILED, rsp.getErrMsg());
            }
        } catch (BusinessException e) {
            log.error(" fail to create tube consumer group  " + GSON.toJson(request), e);
            throw e;
        }
        return "";
    }

    /**
     * Check if the topic is exists
     */
    public boolean queryTopicIsExist(QueryTubeTopicRequest queryTubeTopicRequest) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add("Content-Type", "application/json");
        try {
            String tubeManager = commonOperateService.getSpecifiedParam(Constant.CLUSTER_TUBE_MANAGER);
            TubeManagerResponse response = httpUtils.request(tubeManager + "/v1/topic?method=queryCanWrite",
                    HttpMethod.POST, GSON.toJson(queryTubeTopicRequest), httpHeaders, TubeManagerResponse.class);
            if (response.getErrCode() == 0) { // topic already exists
                log.error("topic {} exists in {} ", queryTubeTopicRequest.getTopicName(), tubeManager);
                return true;
            }
        } catch (Exception e) {
            log.error("fail to query tube topic {}", queryTubeTopicRequest.getTopicName(), e);
        }
        return false;
    }
}
