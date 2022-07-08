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

package org.apache.inlong.manager.web.controller.openapi;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyNodeInfo;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;

/**
 * Data proxy controller.
 */
@RestController
@RequestMapping("/openapi")
@Api(tags = "Open-DataProxy-API")
public class DataProxyController {

    private static final String KEY_SECOND_CLUSTER_TAG = "second_cluster_tag";
    private static final String KEY_SECOND_TOPIC = "second_topic";

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private InlongGroupEntityMapper inlongGroupMapper;

    @PostMapping(value = "/dataproxy/getIpList")
    @ApiOperation(value = "Get data proxy ip list by cluster name and tag")
    public Response<List<DataProxyNodeInfo>> getIpList(@RequestBody ClusterPageRequest request) {
        return Response.success(clusterService.getDataProxyNodeList(request));
    }

    @GetMapping("/dataproxy/getConfig")
    @ApiOperation(value = "Get data proxy topic list")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "clusterTag", value = "cluster tag", dataTypeClass = String.class),
            @ApiImplicitParam(name = "clusterName", value = "cluster name", dataTypeClass = String.class)
    })
    public Response<DataProxyConfig> getConfig(
            @RequestParam(required = false) String clusterTag,
            @RequestParam(required = true) String clusterName) {
        DataProxyConfig config = clusterService.getDataProxyConfig(clusterTag, clusterName);
        if (CollectionUtils.isEmpty(config.getMqClusterList()) || CollectionUtils.isEmpty(config.getTopicList())) {
            return Response.fail("Failed to get MQ Cluster or Topic, make sure Cluster registered or Topic existed.");
        }
        return Response.success(config);
    }

    @GetMapping("/dataproxy/getAllConfig")
    @ApiOperation(value = "Get all proxy config")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "clusterName", dataTypeClass = String.class, required = true),
            @ApiImplicitParam(name = "md5", dataTypeClass = String.class, required = true)
    })
    public String getAllConfig(@RequestParam String clusterName, @RequestParam(required = false) String md5) {
        return clusterService.getAllConfig(clusterName, md5);
    }

    /**
     * changeClusterTag
     */
    @RequestMapping(value = "/changeClusterTag", method = RequestMethod.POST)
    @ApiOperation(value = "Change cluster tag and topic of a inlong group id.")
    public Response<String> changeClusterTag(@RequestBody InlongGroupEntity groupInfo) {
        String inlongGroupId = groupInfo.getInlongGroupId();
        String clusterTag = groupInfo.getInlongClusterTag();
        String topic = groupInfo.getMqResource();
        if (StringUtils.isEmpty(inlongGroupId) || StringUtils.isEmpty(clusterTag) || StringUtils.isEmpty(topic)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        // select
        InlongGroupEntity oldGroup = inlongGroupMapper.selectByGroupId(inlongGroupId);
        if (oldGroup == null) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        // parse ext_params
        String extParams = oldGroup.getExtParams();
        if (StringUtils.isEmpty(extParams)) {
            extParams = "{}";
        }
        // parse json
        Gson gson = new Gson();
        JsonObject extParamsObj = gson.fromJson(extParams, JsonObject.class);
        // change cluster tag
        extParamsObj.addProperty(KEY_SECOND_CLUSTER_TAG, oldGroup.getInlongClusterTag());
        extParamsObj.addProperty(KEY_SECOND_TOPIC, oldGroup.getMqResource());
        oldGroup.setInlongClusterTag(clusterTag);
        oldGroup.setMqResource(topic);
        String newExtParams = extParamsObj.toString();
        oldGroup.setExtParams(newExtParams);
        // update
        inlongGroupMapper.updateByIdentifierSelective(oldGroup);
        return Response.success(groupInfo.getInlongGroupId());
    }

    /**
     * removeSecondClusterTag
     */
    @RequestMapping(value = "/removeSecondClusterTag", method = RequestMethod.POST)
    @ApiOperation(value = "remove second cluster tag and topic of a inlong group id.")
    public Response<String> removeSecondClusterTag(@RequestBody InlongGroupEntity groupInfo) {
        String inlongGroupId = groupInfo.getInlongGroupId();
        if (StringUtils.isEmpty(inlongGroupId)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        // select
        InlongGroupEntity oldGroup = inlongGroupMapper.selectByGroupId(inlongGroupId);
        if (oldGroup == null) {
            throw new BusinessException(ErrorCodeEnum.GROUP_NOT_FOUND);
        }
        // parse ext_params
        String extParams = oldGroup.getExtParams();
        if (StringUtils.isEmpty(extParams)) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        // parse json
        Gson gson = new Gson();
        JsonObject extParamsObj = gson.fromJson(extParams, JsonObject.class);
        extParamsObj.remove(KEY_SECOND_CLUSTER_TAG);
        extParamsObj.remove(KEY_SECOND_TOPIC);
        String newExtParams = extParamsObj.toString();
        oldGroup.setExtParams(newExtParams);
        // update
        inlongGroupMapper.updateByIdentifierSelective(oldGroup);
        return Response.success(groupInfo.getInlongGroupId());
    }

}
