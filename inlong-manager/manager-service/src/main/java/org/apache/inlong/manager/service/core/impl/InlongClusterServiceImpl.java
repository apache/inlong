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

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.pojo.dataproxy.DataProxyCluster;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.common.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.common.pojo.dataproxy.ThirdPartyClusterDTO;
import org.apache.inlong.common.pojo.dataproxy.ThirdPartyClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeRequest;
import org.apache.inlong.manager.common.pojo.cluster.ClusterNodeResponse;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterPageRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterResponse;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyResponse;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.service.core.InlongClusterService;
import org.apache.inlong.manager.service.repository.DataProxyConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Inlong cluster service layer implementation
 */
@Service
public class InlongClusterServiceImpl implements InlongClusterService {

    public static final String SCHEMA_M0_DAY = "m0_day";
    private static final Logger LOGGER = LoggerFactory.getLogger(InlongClusterServiceImpl.class);
    private static final Gson GSON = new Gson();
    private static final String URL_SPLITTER = ",";
    private static final String HOST_SPLITTER = ":";
    private static final String CLUSTER_TUBE = "TUBE";
    private static final String CLUSTER_PULSAR = "PULSAR";
    private static final String CLUSTER_TDMQ_PULSAR = "TDMQ_PULSAR";

    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private DataProxyConfigRepository proxyRepository;

    @Override
    public Integer save(InlongClusterRequest request, String operator) {
        LOGGER.debug("begin to save inlong cluster={}", request);
        Preconditions.checkNotNull(request, "inlong cluster info cannot be empty");

        // check if cluster already exist
        InlongClusterEntity exist = clusterMapper.selectByUniqueKey(request);
        if (exist != null) {
            String errMsg = String.format("inlong cluster already exist for name=%s cluster tag=%s type=%s)",
                    request.getName(), request.getClusterTag(), request.getType());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }
        InlongClusterEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterEntity::new);
        entity.setCreator(operator);
        entity.setCreateTime(new Date());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        clusterMapper.insert(entity);

        LOGGER.info("success to save inlong cluster={} by user={}", request, operator);
        return entity.getId();
    }

    @Override
    public InlongClusterResponse get(Integer id) {
        Preconditions.checkNotNull(id, "inlong cluster id cannot be empty");
        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        InlongClusterResponse response = CommonBeanUtils.copyProperties(entity, InlongClusterResponse::new);
        LOGGER.debug("success to get inlong cluster info by id={}", id);
        return response;
    }

    @Override
    public PageInfo<InlongClusterResponse> list(InlongClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterEntity> entityPage = (Page<InlongClusterEntity>) clusterMapper.selectByCondition(request);
        List<InlongClusterResponse> list = CommonBeanUtils.copyListProperties(entityPage, InlongClusterResponse::new);
        PageInfo<InlongClusterResponse> page = new PageInfo<>(list);
        page.setTotal(list.size());
        LOGGER.debug("success to list inlong cluster by {}", request);
        return page;
    }

    @Override
    public List<String> listNodeIpByType(String type) {
        Preconditions.checkNotNull(type, "cluster type cannot be empty");
        // TODO need to be combined into one query operation
        InlongClusterPageRequest request = new InlongClusterPageRequest();
        request.setType(type);
        List<InlongClusterEntity> clusterList = clusterMapper.selectByCondition(request);
        List<String> ipList = new ArrayList<>();
        for (InlongClusterEntity clusterEntity : clusterList) {
            List<InlongClusterNodeEntity> nodeList = clusterNodeMapper.selectByParentId(clusterEntity.getId());
            for (InlongClusterNodeEntity nodeEntity : nodeList) {
                ipList.add(String.format("%s:%d", nodeEntity.getIp(), nodeEntity.getPort()));
            }
        }
        return ipList;
    }

    @Override
    public Boolean update(InlongClusterRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster={}", request);
        Preconditions.checkNotNull(request, "inlong cluster info cannot be empty");

        Integer id = request.getId();
        Preconditions.checkNotNull(id, "inlong cluster id cannot be empty");
        InlongClusterEntity exist = clusterMapper.selectByUniqueKey(request);
        if (exist != null && !Objects.equals(id, exist.getId())) {
            String errMsg = String.format("inlong cluster already exist for name=%s cluster tag=%s type=%s",
                    request.getName(), request.getClusterTag(), request.getType());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setModifier(operator);
        clusterMapper.updateById(entity);

        LOGGER.info("success to update inlong cluster={}", request);
        return true;
    }

    @Override
    public Boolean delete(Integer id, String operator) {
        Preconditions.checkNotNull(id, "cluster id cannot be empty");
        InlongClusterEntity entity = clusterMapper.selectById(id);
        if (entity == null || entity.getIsDeleted() > GlobalConstants.UN_DELETED) {
            LOGGER.error("inlong cluster not found by id={}, or was already deleted", id);
            return false;
        }

        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        clusterMapper.updateById(entity);
        LOGGER.info("success to delete inlong cluster for id={} by user={}", id, operator);
        return true;
    }

    @Override
    public Integer saveNode(ClusterNodeRequest request, String operator) {
        LOGGER.debug("begin to insert inlong cluster node={}", request);
        Preconditions.checkNotNull(request, "cluster node info cannot be empty");

        // check cluster node if exist
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        if (exist != null) {
            String errMsg = String.format("inlong cluster node already exist for type=%s ip=%s port=%s",
                    request.getType(), request.getIp(), request.getPort());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterNodeEntity entity = CommonBeanUtils.copyProperties(request, InlongClusterNodeEntity::new);
        entity.setCreator(operator);
        entity.setCreateTime(new Date());
        entity.setIsDeleted(GlobalConstants.UN_DELETED);
        clusterNodeMapper.insert(entity);

        LOGGER.info("success to add inlong cluster node={}", request);
        return entity.getId();
    }

    @Override
    public ClusterNodeResponse getNode(Integer id) {
        Preconditions.checkNotNull(id, "cluster node id cannot be empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        ClusterNodeResponse clusterNodeResponse = CommonBeanUtils.copyProperties(entity, ClusterNodeResponse::new);
        LOGGER.debug("success to get inlong cluster node by id={}", id);
        return clusterNodeResponse;
    }

    @Override
    public PageInfo<ClusterNodeResponse> listNode(InlongClusterPageRequest request) {
        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<InlongClusterNodeEntity> entityPage = (Page<InlongClusterNodeEntity>)
                clusterNodeMapper.selectByCondition(request);
        List<ClusterNodeResponse> nodeList = CommonBeanUtils.copyListProperties(entityPage, ClusterNodeResponse::new);
        PageInfo<ClusterNodeResponse> page = new PageInfo<>(nodeList);
        page.setTotal(nodeList.size());

        LOGGER.debug("success to list inlong cluster node by {}", request);
        return page;
    }

    @Override
    public Boolean updateNode(ClusterNodeRequest request, String operator) {
        LOGGER.debug("begin to update inlong cluster node={}", request);
        Preconditions.checkNotNull(request, "inlong cluster node cannot be empty");

        Integer id = request.getId();
        Preconditions.checkNotNull(id, "cluster node id cannot be empty");
        // check cluster node if exist
        InlongClusterNodeEntity exist = clusterNodeMapper.selectByUniqueKey(request);
        if (exist != null && !Objects.equals(id, exist.getId())) {
            String errMsg = String.format("inlong cluster node already exist for type=%s ip=%s port=%s)",
                    request.getType(), request.getIp(), request.getPort());
            LOGGER.error(errMsg);
            throw new BusinessException(errMsg);
        }

        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null) {
            LOGGER.error("cluster node not found by id={}", id);
            throw new BusinessException(ErrorCodeEnum.CLUSTER_NOT_FOUND);
        }
        CommonBeanUtils.copyProperties(request, entity, true);
        entity.setParentId(request.getParentId());
        entity.setModifier(operator);
        clusterNodeMapper.updateById(entity);

        LOGGER.info("success to update inlong cluster node={}", request);
        return true;
    }

    @Override
    public Boolean deleteNode(Integer id, String operator) {
        Preconditions.checkNotNull(id, "cluster node id cannot be empty");
        InlongClusterNodeEntity entity = clusterNodeMapper.selectById(id);
        if (entity == null || entity.getIsDeleted() > GlobalConstants.UN_DELETED) {
            LOGGER.error("inlong cluster node not found by id={}", id);
            return false;
        }
        entity.setIsDeleted(entity.getId());
        entity.setModifier(operator);
        clusterNodeMapper.updateById(entity);
        LOGGER.info("success to delete inlong cluster node by id={}", id);
        return true;
    }

    @Override
    public List<DataProxyResponse> getIpList(String clusterName) {
        InlongClusterPageRequest request = new InlongClusterPageRequest();
        request.setType(InlongGroupSettings.CLUSTER_DATA_PROXY);
        if (StringUtils.isNotBlank(clusterName)) {
            request.setName(clusterName);
        }
        List<InlongClusterEntity> clusterList = clusterMapper.selectByCondition(request);
        Preconditions.checkNotEmpty(clusterList,
                "data proxy cluster not found by type=" + InlongGroupSettings.CLUSTER_DATA_PROXY);

        List<DataProxyResponse> responseList = new ArrayList<>();
        for (InlongClusterEntity clusterEntity : clusterList) {
            Integer clusterId = clusterEntity.getId();
            List<InlongClusterNodeEntity> nodeList = clusterNodeMapper.selectByParentId(clusterId);
            for (InlongClusterNodeEntity nodeEntity : nodeList) {
                DataProxyResponse response = new DataProxyResponse();
                response.setId(clusterId);
                response.setIp(nodeEntity.getIp());
                response.setPort(nodeEntity.getPort());
                responseList.add(response);
            }
        }

        LOGGER.debug("success to list data proxy cluster for name={}, result={}", clusterName, responseList);
        return responseList;
    }

    // TODO need merge into getIpList method
    public List<DataProxyResponse> getIpListOld(String clusterName) {
        LOGGER.debug("begin to list data proxy by clusterName={}", clusterName);

        List<InlongClusterEntity> clusterList = clusterMapper.selectByNameAndType(clusterName,
                InlongGroupSettings.CLUSTER_DATA_PROXY);
        if (CollectionUtils.isEmpty(clusterList)) {
            LOGGER.warn("data proxy cluster not found by name={}", clusterName);
            return null;
        }

        InlongClusterEntity entity = clusterList.get(0);
        List<DataProxyResponse> responseList = new ArrayList<>();
        /*String ipStr = entity.getIp();
        while (ipStr.startsWith(URL_SPLITTER) || ipStr.endsWith(URL_SPLITTER)
                || ipStr.startsWith(HOST_SPLITTER) || ipStr.endsWith(HOST_SPLITTER)) {
            ipStr = InlongStringUtils.trimFirstAndLastChar(ipStr, URL_SPLITTER);
            ipStr = InlongStringUtils.trimFirstAndLastChar(ipStr, HOST_SPLITTER);
        }

        Integer id = entity.getId();
        Integer defaultPort = entity.getPort();
        int index = ipStr.indexOf(URL_SPLITTER);
        if (index <= 0) {
            DataProxyResponse response = new DataProxyResponse();
            response.setId(id);
            setIpAndPort(ipStr, defaultPort, response);
            responseList.add(response);
        } else {
            String[] urlArr = ipStr.split(URL_SPLITTER);
            for (String url : urlArr) {
                DataProxyResponse response = new DataProxyResponse();
                response.setId(id);
                setIpAndPort(url, defaultPort, response);
                responseList.add(response);
            }
        }*/

        LOGGER.debug("success to list data proxy cluster={}", responseList);
        return responseList;
    }

    private void setIpAndPort(String url, Integer defaultPort, DataProxyResponse response) {
        int idx = url.indexOf(HOST_SPLITTER);
        if (idx <= 0) {
            response.setIp(url);
            response.setPort(defaultPort);
        } else {
            response.setIp(url.substring(0, idx));
            response.setPort(Integer.valueOf(url.substring(idx + 1)));
        }
    }

    @Override
    public List<DataProxyConfig> getConfig() {
        // get all configs with inlong group status of 130, that is, config successful
        // TODO Optimize query conditions
        List<InlongGroupEntity> groupEntityList = groupMapper.selectAll(GroupStatus.CONFIG_SUCCESSFUL.getCode());
        List<DataProxyConfig> configList = new ArrayList<>();
        for (InlongGroupEntity groupEntity : groupEntityList) {
            String groupId = groupEntity.getInlongGroupId();
            String bizResource = groupEntity.getMqResource();

            DataProxyConfig config = new DataProxyConfig();
            config.setM(SCHEMA_M0_DAY);
            MQType mqType = MQType.forType(groupEntity.getMqType());
            if (mqType == MQType.TUBE) {
                config.setInlongGroupId(groupId);
                config.setTopic(bizResource);
            } else if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
                List<InlongStreamEntity> streamList = streamMapper.selectByGroupId(groupId);
                for (InlongStreamEntity stream : streamList) {
                    String topic = stream.getMqResource();
                    String streamId = stream.getInlongStreamId();
                    config.setInlongGroupId(groupId + "/" + streamId);
                    config.setTopic("persistent://" + clusterBean.getDefaultTenant() + "/" + bizResource + "/" + topic);
                }
            }
            configList.add(config);
        }

        return configList;
    }

    // TODO need optimized: query conditions use dataProxyClusterId
    @Override
    public ThirdPartyClusterDTO getConfigV2(String clusterName) {
        ThirdPartyClusterDTO object = new ThirdPartyClusterDTO();

        InlongClusterPageRequest request = new InlongClusterPageRequest();
        request.setName(clusterName);
        request.setType(InlongGroupSettings.CLUSTER_DATA_PROXY);
        List<InlongClusterEntity> clusterList = clusterMapper.selectByCondition(request);
        if (CollectionUtils.isEmpty(clusterList)) {
            LOGGER.warn("data proxy cluster not found by name={}, please register it firstly", clusterName);
            return object;
        }

        List<InlongGroupEntity> groupEntityList = groupMapper.selectAll(GroupStatus.CONFIG_SUCCESSFUL.getCode());
        if (CollectionUtils.isEmpty(groupEntityList)) {
            LOGGER.warn("not found any inlong group with success status for data proxy cluster={}", clusterName);
            return object;
        }

        // Get topic list by group id
        String mqType = groupEntityList.get(0).getMqType();
        List<DataProxyConfig> topicList = new ArrayList<>();
        for (InlongGroupEntity groupEntity : groupEntityList) {
            final String groupId = groupEntity.getInlongGroupId();
            final String mqResource = groupEntity.getMqResource();
            MQType type = MQType.forType(mqType);
            if (type == MQType.PULSAR || type == MQType.TDMQ_PULSAR) {
                List<InlongStreamEntity> streamList = streamMapper.selectByGroupId(groupId);
                for (InlongStreamEntity stream : streamList) {
                    DataProxyConfig topicConfig = new DataProxyConfig();
                    String streamId = stream.getInlongStreamId();
                    String topic = stream.getMqResource();
                    String tenant = clusterBean.getDefaultTenant();
                    /*InlongGroupPulsarEntity pulsarEntity = pulsarEntityMapper.selectByGroupId(groupId);
                    if (pulsarEntity != null && StringUtils.isNotEmpty(pulsarEntity.getTenant())) {
                        tenant = pulsarEntity.getTenant();
                    }*/
                    topicConfig.setInlongGroupId(groupId + "/" + streamId);
                    topicConfig.setTopic("persistent://" + tenant + "/" + mqResource + "/" + topic);
                    topicList.add(topicConfig);
                }
            } else if (type == MQType.TUBE) {
                DataProxyConfig topicConfig = new DataProxyConfig();
                topicConfig.setInlongGroupId(groupId);
                topicConfig.setTopic(mqResource);
                topicList.add(topicConfig);
            }
        }

        // construct mq set info
        List<ThirdPartyClusterInfo> mqSet = new ArrayList<>();
        String clusterTag = clusterList.get(0).getClusterTag();
        List<String> typeList = Arrays.asList(CLUSTER_TUBE, CLUSTER_PULSAR, CLUSTER_TDMQ_PULSAR);
        InlongClusterPageRequest pageRequest = new InlongClusterPageRequest();
        pageRequest.setClusterTag(clusterTag);
        pageRequest.setTypeList(typeList);
        List<InlongClusterEntity> mqClusterList = clusterMapper.selectByCondition(pageRequest);
        for (InlongClusterEntity cluster : mqClusterList) {
            ThirdPartyClusterInfo clusterInfo = new ThirdPartyClusterInfo();
            clusterInfo.setUrl(cluster.getUrl());
            clusterInfo.setToken(cluster.getToken());
            Map<String, String> configParams = GSON.fromJson(cluster.getExtParams(), Map.class);
            clusterInfo.setParams(configParams);

            mqSet.add(clusterInfo);
        }
        object.setMqSet(mqSet);
        object.setTopicList(topicList);

        return object;
    }

    // TODO need optimized
    @Override
    public String getAllConfig(String clusterName, String md5) {
        DataProxyConfigResponse response = new DataProxyConfigResponse();
        String configMd5 = proxyRepository.getProxyMd5(clusterName);
        if (configMd5 == null) {
            response.setResult(false);
            response.setErrCode(DataProxyConfigResponse.REQ_PARAMS_ERROR);
            return GSON.toJson(response);
        }

        // same config
        if (configMd5.equals(md5)) {
            response.setResult(true);
            response.setErrCode(DataProxyConfigResponse.NOUPDATE);
            response.setMd5(configMd5);
            response.setData(new DataProxyCluster());
            return GSON.toJson(response);
        }

        String configJson = proxyRepository.getProxyConfigJson(clusterName);
        if (configJson == null) {
            response.setResult(false);
            response.setErrCode(DataProxyConfigResponse.REQ_PARAMS_ERROR);
            return GSON.toJson(response);
        }

        return configJson;
    }

}
