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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.commons.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.commons.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.commons.pojo.dataproxy.ProxyPulsarDTO;
import org.apache.inlong.commons.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.DataProxyClusterPageRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyClusterSet;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpRequest;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyIpResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.ClusterInfoEntity;
import org.apache.inlong.manager.dao.entity.DataProxyClusterEntity;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.ClusterInfoMapper;
import org.apache.inlong.manager.dao.mapper.DataProxyClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.DataProxyClusterService;
import org.apache.inlong.manager.service.repository.DataProxyConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * DataProxy cluster service layer implementation class
 */
@Service
@Slf4j
public class DataProxyClusterServiceImpl implements DataProxyClusterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataProxyClusterServiceImpl.class);

    @Autowired
    private DataProxyClusterEntityMapper dataProxyClusterMapper;
    @Autowired
    private BusinessEntityMapper businessMapper;
    @Autowired
    private DataStreamEntityMapper dataStreamMapper;
    @Autowired
    private DataProxyConfigRepository proxyRepository;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private ClusterInfoMapper clusterInfoMapper;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Integer save(DataProxyClusterInfo clusterInfo, String operator) {
        LOGGER.info("begin to save data proxy cluster={}", clusterInfo);
        Preconditions.checkNotNull(clusterInfo, "data proxy cluster is empty");

        DataProxyClusterEntity entity = CommonBeanUtils.copyProperties(clusterInfo, DataProxyClusterEntity::new);

        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(new Date());
        dataProxyClusterMapper.insertSelective(entity);

        LOGGER.info("success to save data proxy cluster");
        return entity.getId();
    }

    @Override
    public DataProxyClusterInfo get(Integer id) {
        LOGGER.info("begin to get data proxy cluster by id={}", id);
        Preconditions.checkNotNull(id, "data proxy cluster id is empty");

        DataProxyClusterEntity entity = dataProxyClusterMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("data proxy cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        DataProxyClusterInfo clusterInfo = CommonBeanUtils.copyProperties(entity, DataProxyClusterInfo::new);

        LOGGER.info("success to get data proxy cluster info");
        return clusterInfo;
    }

    @Override
    public PageInfo<DataProxyClusterInfo> listByCondition(DataProxyClusterPageRequest request) {
        LOGGER.info("begin to list data proxy cluster by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<DataProxyClusterEntity> entityPage = (Page<DataProxyClusterEntity>) dataProxyClusterMapper
                .selectByCondition(request);
        List<DataProxyClusterInfo> clusterList = CommonBeanUtils.copyListProperties(entityPage,
                DataProxyClusterInfo::new);
        // Encapsulate the paging query results into the PageInfo object to obtain
        // related paging information
        PageInfo<DataProxyClusterInfo> page = new PageInfo<>(clusterList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list data proxy cluster");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean update(DataProxyClusterInfo clusterInfo, String operator) {
        LOGGER.info("begin to update data proxy cluster={}", clusterInfo);
        Preconditions.checkNotNull(clusterInfo, "data proxy cluster is empty");
        Integer id = clusterInfo.getId();
        Preconditions.checkNotNull(id, "data proxy cluster id is empty");

        DataProxyClusterEntity entity = dataProxyClusterMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("data proxy cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        CommonBeanUtils.copyProperties(clusterInfo, entity, true);
        entity.setModifier(operator);
        dataProxyClusterMapper.updateByPrimaryKeySelective(entity);

        LOGGER.info("success to update data proxy cluster");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public Boolean delete(Integer id, String operator) {
        LOGGER.info("begin to delete data proxy cluster by id={}", id);
        Preconditions.checkNotNull(id, "data proxy cluster id is empty");

        DataProxyClusterEntity entity = dataProxyClusterMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("data proxy cluster not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.CLUSTER_NOT_FOUND);
        }

        entity.setIsDeleted(EntityStatus.IS_DELETED.getCode());
        entity.setStatus(EntityStatus.DELETED.getCode());
        entity.setModifier(operator);
        dataProxyClusterMapper.updateByPrimaryKey(entity);

        LOGGER.info("success to delete data proxy cluster");
        return true;
    }

    @Override
    public List<DataProxyIpResponse> getIpList(DataProxyIpRequest request) {
        LOGGER.debug("begin to get data proxy ip list, request: {}", request);

        List<DataProxyClusterEntity> entityList = dataProxyClusterMapper.selectAll();
        if (entityList == null || entityList.isEmpty()) {
            LOGGER.info("success to get data proxy ip list, but result is empty, request ip={}", request.getIp());
            return null;
        }

        final String requestNetTag = request.getNetTag();
        List<DataProxyIpResponse> responseList = new ArrayList<>();
        for (DataProxyClusterEntity entity : entityList) {
            // Subject to the net tag of any entity
            String netTag = requestNetTag;
            if (StringUtils.isEmpty(netTag)) {
                int innerIp = entity.getIsInnerIp();
                if (innerIp == 1) {
                    netTag = "auto";
                } else {
                    netTag = entity.getNetType();
                }

                if (StringUtils.isEmpty(netTag)) {
                    netTag = "all";
                }
            }

            DataProxyIpResponse response = new DataProxyIpResponse();
            response.setId(entity.getId());
            response.setPort(entity.getPort());
            response.setIp(entity.getAddress());
            response.setNetTag(netTag);

            responseList.add(response);
        }

        LOGGER.info("success to get data proxy ip list, response size={}", responseList.size());
        return responseList;
    }

    @Override
    public List<DataProxyConfig> getConfig() {
        // get all configs with business status of 130, that is, config successful
        // TODO Optimize query conditions
        List<BusinessEntity> bizEntityList = businessMapper.selectAll(EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode());
        List<DataProxyConfig> configList = new ArrayList<>();
        for (BusinessEntity entity : bizEntityList) {
            String groupId = entity.getInlongGroupId();
            String bizResource = entity.getMqResourceObj();

            DataProxyConfig config = new DataProxyConfig();
            config.setM(entity.getSchemaName());
            if (BizConstant.MIDDLEWARE_TUBE.equals(entity.getMiddlewareType())) {
                config.setInlongGroupId(groupId);
                config.setTopic(bizResource);
            } else if (BizConstant.MIDDLEWARE_PULSAR.equals(entity.getMiddlewareType())) {
                List<DataStreamEntity> streamList = dataStreamMapper.selectByGroupId(groupId);
                for (DataStreamEntity stream : streamList) {
                    String streamId = stream.getInlongStreamId();
                    config.setInlongGroupId(groupId + "/" + streamId);
                    config.setTopic("persistent://" + clusterBean.getDefaultTenant() + "/" + groupId + "/" + streamId);

                }
            }
            configList.add(config);
        }

        return configList;
    }

    /**
     * query data proxy config by cluster id, result includes pulsar cluster configs and topic etc
     *
     * @param dataproxyClusterName
     * @return
     */
    @Override
    public ProxyPulsarDTO getConfigV2(String dataproxyClusterName) {
        ProxyPulsarDTO object = new ProxyPulsarDTO();
        List<PulsarClusterInfo> pulsarSet = new ArrayList<>();
        List<DataProxyConfig> topicList = new ArrayList<>();

        DataProxyClusterEntity dataProxyClusterEntity = dataProxyClusterMapper.selectByName(dataproxyClusterName);
        List<String> groudIdList = businessMapper.selectGroupIdByProxyId(dataProxyClusterEntity.getId());
        ClusterRequest request = ClusterRequest.builder().mqSetName(dataProxyClusterEntity.getMqSetName()).build();
        List<ClusterInfoEntity> clusterInfoEntities = clusterInfoMapper
                .selectByCondition(request);
        String tenant = clusterBean.getDefaultTenant();
        /*
         * based on group id, get topiclist
         */
        for (String groupId : groudIdList) {
            List<DataStreamEntity> streamList = dataStreamMapper.selectByGroupId(groupId);
            for (DataStreamEntity stream : streamList) {
                DataProxyConfig topicConfig = new DataProxyConfig();
                String streamId = stream.getInlongStreamId();
                topicConfig.setInlongGroupId(groupId + "/" + streamId);
                topicConfig.setTopic("persistent://" + tenant + "/" + groupId + "/" + streamId);
                topicList.add(topicConfig);

            }
        }
        /*
         * construct pulsarSet info
         */
        Gson gson = new Gson();
        for (ClusterInfoEntity cluster : clusterInfoEntities) {
            PulsarClusterInfo pulsarCluster = new PulsarClusterInfo();
            pulsarCluster.setUrl(cluster.getUrl());
            pulsarCluster.setToken(cluster.getToken());
            Map<String, String> configParams = gson.fromJson(cluster.getExtProps(), Map.class);
            pulsarCluster.setParams(configParams);

            pulsarSet.add(pulsarCluster);
        }

        object.setPulsarSet(pulsarSet);
        object.setTopicList(topicList);

        return object;
    }

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
        if (configMd5 == null || !configMd5.equals(md5)) {
            return this.getErrorAllConfig();
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
