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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailListVO;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailPageRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.SourceDbBasicEntity;
import org.apache.inlong.manager.dao.entity.SourceDbDetailEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.SourceDbBasicEntityMapper;
import org.apache.inlong.manager.dao.mapper.SourceDbDetailEntityMapper;
import org.apache.inlong.manager.service.core.SourceDbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DB data source service layer implementation
 */
@Service
public class SourceDbServiceImpl implements SourceDbService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceDbServiceImpl.class);

    @Autowired
    private SourceDbBasicEntityMapper dbBasicMapper;
    @Autowired
    private SourceDbDetailEntityMapper dbDetailMapper;
    @Autowired
    private BusinessEntityMapper businessMapper;

    @Override
    public Integer saveBasic(SourceDbBasicInfo basicInfo, String operator) {
        LOGGER.info("begin to save db data source basic={}", basicInfo);
        Preconditions.checkNotNull(basicInfo, "db data source basic");
        String groupId = basicInfo.getInlongGroupId();
        String streamId = basicInfo.getInlongStreamId();
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be added
        this.checkBizIsTempStatus(groupId);

        // Each groupId + streamId has only 1 valid basic information
        SourceDbBasicEntity exist = dbBasicMapper.selectByIdentifier(groupId, streamId);
        if (exist != null) {
            LOGGER.error("db data source basic already exists");
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DUPLICATE);
        }

        SourceDbBasicEntity entity = CommonBeanUtils.copyProperties(basicInfo, SourceDbBasicEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(new Date());
        dbBasicMapper.insertSelective(entity);

        LOGGER.info("success to save db data source basic");
        return entity.getId();
    }

    @Override
    public SourceDbBasicInfo getBasicByIdentifier(String groupId, String streamId) {
        LOGGER.info("begin to get db data source basic by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        SourceDbBasicEntity entity = dbBasicMapper.selectByIdentifier(groupId, streamId);
        SourceDbBasicInfo basicInfo = new SourceDbBasicInfo();
        if (entity == null) {
            LOGGER.error("file data source basic not found by streamId={}", streamId);
            // throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_NOTFOUND);
            return basicInfo;
        }
        BeanUtils.copyProperties(entity, basicInfo);

        LOGGER.info("success to get db data source basic");
        return basicInfo;
    }

    @Override
    public boolean updateBasic(SourceDbBasicInfo basicInfo, String operator) {
        LOGGER.info("begin to update db data source basic={}", basicInfo);
        Preconditions.checkNotNull(basicInfo, "db data source basic is empty");

        // The groupId may be modified, it is necessary to determine whether the business status of
        // the modified groupId supports modification
        this.checkBizIsTempStatus(basicInfo.getInlongGroupId());

        // If id is empty, add
        if (basicInfo.getId() == null) {
            this.saveBasic(basicInfo, operator);
        } else {
            SourceDbBasicEntity basicEntity = dbBasicMapper.selectByPrimaryKey(basicInfo.getId());
            if (basicEntity == null) {
                LOGGER.error("db data source basic not found by id={}, update failed", basicInfo.getId());
                throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_NOT_FOUND);
            }
            BeanUtils.copyProperties(basicInfo, basicEntity);
            basicEntity.setModifier(operator);
            dbBasicMapper.updateByPrimaryKeySelective(basicEntity);
        }

        LOGGER.info("success to update db data source basic");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteBasic(Integer id, String operator) {
        LOGGER.info("begin to delete db data source basic, id={}", id);
        Preconditions.checkNotNull(id, "db data source basic's id is null");

        SourceDbBasicEntity entity = dbBasicMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("db data source basic not found by id={}, delete failed", id);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_NOT_FOUND);
        }

        String groupId = entity.getInlongGroupId();
        String streamId = entity.getInlongStreamId();
        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        // If there are related data source details, it is not allowed to delete
        List<SourceDbDetailEntity> detailEntities = dbDetailMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isNotEmpty(detailEntities)) {
            LOGGER.error("the data source basic have [{}] details, delete failed", detailEntities.size());
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_DELETE_HAS_DETAIL);
        }
        entity.setIsDeleted(1);
        entity.setModifier(operator);
        int resultCount = dbBasicMapper.updateByPrimaryKey(entity);

        LOGGER.info("success to delete db data source basic");
        return resultCount >= 0;
    }

    @Override
    public Integer saveDetail(SourceDbDetailInfo detailInfo, String operator) {
        LOGGER.info("begin to save db data source detail={}", detailInfo);
        Preconditions.checkNotNull(detailInfo, "db data source basic is null");
        Preconditions.checkNotNull(detailInfo.getInlongGroupId(), BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(detailInfo.getInlongStreamId(), BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be added
        this.checkBizIsTempStatus(detailInfo.getInlongGroupId());

        int id = saveDetailOpt(detailInfo, operator);
        LOGGER.info("success to save db data source detail");
        return id;
    }

    @Override
    public SourceDbDetailInfo getDetailById(Integer id) {
        LOGGER.info("begin to get db data source detail by id={}", id);
        Preconditions.checkNotNull(id, "db data source detail's id is null");

        SourceDbDetailEntity entity = dbDetailMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("db data source detail not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOT_FOUND);
        }
        SourceDbDetailInfo detailInfo = CommonBeanUtils.copyProperties(entity, SourceDbDetailInfo::new);

        LOGGER.info("success to get db data source detail");
        return detailInfo;
    }

    @Override
    public List<SourceDbDetailInfo> listDetailByIdentifier(String groupId, String streamId) {
        LOGGER.info("begin to list db data source detail by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);

        List<SourceDbDetailEntity> entities = dbDetailMapper.selectByIdentifier(groupId, streamId);
        if (CollectionUtils.isEmpty(entities)) {
            LOGGER.error("db data source detail not found");
            // throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOTFOUND);
            return Collections.emptyList();
        }
        List<SourceDbDetailInfo> infoList = CommonBeanUtils.copyListProperties(entities, SourceDbDetailInfo::new);

        LOGGER.info("success to list db data source detail");
        return infoList;
    }

    @Override
    public PageInfo<SourceDbDetailListVO> listByCondition(SourceDbDetailPageRequest request) {
        LOGGER.info("begin to list db data source detail page by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<SourceDbDetailEntity> entityPage = (Page<SourceDbDetailEntity>) dbDetailMapper.selectByCondition(request);
        List<SourceDbDetailListVO> detailList = CommonBeanUtils
                .copyListProperties(entityPage, SourceDbDetailListVO::new);

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<SourceDbDetailListVO> page = new PageInfo<>(detailList);
        page.setTotal(entityPage.getTotal());

        LOGGER.info("success to list db data source detail");
        return page;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateDetail(SourceDbDetailInfo detailInfo, String operator) {
        LOGGER.info("begin to update db data source detail={}", detailInfo);
        Preconditions.checkNotNull(detailInfo, "db data source detail is empty");
        Preconditions.checkNotNull(detailInfo.getInlongGroupId(), BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(detailInfo.getInlongStreamId(), BizConstant.STREAM_ID_IS_EMPTY);

        // The groupId may be modified, it is necessary to determine whether the business status of
        // the modified groupId supports modification
        this.checkBizIsTempStatus(detailInfo.getInlongGroupId());

        // id exists, update, otherwise add
        if (detailInfo.getId() != null) {
            SourceDbDetailEntity entity = dbDetailMapper.selectByPrimaryKey(detailInfo.getId());
            if (entity == null) {
                LOGGER.error("db data source detail not found by id=" + detailInfo.getId());
                throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOT_FOUND);
            }
            SourceDbDetailEntity dbEntity = CommonBeanUtils.copyProperties(detailInfo, SourceDbDetailEntity::new);
            dbEntity.setStatus(EntityStatus.BIZ_CONFIG_ING.getCode());
            dbEntity.setModifier(operator);
            dbDetailMapper.updateByPrimaryKeySelective(dbEntity);
        } else {
            saveDetailOpt(detailInfo, operator);
        }

        LOGGER.info("success to update db data source detail");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteDetail(Integer id, String operator) {
        LOGGER.info("begin to delete db data source detail, id={}", id);
        Preconditions.checkNotNull(id, "db data source detail's id is null");

        SourceDbDetailEntity entity = dbDetailMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("db data source detail not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOT_FOUND);
        }

        // Check if it can be deleted
        this.checkBizIsTempStatus(entity.getInlongGroupId());

        entity.setIsDeleted(1);
        entity.setModifier(operator);
        int resultCount = dbDetailMapper.updateByPrimaryKey(entity);
        LOGGER.info("success to delete db data source detail");
        return resultCount >= 0;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean deleteAllByIdentifier(String groupId, String streamId) {
        LOGGER.info("begin delete all db basic and detail by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        dbBasicMapper.deleteByIdentifier(groupId, streamId);
        dbDetailMapper.deleteByIdentifier(groupId, streamId);
        LOGGER.info("success delete all db basic and detail");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteAllByIdentifier(String groupId, String streamId, String operator) {
        LOGGER.info("begin logic delete all db basic and detail by groupId={}, streamId={}", groupId, streamId);
        Preconditions.checkNotNull(groupId, BizConstant.GROUP_ID_IS_EMPTY);
        Preconditions.checkNotNull(streamId, BizConstant.STREAM_ID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(groupId);

        dbBasicMapper.logicDeleteByIdentifier(groupId, streamId, operator);
        dbDetailMapper.logicDeleteByIdentifier(groupId, streamId, operator);
        LOGGER.info("success logic delete all db basic and detail");
        return true;
    }

    /**
     * Save data source details
     */
    @Transactional(rollbackFor = Throwable.class)
    int saveDetailOpt(SourceDbDetailInfo detailInfo, String operator) {
        // DB type judgment uniqueness: if the same groupId, streamId, dbName, connectionName
        // correspond to the same data source
        String groupId = detailInfo.getInlongGroupId();
        String streamId = detailInfo.getInlongStreamId();
        String dbName = detailInfo.getDbName();
        String connectionName = detailInfo.getConnectionName();
        Integer count = dbDetailMapper.selectDetailExist(groupId, streamId, dbName, connectionName);
        if (count > 0) {
            LOGGER.error("db source detail already exists, groupId={}, streamId={}, dbName={}, connectionName={}",
                    groupId, streamId, dbName, connectionName);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DUPLICATE);
        }

        SourceDbDetailEntity dbEntity = CommonBeanUtils.copyProperties(detailInfo, SourceDbDetailEntity::new);
        dbEntity.setStatus(EntityStatus.AGENT_ADD.getCode());
        dbEntity.setCreator(operator);
        dbEntity.setModifier(operator);
        dbEntity.setCreateTime(new Date());
        dbDetailMapper.insertSelective(dbEntity);

        return dbEntity.getId();
    }

    /**
     * Check whether the business status is temporary
     *
     * @param groupId Business group id
     * @return usiness entity for caller reuse
     */
    private BusinessEntity checkBizIsTempStatus(String groupId) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(groupId);
        Preconditions.checkNotNull(businessEntity, "groupId is invalid");
        // Add/modify/delete is not allowed under certain business status
        if (EntityStatus.BIZ_TEMP_STATUS.contains(businessEntity.getStatus())) {
            LOGGER.error("business status was not allowed to add/update/delete data source info");
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_OPT_NOT_ALLOWED);
        }

        return businessEntity;
    }
}