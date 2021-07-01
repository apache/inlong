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
import org.apache.inlong.manager.common.pojo.datasource.SourceFileBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailListVO;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailPageRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.apache.inlong.manager.dao.entity.SourceFileBasicEntity;
import org.apache.inlong.manager.dao.entity.SourceFileDetailEntity;
import org.apache.inlong.manager.dao.mapper.BusinessEntityMapper;
import org.apache.inlong.manager.dao.mapper.SourceFileBasicEntityMapper;
import org.apache.inlong.manager.dao.mapper.SourceFileDetailEntityMapper;
import org.apache.inlong.manager.service.core.SourceFileService;
import org.apache.inlong.manager.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * File data source service layer implementation
 */
@Service
public class SourceFileServiceImpl implements SourceFileService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceFileServiceImpl.class);

    @Autowired
    private SourceFileBasicEntityMapper fileBasicMapper;
    @Autowired
    private SourceFileDetailEntityMapper fileDetailMapper;
    @Autowired
    private BusinessEntityMapper businessMapper;

    @Override
    public Integer saveBasic(SourceFileBasicInfo basicInfo, String operator) {
        LOGGER.info("begin to save file data source basic={}", basicInfo);
        Preconditions.checkNotNull(basicInfo, "file data source basic is empty");
        String bid = basicInfo.getBusinessIdentifier();
        String dsid = basicInfo.getDataStreamIdentifier();
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be added
        this.checkBizIsTempStatus(bid);

        // Each businessIdentifier + dataStreamIdentifier has only 1 valid basic information
        SourceFileBasicEntity exist = fileBasicMapper.selectByIdentifier(bid, dsid);
        if (exist != null) {
            LOGGER.error("file data source basic already exists, please check");
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DUPLICATE);
        }

        SourceFileBasicEntity entity = CommonBeanUtils.copyProperties(basicInfo, SourceFileBasicEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);
        entity.setCreateTime(new Date());
        fileBasicMapper.insertSelective(entity);

        LOGGER.info("success to save file data source basic");
        return entity.getId();
    }

    @Override
    public SourceFileBasicInfo getBasicByIdentifier(String bid, String dsid) {
        LOGGER.info("begin to get file data source basic by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        SourceFileBasicEntity entity = fileBasicMapper.selectByIdentifier(bid, dsid);
        SourceFileBasicInfo basicInfo = new SourceFileBasicInfo();
        if (entity == null) {
            LOGGER.error("file data source basic not found by dataStreamIdentifier={}", dsid);
            // throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_NOTFOUND);
            return basicInfo;
        }
        CommonBeanUtils.copyProperties(entity, basicInfo);

        LOGGER.info("success to get file data source basic");
        return basicInfo;
    }

    @Override
    public boolean updateBasic(SourceFileBasicInfo basicInfo, String operator) {
        LOGGER.info("begin to update file data source basic={}", basicInfo);
        Preconditions.checkNotNull(basicInfo, "file data source basic is empty");

        // The bid may be modified, it is necessary to determine whether the business status of
        // the modified bid supports modification
        this.checkBizIsTempStatus(basicInfo.getBusinessIdentifier());

        // If id is empty, add
        if (basicInfo.getId() == null) {
            this.saveBasic(basicInfo, operator);
        } else {
            SourceFileBasicEntity basicEntity = fileBasicMapper.selectByPrimaryKey(basicInfo.getId());
            if (basicEntity == null) {
                LOGGER.error("file data source basic not found by id={}, update failed", basicInfo.getId());
                throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_NOT_FOUND);
            }

            BeanUtils.copyProperties(basicInfo, basicEntity);
            basicEntity.setModifier(operator);
            fileBasicMapper.updateByPrimaryKeySelective(basicEntity);
        }

        LOGGER.info("success to update file data source basic");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteBasic(Integer id, String operator) {
        LOGGER.info("begin to delete file data source basic, id={}", id);
        Preconditions.checkNotNull(id, "file data source basic's id is null");

        SourceFileBasicEntity entity = fileBasicMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("file data source basic not found by id={}, delete failed", id);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_NOT_FOUND);
        }

        String bid = entity.getBusinessIdentifier();
        String dsid = entity.getDataStreamIdentifier();
        // Check if it can be deleted
        this.checkBizIsTempStatus(bid);

        // If there are related data source details, it is not allowed to delete
        List<SourceFileDetailEntity> detailEntities = fileDetailMapper.selectByIdentifier(bid, dsid);
        if (CollectionUtils.isNotEmpty(detailEntities)) {
            LOGGER.error("the data source basic have [{}] details, delete failed", detailEntities.size());
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_BASIC_DELETE_HAS_DETAIL);
        }

        entity.setIsDeleted(1);
        entity.setModifier(operator);
        int resultCount = fileBasicMapper.updateByPrimaryKey(entity);

        LOGGER.info("success to delete file data source basic");
        return resultCount >= 0;
    }

    @Override
    public Integer saveDetail(SourceFileDetailInfo detailInfo, String operator) {
        LOGGER.info("begin to save file data source detail={}", detailInfo);
        Preconditions.checkNotNull(detailInfo, "file data source detail is empty");
        Preconditions.checkNotNull(detailInfo.getBusinessIdentifier(), BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(detailInfo.getDataStreamIdentifier(), BizConstant.DSID_IS_EMPTY);

        // Check if it can be added
        this.checkBizIsTempStatus(detailInfo.getBusinessIdentifier());

        int id = saveDetailOpt(detailInfo, operator);
        LOGGER.info("success to save file data source detail");
        return id;
    }

    @Override
    public SourceFileDetailInfo getDetailById(Integer id) {
        LOGGER.info("begin to get file data source detail by id={}", id);
        Preconditions.checkNotNull(id, "file data source detail's id is null");

        SourceFileDetailEntity entity = fileDetailMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("file data source detail not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOT_FOUND);
        }
        SourceFileDetailInfo detailInfo = CommonBeanUtils.copyProperties(entity, SourceFileDetailInfo::new);

        LOGGER.info("success to get file data source detail");
        return detailInfo;
    }

    @Override
    public List<SourceFileDetailInfo> listDetailByIdentifier(String bid, String dsid) {
        LOGGER.info("begin list file data source detail by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);

        List<SourceFileDetailEntity> entities = fileDetailMapper.selectByIdentifier(bid, dsid);
        if (CollectionUtils.isEmpty(entities)) {
            LOGGER.error("file data source detail not found");
            // throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOTFOUND);
            return Collections.emptyList();
        }

        List<SourceFileDetailInfo> infoList = CommonBeanUtils.copyListProperties(entities, SourceFileDetailInfo::new);
        LOGGER.info("success to list file data source detail");
        return infoList;
    }

    @Override
    public PageInfo<SourceFileDetailListVO> listByCondition(SourceFileDetailPageRequest request) {
        LOGGER.info("begin to list file data source detail page by {}", request);

        PageHelper.startPage(request.getPageNum(), request.getPageSize());
        Page<SourceFileDetailEntity> page = (Page<SourceFileDetailEntity>) fileDetailMapper.selectByCondition(request);
        List<SourceFileDetailListVO> detailList = CommonBeanUtils.copyListProperties(page, SourceFileDetailListVO::new);

        // Encapsulate the paging query results into the PageInfo object to obtain related paging information
        PageInfo<SourceFileDetailListVO> pageInfo = new PageInfo<>(detailList);
        pageInfo.setTotal(page.getTotal());

        LOGGER.info("success to list file data source detail");
        return pageInfo;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean updateDetail(SourceFileDetailInfo detailInfo, String operator) {
        LOGGER.info("begin to update file data source detail={}", detailInfo);
        Preconditions.checkNotNull(detailInfo, "file data source detail is empty");
        Preconditions.checkNotNull(detailInfo.getBusinessIdentifier(), BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(detailInfo.getDataStreamIdentifier(), BizConstant.DSID_IS_EMPTY);

        // The bid may be modified, it is necessary to determine whether the business status of
        // the modified bid supports modification
        this.checkBizIsTempStatus(detailInfo.getBusinessIdentifier());

        // id exists, update, otherwise add
        if (detailInfo.getId() != null) {
            SourceFileDetailEntity entity = fileDetailMapper.selectByPrimaryKey(detailInfo.getId());
            if (entity == null) {
                LOGGER.error("file data source detail not found by id=" + detailInfo.getIp());
                throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOT_FOUND);
            }
            BeanUtils.copyProperties(detailInfo, entity);
            entity.setStatus(EntityStatus.BIZ_CONFIG_ING.getCode());
            entity.setModifier(operator);
            fileDetailMapper.updateByPrimaryKeySelective(entity);
        } else {
            saveDetailOpt(detailInfo, operator);
        }

        LOGGER.info("success to update file data source detail");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteDetail(Integer id, String operator) {
        LOGGER.info("begin to delete file data source detail, id={}", id);
        Preconditions.checkNotNull(id, "file data source detail's id is null");

        SourceFileDetailEntity entity = fileDetailMapper.selectByPrimaryKey(id);
        if (entity == null) {
            LOGGER.error("file data source detail not found by id={}", id);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DETAIL_NOT_FOUND);
        }

        // Check if it can be deleted
        this.checkBizIsTempStatus(entity.getBusinessIdentifier());

        entity.setIsDeleted(1);
        entity.setModifier(operator);
        int resultCount = fileDetailMapper.updateByPrimaryKey(entity);

        LOGGER.info("success to delete file data source detail");
        return resultCount >= 0;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean deleteAllByIdentifier(String bid, String dsid) {
        LOGGER.info("begin delete all file basic and detail by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(bid);

        fileBasicMapper.deleteByIdentifier(bid, dsid);
        fileDetailMapper.deleteByIdentifier(bid, dsid);
        LOGGER.info("success delete all file basic and detail");
        return true;
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public boolean logicDeleteAllByIdentifier(String bid, String dsid, String operator) {
        LOGGER.info("begin logic delete all file basic and detail by bid={}, dsid={}", bid, dsid);
        Preconditions.checkNotNull(bid, BizConstant.BID_IS_EMPTY);
        Preconditions.checkNotNull(dsid, BizConstant.DSID_IS_EMPTY);

        // Check if it can be deleted
        this.checkBizIsTempStatus(bid);

        fileBasicMapper.logicDeleteByIdentifier(bid, dsid, operator);
        fileDetailMapper.logicDeleteByIdentifier(bid, dsid, operator);
        LOGGER.info("success logic delete all file basic and detail");
        return true;
    }

    /**
     * Save data source details
     */
    @Transactional(rollbackFor = Throwable.class)
    int saveDetailOpt(SourceFileDetailInfo detailInfo, String operator) {
        // If there are data sources under the same bid, dsid, ip, username, the addition fails
        String bid = detailInfo.getBusinessIdentifier();
        String dsid = detailInfo.getDataStreamIdentifier();
        String ip = detailInfo.getIp();
        String username = detailInfo.getUsername();
        Integer count = fileDetailMapper.selectDetailExist(bid, dsid, ip, username);
        if (count > 0) {
            LOGGER.error("file data source already exists: bid=" + bid + ", dsid=" + dsid
                    + ", ip=" + ip + ", username=" + username);
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_DUPLICATE);
        }

        SourceFileDetailEntity detailEntity = CommonBeanUtils.copyProperties(detailInfo, SourceFileDetailEntity::new);
        detailEntity.setStatus(EntityStatus.DATA_RESOURCE_NEW.getCode());
        detailEntity.setCreator(operator);
        detailEntity.setModifier(operator);
        detailEntity.setCreateTime(new Date());
        fileDetailMapper.insertSelective(detailEntity);

        return detailEntity.getId();
    }

    /**
     * Check whether the business status is temporary
     *
     * @param bid Business identifier
     * @return Business entity for caller reuse
     */
    private void checkBizIsTempStatus(String bid) {
        BusinessEntity businessEntity = businessMapper.selectByIdentifier(bid);
        Preconditions.checkNotNull(businessEntity, "businessIdentifier is invalid");
        // Add/modify/delete is not allowed under certain business status
        if (EntityStatus.BIZ_TEMP_STATUS.contains(businessEntity.getStatus())) {
            LOGGER.error("business status was not allowed to add/update/delete data source info");
            throw new BusinessException(BizErrorCodeEnum.DATA_SOURCE_OPT_NOT_ALLOWED);
        }
    }

}