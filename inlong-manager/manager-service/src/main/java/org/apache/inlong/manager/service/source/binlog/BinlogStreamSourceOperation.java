package org.apache.inlong.manager.service.source.binlog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceDTO;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceListResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.source.StreamSourceOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.function.Supplier;

/**
 * Binlog source operation
 */
@Service
public class BinlogStreamSourceOperation implements StreamSourceOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogStreamSourceOperation.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    @Override
    public Boolean accept(SourceType sourceType) {
        return SourceType.DB_BINLOG == sourceType;
    }

    @Override
    public Integer saveOpt(SourceRequest request, String operator) {
        String sourceType = request.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_DB_BINLOG.equals(sourceType),
                ErrorCodeEnum.SOURCE_TYPE_NOT_SUPPORT.getMessage() + ": " + sourceType);

        BinlogSourceRequest sourceRequest = (BinlogSourceRequest) request;
        StreamSourceEntity entity = CommonBeanUtils.copyProperties(sourceRequest, StreamSourceEntity::new);
        entity.setStatus(EntityStatus.SOURCE_NEW.getCode());
        entity.setIsDeleted(EntityStatus.UN_DELETED.getCode());
        entity.setCreator(operator);
        entity.setModifier(operator);
        Date now = new Date();
        entity.setCreateTime(now);
        entity.setModifyTime(now);

        // get the ext params
        BinlogSourceDTO dto = BinlogSourceDTO.getFromRequest(sourceRequest);
        try {
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_SAVE_FAILED);
        }
        sourceMapper.insert(entity);

        Integer sourceId = entity.getId();
        request.setId(sourceId);
        return sourceId;
    }

    @Override
    public SourceResponse getById(@NotNull String sourceType, @NotNull Integer id) {
        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(id);
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        String existType = entity.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_DB_BINLOG.equals(existType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, Constant.SOURCE_DB_BINLOG, existType));

        return this.getFromEntity(entity, BinlogSourceResponse::new);
    }

    @Override
    public <T> T getFromEntity(StreamSourceEntity entity, Supplier<T> target) {
        T result = target.get();
        if (entity == null) {
            return result;
        }

        String existType = entity.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_DB_BINLOG.equals(existType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, Constant.SOURCE_DB_BINLOG, existType));

        BinlogSourceDTO dto = BinlogSourceDTO.getFromJson(entity.getExtParams());
        CommonBeanUtils.copyProperties(entity, result, true);
        CommonBeanUtils.copyProperties(dto, result, true);

        return result;
    }

    @Override
    public PageInfo<? extends SourceListResponse> getPageInfo(Page<StreamSourceEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return new PageInfo<>();
        }
        return entityPage.toPageInfo(entity -> this.getFromEntity(entity, BinlogSourceListResponse::new));
    }

    @Override
    public void updateOpt(SourceRequest request, String operator) {
        String sourceType = request.getSourceType();
        Preconditions.checkTrue(Constant.SOURCE_DB_BINLOG.equals(sourceType),
                String.format(Constant.SOURCE_TYPE_NOT_SAME, Constant.SOURCE_DB_BINLOG, sourceType));

        StreamSourceEntity entity = sourceMapper.selectByPrimaryKey(request.getId());
        Preconditions.checkNotNull(entity, ErrorCodeEnum.SOURCE_INFO_NOT_FOUND.getMessage());
        BinlogSourceRequest sourceRequest = (BinlogSourceRequest) request;
        CommonBeanUtils.copyProperties(sourceRequest, entity, true);
        try {
            BinlogSourceDTO dto = BinlogSourceDTO.getFromRequest(sourceRequest);
            entity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT.getMessage());
        }

        entity.setPreviousStatus(entity.getStatus());
        entity.setStatus(EntityStatus.GROUP_CONFIG_ING.getCode());
        entity.setModifier(operator);
        entity.setModifyTime(new Date());
        sourceMapper.updateByPrimaryKeySelective(entity);

        LOGGER.info("success to update source of type={}", sourceType);
    }

}
