package org.apache.inlong.manager.service.node.cls;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.cls.TencentClsDataNodeDTO;
import org.apache.inlong.manager.pojo.node.cls.TencentClsDataNodeInfo;
import org.apache.inlong.manager.pojo.node.cls.TencentClsDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TencentClsDataNodeOperator extends AbstractDataNodeOperator {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        TencentClsDataNodeRequest esRequest = (TencentClsDataNodeRequest) request;
        CommonBeanUtils.copyProperties(esRequest, targetEntity, true);
        try {
            TencentClsDataNodeDTO dto =
                    TencentClsDataNodeDTO.getFromRequest(esRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Failed to build extParams for Tencent cloud log service node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean accept(String dataNodeType) {
        return DataNodeType.CLS.equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.CLS;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }
        TencentClsDataNodeInfo info = new TencentClsDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, info);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            TencentClsDataNodeDTO dto = TencentClsDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, info);
        }
        return info;
    }

    @Override
    public Boolean testConnection(DataNodeRequest request) {
        return super.testConnection(request);
    }
}
