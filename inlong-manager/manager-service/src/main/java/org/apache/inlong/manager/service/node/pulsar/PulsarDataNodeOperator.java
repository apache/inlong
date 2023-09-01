package org.apache.inlong.manager.service.node.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeDTO;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeInfo;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeRequest;
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeDTO;
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeInfo;
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeRequest;
import org.apache.inlong.manager.service.node.AbstractDataNodeOperator;
import org.apache.inlong.manager.service.node.starrocks.StarRocksDataNodeOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;
import org.apache.inlong.manager.service.resource.sink.starrocks.StarRocksJdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PulsarDataNodeOperator extends AbstractDataNodeOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarDataNodeOperator.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Boolean accept(String dataNodeType) {
        return getDataNodeType().equals(dataNodeType);
    }

    @Override
    public String getDataNodeType() {
        return DataNodeType.PULSAR;
    }

    @Override
    public DataNodeInfo getFromEntity(DataNodeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.DATA_NODE_NOT_FOUND);
        }

        PulsarDataNodeInfo pulsarDataNodeInfo = new PulsarDataNodeInfo();
        CommonBeanUtils.copyProperties(entity, pulsarDataNodeInfo);
        if (StringUtils.isNotBlank(entity.getExtParams())) {
            PulsarDataNodeDTO dto = PulsarDataNodeDTO.getFromJson(entity.getExtParams());
            CommonBeanUtils.copyProperties(dto, pulsarDataNodeInfo);
        }
        return pulsarDataNodeInfo;
    }

    @Override
    protected void setTargetEntity(DataNodeRequest request, DataNodeEntity targetEntity) {
        PulsarDataNodeRequest nodeRequest = (PulsarDataNodeRequest) request;
        CommonBeanUtils.copyProperties(nodeRequest, targetEntity, true);
        try {
            PulsarDataNodeDTO dto = PulsarDataNodeDTO.getFromRequest(nodeRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("Failed to build extParams for StarRocks node: %s", e.getMessage()));
        }
    }

    @Override
    public Boolean testConnection(DataNodeRequest request) {
        PulsarDataNodeRequest pulsarDataNodeRequest = (PulsarDataNodeRequest) request;
        String adminUrl = pulsarDataNodeRequest.getAdminUrl();
        String token = pulsarDataNodeRequest.getToken();
        Preconditions.expectNotBlank(adminUrl, ErrorCodeEnum.INVALID_PARAMETER, "connection jdbcUrl cannot be empty");
        if (PulsarUtils.testConnection(adminUrl, token)) {
            LOGGER.info("pulsar connection not null - connection success for jdbcUrl={}, token={}",
                    adminUrl, token);
            return true;
        } else {
            String errMsg = String.format("StarRocks connection failed for jdbcUrl=%s,  token=%s",
                    adminUrl, token);
            throw new BusinessException(errMsg);
        }
    }
}
