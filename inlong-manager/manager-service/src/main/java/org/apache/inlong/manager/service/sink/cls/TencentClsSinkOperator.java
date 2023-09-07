package org.apache.inlong.manager.service.sink.cls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.pojo.node.cls.TencentClsDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.cls.TencentClsDTO;
import org.apache.inlong.manager.pojo.sink.cls.TencentClsRequest;
import org.apache.inlong.manager.pojo.sink.cls.TencentClsSink;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Tencent cloud log service sink operator
 */
@Service
public class TencentClsSinkOperator extends AbstractSinkOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TencentClsSinkOperator.class);

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage()+ ": " + getSinkType());
        }
        TencentClsRequest sinkRequest = (TencentClsRequest) request;
        try {
            TencentClsDTO dto = TencentClsDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
            targetEntity.setExtParams(objectMapper.writeValueAsString(dto));
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_SAVE_FAILED,
                    String.format("serialize extParams of Doris SinkDTO failure: %s", e.getMessage()));
        }
    }

    @Override
    protected String getSinkType() {
        return SinkType.CLS;
    }

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.CLS.equals(sinkType);
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        TencentClsSink sink = new TencentClsSink();
        if (entity == null) {
            return sink;
        }

        TencentClsDTO dto = TencentClsDTO.getFromJson(entity.getExtParams());
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(entity.getDataNodeName(),
                DataNodeType.CLS);
        try {
            objectMapper.readValue(dataNodeEntity.getExtParams(), TencentClsDataNodeDTO.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        List<SinkField> sinkFields = getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    public Map<String, String> parse2IdParams(StreamSinkEntity streamSink, List<String> fields) {

        return super.parse2IdParams(streamSink, fields);
    }
}
