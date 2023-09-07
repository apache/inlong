package org.apache.inlong.manager.service.sink.cls;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.pojo.node.cls.TencentClsDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.cls.TencentClsSinkDTO;
import org.apache.inlong.manager.pojo.sink.cls.TencentClsSinkRequest;
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
    private static final String KEY_FIELDS = "fieldNames";
    private static final String SECRET_KEY = "secretKey";
    private static final String SECRET_ID = "secretId";
    private static final String END_POINT = "endpoint";
    private static final String TOPIC_ID = "topicId";

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {
        if (!this.getSinkType().equals(request.getSinkType())) {
            throw new BusinessException(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT,
                    ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage() + ": " + getSinkType());
        }
        TencentClsSinkRequest sinkRequest = (TencentClsSinkRequest) request;
        try {
            TencentClsSinkDTO dto = TencentClsSinkDTO.getFromRequest(sinkRequest, targetEntity.getExtParams());
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

        TencentClsSinkDTO dto = TencentClsSinkDTO.getFromJson(entity.getExtParams());
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(entity.getDataNodeName(),
                DataNodeType.CLS);
        TencentClsDataNodeDTO tencentClsDataNodeDTO = JsonUtils.parseObject(dataNodeEntity.getExtParams(),
                TencentClsDataNodeDTO.class);
        CommonBeanUtils.copyProperties(entity, sink, true);
        CommonBeanUtils.copyProperties(dto, sink, true);
        CommonBeanUtils.copyProperties(tencentClsDataNodeDTO, sink, true);
        List<SinkField> sinkFields = getSinkFields(entity.getId());
        sink.setSinkFieldList(sinkFields);
        return sink;
    }

    @Override
    public Map<String, String> parse2IdParams(StreamSinkEntity streamSink, List<String> fields) {
        Map<String, String> params = super.parse2IdParams(streamSink, fields);
        TencentClsSinkDTO tencentClsSinkDTO = JsonUtils.parseObject(streamSink.getExtParams(), TencentClsSinkDTO.class);
        params.put(TOPIC_ID, tencentClsSinkDTO.getTopicID());
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(streamSink.getDataNodeName(),
                DataNodeType.CLS);
        TencentClsDataNodeDTO tencentClsDataNodeDTO = JsonUtils.parseObject(dataNodeEntity.getExtParams(),
                TencentClsDataNodeDTO.class);
        params.put(SECRET_ID, tencentClsDataNodeDTO.getSecretId());
        params.put(SECRET_KEY, tencentClsDataNodeDTO.getSecretKey());
        params.put(END_POINT, tencentClsDataNodeDTO.getEndpoint());
        StringBuilder fieldNames = new StringBuilder();
        for (String field : fields) {
            fieldNames.append(field).append(" ");
        }
        params.put(KEY_FIELDS, fieldNames.toString());
        return params;
    }
}
