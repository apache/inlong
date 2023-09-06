package org.apache.inlong.manager.service.sink.cls;

import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.service.sink.AbstractSinkOperator;
import org.springframework.stereotype.Service;

/**
 * tencent cloud log service sink operator
 */
@Service
public class TencentCloudLogServiceSinkOperator extends AbstractSinkOperator {

    @Override
    protected void setTargetEntity(SinkRequest request, StreamSinkEntity targetEntity) {

    }

    @Override
    protected String getSinkType() {
        return null;
    }

    @Override
    public Boolean accept(String sinkType) {
        return null;
    }

    @Override
    public StreamSink getFromEntity(StreamSinkEntity entity) {
        return null;
    }
}
