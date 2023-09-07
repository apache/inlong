package org.apache.inlong.manager.pojo.sink.cls;

import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;

/**
 * Tencent cloud log service sink info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Tencent cloud log service sink info")
@JsonTypeDefine(value = SinkType.CLS)
public class TencentClsSink extends StreamSink {

    /**
     * Tencent cloud log service topic id
     */
    private String topicID;

    /**
     * Tencent cloud log service api secretKey
     */
    private String secretKey;

    /**
     * Tencent cloud log service api secretId
     */
    private String secretId;

    /**
     * Tencent cloud log service endpoint
     */
    private String endpoint;

    public TencentClsSink() {
        this.setSinkType(SinkType.CLS);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, TencentClsRequest::new);
    }
}
