package org.apache.inlong.manager.pojo.sink.pulsar;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.kafka.KafkaSinkRequest;

/**
 * Kafka sink info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "pulsar sink info")
@JsonTypeDefine(value = SinkType.PULSAR)
public class PulsarSink extends StreamSink {

    @ApiModelProperty("pulsar service url")
    private String serviceUrl;
    @ApiModelProperty("pulsar tenant")
    private String tenant;
    @ApiModelProperty("pulsar namespace")
    private String namespace;
    @ApiModelProperty("pulsar topic")
    private String topic;

    public PulsarSink(){
        this.setSinkType(SinkType.PULSAR);
    }

    @Override
    public SinkRequest genSinkRequest() {
        return CommonBeanUtils.copyProperties(this, PulsarSinkRequest::new);
    }
}
