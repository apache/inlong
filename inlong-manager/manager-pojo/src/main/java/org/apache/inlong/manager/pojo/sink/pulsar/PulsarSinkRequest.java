package org.apache.inlong.manager.pojo.sink.pulsar;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;

/**
 * pulsar sink request.
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Pulsar sink request")
@JsonTypeDefine(value = SinkType.PULSAR)
public class PulsarSinkRequest extends SinkRequest {

    @ApiModelProperty("pulsar service url")
    private String serviceUrl;
    @ApiModelProperty("pulsar tenant")
    private String tenant;
    @ApiModelProperty("pulsar namespace")
    private String namespace;
    @ApiModelProperty("pulsar topic")
    private String topic;
}
