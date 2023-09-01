package org.apache.inlong.manager.pojo.node.pulsar;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

/**
 * StarRocks data node request
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.PULSAR)
@ApiModel("Pulsar data node request")
public class PulsarDataNodeRequest extends DataNodeRequest {

    @ApiModelProperty("pulsar service url")
    private String serviceUrl;
    @ApiModelProperty("pulsar admin url")
    private String adminUrl;

    public PulsarDataNodeRequest() {
        this.setType(DataNodeType.PULSAR);
    }
}
