package org.apache.inlong.manager.pojo.node.pulsar;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeRequest;

/**
 * Pulsar data node info
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.PULSAR)
@ApiModel("Pulsar data node info")
public class PulsarDataNodeInfo extends DataNodeInfo {

    @ApiModelProperty("pulsar service url")
    private String serviceUrl;
    @ApiModelProperty("pulsar admin url")
    private String adminUrl;

    public PulsarDataNodeInfo() {
        this.setType(DataNodeType.PULSAR);
    }

    @Override
    public PulsarDataNodeRequest genRequest() {
        return CommonBeanUtils.copyProperties(this, PulsarDataNodeRequest::new);
    }
}
