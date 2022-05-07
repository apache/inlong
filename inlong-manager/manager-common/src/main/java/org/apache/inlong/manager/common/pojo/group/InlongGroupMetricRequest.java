package org.apache.inlong.manager.common.pojo.group;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * inlongGroup metric query request
 */
@Data
@ApiModel("Heartbeat query request")
public class InlongGroupMetricRequest {

    @ApiModelProperty(value = "Component name, such as: Agent, Sort...")
    private String component = "Sort";

    @ApiModelProperty(value = "Inlong group id")
    private String inlongGroupId;

}
