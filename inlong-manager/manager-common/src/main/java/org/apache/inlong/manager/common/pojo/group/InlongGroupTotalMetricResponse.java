package org.apache.inlong.manager.common.pojo.group;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamMetricResponse;

import java.util.List;
import java.util.Map;

/**
 * Inlong group total metric
 */
@Data
@ApiModel("Inlong group total metric")
public class InlongGroupTotalMetricResponse {

    @ApiModelProperty(value = "Inlong group metric response")
    private InlongGroupMetricResponse inlongGroupMetricResponse;

    @ApiModelProperty(value = "Stream metric map")
    private Map<String, List<InlongStreamMetricResponse>> streamMetricMap;
}
