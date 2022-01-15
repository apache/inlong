package org.apache.inlong.manager.common.pojo.sort;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel("Sort-StandAlone cluster config request")
public class SortStandAloneClusterConfigRequest {

    @ApiModelProperty
    String clusterName;

    @ApiModelProperty
    String md5;

    @ApiModelProperty
    String apiVersion;
}
