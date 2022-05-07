package org.apache.inlong.manager.common.pojo.group;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Inlong group metric
 */
@Data
@ApiModel("Inlong group metric")
public class InlongGroupMetricResponse {

    @ApiModelProperty(value = "Total record num of read")
    private Integer totalRecordNumOfRead;

    @ApiModelProperty(value = "Total recordNum byte num of read")
    private Integer totalRecordByteNumOfRead;

    @ApiModelProperty(value = "Total record num of write")
    private Integer totalRecordNumOfWrite;

    @ApiModelProperty(value = "Total recordNum byte num of write")
    private Integer totalRecordByteNumOfWrite;

    @ApiModelProperty(value = "Total dirty recode num")
    private Integer totalDirtyRecordNum;

    @ApiModelProperty(value = "Total dirty recode byte")
    private Integer totalDirtyRecordByte;

    @ApiModelProperty(value = "Total speed")
    private Integer totalSpeed;

    @ApiModelProperty(value = "Total Through put")
    private Integer totalThroughput;

    @ApiModelProperty(value = "Total Through put")
    private Integer totalDuration;

}
