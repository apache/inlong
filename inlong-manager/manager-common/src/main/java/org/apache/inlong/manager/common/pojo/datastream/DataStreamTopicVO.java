package org.apache.inlong.manager.common.pojo.datastream;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * Topic View Object of the Data stream
 */
@Data
@ApiModel("Data stream ID and topic interaction object")
public class DataStreamTopicVO {

    @ApiModelProperty(value = "InLong stream ID")
    private String inlongStreamId;

    @ApiModelProperty(value = "Message queue type, the data stream id corresponds to the topic of Pulsar one-to-one")
    private String mqResourceObj;

}
