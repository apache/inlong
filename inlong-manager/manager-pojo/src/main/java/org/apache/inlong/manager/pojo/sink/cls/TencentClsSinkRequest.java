package org.apache.inlong.manager.pojo.sink.cls;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.SinkRequest;

/**
 * Tencent cloud log service sink request.
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Tencent cloud log service sink request")
@JsonTypeDefine(value = SinkType.CLS)
public class TencentClsSinkRequest extends SinkRequest {

    /**
     * Tencent cloud log service topic id
     */
    private String topicID;

    /**
     * Tencent cloud log service topic save time
     */
    private Integer saveTime;
}
