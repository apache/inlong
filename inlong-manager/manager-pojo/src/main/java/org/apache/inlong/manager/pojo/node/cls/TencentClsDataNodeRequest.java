package org.apache.inlong.manager.pojo.node.cls;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

/**
 * Tencent cloud log service data node request
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.CLS)
@ApiModel("Tencent cloud log service data node request")
public class TencentClsDataNodeRequest extends DataNodeRequest {

    /**
     * Tencent cloud log service api secretKey
     */
    private String secretKey;

    /**
     * Tencent cloud log service api secretId
     */
    private String secretId;

    /**
     * Tencent cloud log service endpoint
     */
    private String endpoint;

    public TencentClsDataNodeRequest() {
        this.setType(DataNodeType.CLS);
    }

}
