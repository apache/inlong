package org.apache.inlong.manager.pojo.node.cls;

import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

/**
 * Tencent cloud log service data node info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.ELASTICSEARCH)
@ApiModel("Tencent cloud log service data node info")
public class TencentClsDataNodeInfo extends DataNodeInfo {

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

    public TencentClsDataNodeInfo() {
        setType(DataNodeType.CLS);
    }

    @Override
    public DataNodeRequest genRequest() {
        return CommonBeanUtils.copyProperties(this, TencentClsDataNodeRequest::new);
    }
}
