package org.apache.inlong.manager.pojo.node.cls;

import io.swagger.annotations.ApiModel;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

/**
 * Tencent cloud log service data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Tencent cloud log service data node info")
public class TencentClsDataNodeDTO {

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

    /**
     * Get the dto instance from the request
     */
    public static TencentClsDataNodeDTO getFromRequest(TencentClsDataNodeRequest request, String extParams) {
        TencentClsDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? TencentClsDataNodeDTO.getFromJson(extParams)
                : new TencentClsDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static TencentClsDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, TencentClsDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT,
                    String.format("Failed to parse extParams for Tencent cloud log service node: %s", e.getMessage()));
        }
    }

}
