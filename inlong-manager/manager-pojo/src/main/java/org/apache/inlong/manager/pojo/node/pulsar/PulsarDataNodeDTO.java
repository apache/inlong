package org.apache.inlong.manager.pojo.node.pulsar;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
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
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeDTO;
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeRequest;

/**
 * StarRocks data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("pulsar data node info")
public class PulsarDataNodeDTO {

    @ApiModelProperty("pulsar service url")
    private String serviceUrl;
    @ApiModelProperty("pulsar admin url")
    private String adminUrl;


    /**
     * Get the dto instance from the request
     */
    public static PulsarDataNodeDTO getFromRequest(PulsarDataNodeRequest request, String extParams) {
        PulsarDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? PulsarDataNodeDTO.getFromJson(extParams)
                : new PulsarDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static PulsarDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, PulsarDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT,
                    String.format("Failed to parse extParams for StarRocks node: %s", e.getMessage()));
        }
    }
}
