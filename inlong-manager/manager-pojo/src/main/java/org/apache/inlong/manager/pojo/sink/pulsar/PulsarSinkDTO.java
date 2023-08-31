package org.apache.inlong.manager.pojo.sink.pulsar;

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
import org.apache.inlong.manager.pojo.sink.kafka.KafkaSinkDTO;
import org.apache.inlong.manager.pojo.sink.kafka.KafkaSinkRequest;

/**
 * Pulsar sink info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarSinkDTO {

    @ApiModelProperty("pulsar service url")
    private String serviceUrl;
    @ApiModelProperty("pulsar tenant")
    private String tenant;
    @ApiModelProperty("pulsar namespace")
    private String namespace;
    @ApiModelProperty("pulsar topic")
    private String topic;

    /**
     * Get the dto instance from the request
     */
    public static PulsarSinkDTO getFromRequest(PulsarSinkRequest request, String extParams) {
        PulsarSinkDTO dto = StringUtils.isNotBlank(extParams) ? PulsarSinkDTO.getFromJson(extParams) : new PulsarSinkDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    public static PulsarSinkDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, PulsarSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    String.format("parse extParams of pulsar SinkDTO failure: %s", e.getMessage()));
        }
    }
}
