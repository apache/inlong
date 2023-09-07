package org.apache.inlong.manager.pojo.sink.cls;

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
 * Sink info of Tencent cloud log service
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TencentClsDTO {

    /**
     * Tencent cloud log service topic id
     */
    private String topicID;

    /**
     * Tencent cloud log service topic save time
     */
    private Integer saveTime;

    /**
     * Get the dto instance from the request
     */
    public static TencentClsDTO getFromRequest(TencentClsRequest request, String extParams) {
        TencentClsDTO dto =
                StringUtils.isNotBlank(extParams) ? TencentClsDTO.getFromJson(extParams) : new TencentClsDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    public static TencentClsDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, TencentClsDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT,
                    String.format("parse extParams of Kafka SinkDTO failure: %s", e.getMessage()));
        }
    }
}
