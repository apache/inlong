package org.apache.inlong.manager.pojo.cluster.agent;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import javax.validation.constraints.NotNull;

/**
 * Agent cluster info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Agent cluster info")
public class AgentClusterDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty(value = "Transfer machine IP, such as: http://127.0.0.1:8080", notes = "Transfer machine IP")
    private String transferIp;

    @ApiModelProperty(value = "Version number of the server list collected by the cluster",
            notes = "Version number of the server list collected by the cluster")
    private String serverVersion;

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    /**
     * Get the dto instance from the request
     */
    public static AgentClusterDTO getFromRequest(AgentClusterRequest request) {
        return AgentClusterDTO.builder()
                .transferIp(request.getTransferIp())
                .serverVersion(request.getServerVersion())
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static AgentClusterDTO getFromJson(@NotNull String extParams) {
        try {
            return OBJECT_MAPPER.readValue(extParams, AgentClusterDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.CLUSTER_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

}
