package org.apache.inlong.manager.pojo.source.dameng;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.source.hudi.HudiSourceDTO;
import org.apache.inlong.manager.pojo.source.hudi.HudiSourceRequest;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DamengSourceDTO {

    @ApiModelProperty("The database name")
    private String dbName;

    @ApiModelProperty("Schema name")
    private String schemaName;

    @ApiModelProperty("The table name")
    private String tableName;

    @ApiModelProperty("host")
    private String host;

    @ApiModelProperty("port")
    private String port;

    @ApiModelProperty("username")
    private String username;

    @ApiModelProperty("password")
    private String password;

    @ApiModelProperty("Scan startup mode")
    private String scanStartupMode;

    @ApiModelProperty("Primary key must be shared by all tables")
    private String primaryKey;

    @ApiModelProperty("Need transfer total database")
    @Builder.Default
    private boolean allMigration = false;

    /**
     * Get the dto instance from the request
     */
    public static DamengSourceDTO getFromRequest(DamengSourceRequest request) {
        DamengSourceDTO damengSourceDTO = new DamengSourceDTO();
        CommonBeanUtils.copyProperties(request, damengSourceDTO);
        return damengSourceDTO;
    }

    /**
     * Get the dto instance from the JSON string
     */
    public static DamengSourceDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, DamengSourceDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_INCORRECT,
                    String.format("parse extParams of DamengSourceDTO failure: %s", e.getMessage()));
        }
    }
}
