package org.apache.inlong.manager.pojo.source.dameng;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;

import java.util.Map;

@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Dameng source info")
@JsonTypeDefine(value = SourceType.DAMENG)
public class DamengSource extends StreamSource {
    @ApiModelProperty("The database name")
    private String dbName;

    @ApiModelProperty("Schema name")
    private String schemaName;

    @ApiModelProperty("The table name")
    private String tableName;

    @ApiModelProperty("host")
    private String host;

    @ApiModelProperty("port")
    private Integer port;

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

    public DamengSource() {
        this.setSourceType(SourceType.DAMENG);
    }

    @Override
    public SourceRequest genSourceRequest() {
        return CommonBeanUtils.copyProperties(this, DamengSourceRequest::new);
    }

}
