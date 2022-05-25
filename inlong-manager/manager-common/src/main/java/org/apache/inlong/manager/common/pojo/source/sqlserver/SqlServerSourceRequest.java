package org.apache.inlong.manager.common.pojo.source.sqlserver;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.util.JsonTypeDefine;


/**
 * Request info of the Sqlserver source info
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Request of the binlog source info")
@JsonTypeDefine(value = SourceType.SOURCE_SQLSERVER)
public class SqlServerSourceRequest extends SourceRequest {

    @ApiModelProperty("Username of the Sqlserver")
    private String username;

    @ApiModelProperty("Password of the Sqlserver")
    private String password;

    @ApiModelProperty("Hostname of the Sqlserver")
    private String hostname;

    @ApiModelProperty("Exposed port of the Sqlserver")
    private int port;

    @ApiModelProperty("database of the Sqlserver")
    private String database;

    @ApiModelProperty("schemaName of the Sqlserver")
    private String schemaName;

    @ApiModelProperty("tableName of the Sqlserver")
    private String tableName;

    @ApiModelProperty("Database time zone, Default is UTC")
    private String serverTimezone;

    @ApiModelProperty("Whether to migrate all databases")
    private boolean allMigration;

    @ApiModelProperty(value = "Primary key must be shared by all tables")
    private String primaryKey;

    public SqlServerSourceRequest() {
        this.setSourceType(SourceType.SQLSERVER.toString());
    }

}
