package org.apache.inlong.manager.common.pojo.sink.doris;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.util.JsonTypeDefine;

/**
 * Doris sink request.
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Doris sink request")
@JsonTypeDefine(value = SinkType.SINK_DORIS)
public class DorisSinkRequest extends SinkRequest {
    @ApiModelProperty("Doris JDBC URL, such as jdbc:mysql://host:port/database")
    private String jdbcUrl;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("User password")
    private String password;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Primary key")
    private String primaryKey;

}
