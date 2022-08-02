package org.apache.inlong.manager.common.pojo.sink.doris;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.bouncycastle.pqc.crypto.newhope.NHOtherInfoGenerator;

/**
 * Doris sink info.
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Doris sink info")
@JsonTypeDefine(value = SinkType.SINK_DORIS)
public class DorisSink extends StreamSink {

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

    public DorisSink(){this.setSinkType(SinkType.SINK_DORIS);}

    @Override
    public SinkRequest genSinkRequest(){return CommonBeanUtils.copyProperties(this,DorisSinkRequest::new);}
}
