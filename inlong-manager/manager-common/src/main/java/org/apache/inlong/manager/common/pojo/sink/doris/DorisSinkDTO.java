package org.apache.inlong.manager.common.pojo.sink.doris;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Doris sink info.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DorisSinkDTO {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(DorisSinkDTO.class);

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

    @ApiModelProperty("Properties for Doris")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static DorisSinkDTO getFromRequest(DorisSinkRequest request){
        return DorisSinkDTO.builder()
                .jdbcUrl(request.getJdbcUrl())
                .tableName(request.getTableName())
                .username(request.getUsername())
                .password(request.getPassword())
                .primaryKey(request.getPrimaryKey())
                .properties(request.getProperties())
                .build();
    }

    /**
     * Get Doris sink info from JSON string
     */
    public static DorisSinkDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, DorisSinkDTO.class);
        } catch (Exception e) {
            LOGGER.error("fetch doris sink info failed from json params: " + extParams, e);
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage() + ": " + e.getMessage());
        }
    }

    /**
     * Get Doris table info
     */
    public static DorisTableInfo getTableInfo(DorisSinkDTO dorisSink, List<DorisColumnInfo> columnList){
        DorisTableInfo tableInfo = new DorisTableInfo();
        tableInfo.setTableName(dorisSink.getTableName());
        tableInfo.setPrimaryKey(dorisSink.getPrimaryKey());
        tableInfo.setUserName(dorisSink.getUsername());
        tableInfo.setColumns(columnList);
        return tableInfo;
    }





}
