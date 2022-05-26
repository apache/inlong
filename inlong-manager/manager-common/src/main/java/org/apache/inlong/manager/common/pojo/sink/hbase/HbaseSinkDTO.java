/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.common.pojo.sink.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Hbase sink info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HbaseSinkDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Namespace")
    private String namespace;

    @ApiModelProperty("Row key")
    private String rowKey;

    @ApiModelProperty("Zookeeper quorm")
    private String zookeeperQuorum;

    @ApiModelProperty("Sink buffer flush maxsize")
    private String sinkBufferFlushMaxSize;

    @ApiModelProperty("Zookeeper znode parent")
    private String zookeeperZnodeParent;

    @ApiModelProperty("Sink buffer flush max rows")
    private String sinkBufferFlushMaxRows;

    @ApiModelProperty("Sink buffer flush interval")
    private String sinkBufferFlushInterval;

    @ApiModelProperty("Properties for hbase")
    private Map<String, Object> properties;

    /**
     * Get the dto instance from the request
     */
    public static HbaseSinkDTO getFromRequest(HbaseSinkRequest request) {
        return HbaseSinkDTO.builder()
                .tableName(request.getTableName())
                .namespace(request.getNamespace())
                .rowKey(request.getRowKey())
                .zookeeperQuorum(request.getZookeeperQuorum())
                .sinkBufferFlushMaxSize(request.getSinkBufferFlushMaxSize())
                .zookeeperZnodeParent(request.getZookeeperZnodeParent())
                .sinkBufferFlushMaxRows(request.getSinkBufferFlushMaxRows())
                .sinkBufferFlushInterval(request.getSinkBufferFlushInterval())
                .properties(request.getProperties())
                .build();
    }

    /**
     * Get hbase sink info from JSON string
     */
    public static HbaseSinkDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, HbaseSinkDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.SINK_INFO_INCORRECT.getMessage());
        }
    }

    /**
     * Get hbase table info
     */
    public static HbaseTableInfo getHbaseTableInfo(HbaseSinkDTO hbaseInfo, List<HbaseColumnFamilyInfo> columnFamilies) {
        HbaseTableInfo info = new HbaseTableInfo();
        info.setNamespace(hbaseInfo.getNamespace());
        info.setTableName(hbaseInfo.getTableName());
        info.setTblProperties(hbaseInfo.getProperties());
        info.setColumnFamilies(columnFamilies);
        return info;
    }

}
