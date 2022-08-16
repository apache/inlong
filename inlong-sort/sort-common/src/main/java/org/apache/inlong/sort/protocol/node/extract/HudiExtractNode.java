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

package org.apache.inlong.sort.protocol.node.extract;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.constant.HudiConstant;
import org.apache.inlong.sort.protocol.constant.HudiConstant.TableType;
import org.apache.inlong.sort.protocol.node.ExtractNode;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@JsonTypeName("hudiExtract")
@Data
public class HudiExtractNode extends ExtractNode implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("path")
    @Nonnull
    private String path;

    @JsonProperty("tableType")
    private HudiConstant.TableType tableType;

    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonCreator
    public HudiExtractNode(@JsonProperty("id") String id,
                           @JsonProperty("name") String name,
                           @JsonProperty("fields") List<FieldInfo> fields,
                           @Nullable @JsonProperty("watermarkField") WatermarkField waterMarkField,
                           @JsonProperty("properties") Map<String, String> properties,
                           @JsonProperty("path") @Nonnull String path,
                           @JsonProperty("catalogType") HudiConstant.TableType tableType,
                           @JsonProperty("primaryKey") String primaryKey) {
        super(id, name, fields, waterMarkField, properties);
        this.path = Preconditions.checkNotNull(path, "path is null");
        this.tableType = tableType == null ? TableType.COPY_ON_WRITE : tableType;
        this.primaryKey = primaryKey;
    }

    @Override
    public Map<String, String> tableOptions() {
        Map<String, String> options = super.tableOptions();
        options.put("connector", "inlong-hudi");
        options.put("path", path);
        options.put("table.type", tableType.name());
        return options;
    }

    @Override
    public String genTableName() {
        return String.format("table_%s", super.getId());
    }

    @Override
    public List<FieldInfo> getPartitionFields() {
        return super.getPartitionFields();
    }
}
