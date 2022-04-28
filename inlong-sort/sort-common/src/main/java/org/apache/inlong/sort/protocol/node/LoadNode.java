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

package org.apache.inlong.sort.protocol.node;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.node.load.HiveLoadNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KafkaLoadNode.class, name = "kafkaLoad"),
        @JsonSubTypes.Type(value = HiveLoadNode.class, name = "hiveLoad")
})
@NoArgsConstructor
@Data
public abstract class LoadNode implements Node {

    @JsonProperty("id")
    private String id;
    @JsonInclude(Include.NON_NULL)
    @JsonProperty("name")
    private String name;
    @JsonProperty("fields")
    private List<FieldInfo> fields;
    @JsonProperty("fieldRelationShips")
    private List<FieldRelationShip> fieldRelationShips;
    @Nullable
    @JsonInclude(Include.NON_NULL)
    @JsonProperty("sinkParallelism")
    private Integer sinkParallelism;
    @JsonProperty("filters")
    @JsonInclude(Include.NON_NULL)
    private List<FilterFunction> filters = new ArrayList<>();
    @Nullable
    @JsonInclude(Include.NON_NULL)
    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonCreator
    public LoadNode(@JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<FieldInfo> fields,
            @JsonProperty("fieldRelationShips") List<FieldRelationShip> fieldRelationShips,
            @JsonProperty("filters") List<FilterFunction> filters,
            @Nullable @JsonProperty("sinkParallelism") Integer sinkParallelism,
            @JsonProperty("properties") Map<String, String> properties) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = name;
        this.fields = Preconditions.checkNotNull(fields, "fields is null");
        Preconditions.checkState(!fields.isEmpty(), "fields is empty");
        this.fieldRelationShips = Preconditions.checkNotNull(fieldRelationShips,
                "fieldRelationShips is null");
        Preconditions.checkState(!fieldRelationShips.isEmpty(), "fieldRelationShips is empty");
        this.filters = filters;
        this.sinkParallelism = sinkParallelism;
        this.properties = properties;
    }
}
