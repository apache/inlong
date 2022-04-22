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

package org.apache.inlong.sort.protocol.transformation.relation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import java.util.List;
import java.util.Map;

@JsonTypeName("innerJoin")
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class InnerJoinNodeRelationShip extends JoinRelationShip {

    private static final long serialVersionUID = -5446480979888656724L;

    @JsonCreator
    public InnerJoinNodeRelationShip(@JsonProperty("inputs") List<String> inputs,
            @JsonProperty("outputs") List<String> outputs,
            @JsonProperty("joinConditionMap") Map<String, List<FilterFunction>> joinConditionMap) {
        super(inputs, outputs, joinConditionMap);
    }

    @Override
    public String format() {
        return "INNER JOIN";
    }
}
