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

package org.apache.inlong.manager.common.pojo.transform.joiner;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.stream.StreamNode;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;

import java.util.Map;

/**
 * A class to define operations to join two streamNode in one with relation defined.
 */
@Data
@Builder
public class JoinerDefinition extends TransformDefinition {

    public JoinerDefinition(StreamNode leftNode,
            StreamNode rightNode,
            Map<StreamField, StreamField> joinKeyFields,
            JoinMode joinMode) {
        this.transformType = TransformType.JOINER;
        this.leftNode = leftNode;
        this.rightNode = rightNode;
        this.joinKeyFields = joinKeyFields;
        this.joinMode = joinMode;
    }

    /**
     * Left node for join
     */
    private StreamNode leftNode;

    /**
     * Right node for join
     */
    private StreamNode rightNode;

    /**
     * Key is streamField from left node, value is streamField from right node;
     */
    private Map<StreamField, StreamField> joinKeyFields;

    @JsonFormat
    public enum JoinMode {
        LEFT_JOIN, RIGHT_JOIN, INNER_JOIN
    }

    /**
     * Join mode for join transform
     */
    private JoinMode joinMode;
}
