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

package org.apache.inlong.common.enums;

/**
 * Enum of inlong component type.
 */
public enum MessageWrapType {

    NONE(0, "NONE"),
    INLONG_MSG(1, "INLONG_MSG"),
    PB(2, "PB");

    private final Integer typeId;

    private final String type;

    MessageWrapType(Integer typeId, String type) {
        this.typeId = typeId;
        this.type = type;
    }

    public static MessageWrapType forType(Integer typeId) {
        for (MessageWrapType componentType : values()) {
            if (componentType.getTypeId().equals(typeId)) {
                return componentType;
            }
        }
        throw new IllegalArgumentException("Unsupported component type for " + typeId);
    }

    public String getType() {
        return type;
    }

    public Integer getTypeId() {
        return typeId;
    }

}
