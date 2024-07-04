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

package org.apache.inlong.common.bounded;

import lombok.Getter;

/**
 * Source boundary types.
 * TIME is the common boundary type should be supported in every bounded source.
 * OFFSET is the boundary for MQ type bounded source, like offset in kafka or messageId in pulsar.
 * */
@Getter
public enum BoundaryType {

    TIME("time"),
    OFFSET("offset");

    private final String type;

    BoundaryType(String boundaryType) {
        this.type = boundaryType;
    }

    public static BoundaryType getInstance(String boundaryType) {
        for (BoundaryType type : values()) {
            if (type.getType().equalsIgnoreCase(boundaryType)) {
                return type;
            }
        }
        return null;
    }

    public static boolean isSupportBoundaryType(String boundaryType) {
        for (BoundaryType source : values()) {
            if (source.getType().equalsIgnoreCase(boundaryType)) {
                return true;
            }
        }
        return false;
    }
}
