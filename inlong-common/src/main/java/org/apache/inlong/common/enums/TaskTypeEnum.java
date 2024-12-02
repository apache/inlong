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

import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Enum of task type.
 */
public enum TaskTypeEnum {

    DATABASE_MIGRATION(0),
    SQL(1),
    BINLOG(2),
    FILE(3),
    KAFKA(4),
    PULSAR(5),
    POSTGRES(6),
    ORACLE(7),
    SQLSERVER(8),
    MONGODB(9),
    TUBEMQ(10),
    REDIS(11),
    MQTT(12),
    HUDI(13),
    COS(14),

    HA_BINLOG(101),
    // only used for unit test
    MOCK(201);

    private static final Map<Integer, TaskTypeEnum> TASK_TYPE_ENUM_MAP = Maps.newHashMap();

    /*
     * Init tasktype
     */
    static {
        TASK_TYPE_ENUM_MAP.putAll(
                Arrays.stream(TaskTypeEnum.values()).collect(Collectors.toMap(TaskTypeEnum::getType, type -> type)));

    }

    private final int type;

    TaskTypeEnum(int type) {
        this.type = type;
    }

    public static TaskTypeEnum getTaskType(int taskType) {
        TaskTypeEnum taskTypeEnum = TASK_TYPE_ENUM_MAP.get(taskType);
        if (Objects.isNull(taskTypeEnum)) {
            throw new NoSuchElementException(String.format("Unsupported task type:[%s]", taskType));
        }
        return taskTypeEnum;
    }

    public int getType() {
        return type;
    }

}
