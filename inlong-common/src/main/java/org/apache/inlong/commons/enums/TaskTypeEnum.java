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

package org.apache.inlong.commons.enums;

import static java.util.Objects.requireNonNull;

public enum TaskTypeEnum {
    SQL(1), BINLOG(2), FILE(3), KAFKA(4);

    private int type;

    TaskTypeEnum(int type) {
        this.type = type;
    }

    public static TaskTypeEnum getTaskType(int taskType) {
        requireNonNull(taskType);
        switch (taskType) {
            case 1:
                return SQL;
            case 2:
                return BINLOG;
            case 3:
                return FILE;
            case 4:
                return KAFKA;
            default:
                throw new RuntimeException("such task type doesn't exist");
        }
    }

    public int getType() {
        return type;
    }
}