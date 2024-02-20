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
 * Enum of task state.
 */
public enum TaskStateEnum {

    NEW(0),
    RUNNING(1),
    FROZEN(2),
    RETRY_FINISH(3);

    private final int state;

    TaskStateEnum(int state) {
        this.state = state;
    }

    public static TaskStateEnum getTaskState(int state) {
        switch (state) {
            case 0:
                return NEW;
            case 1:
                return RUNNING;
            case 2:
                return FROZEN;
            case 3:
                return RETRY_FINISH;
            default:
                throw new RuntimeException("Unsupported task state " + state);
        }
    }

    public int getType() {
        return state;
    }

}
