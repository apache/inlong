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

package org.apache.inlong.manager.client.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.enums.SourceState;
import org.apache.inlong.manager.common.enums.SourceType;

@Data
@ApiModel("Stream source configuration")
public abstract class StreamSource {

    public enum State {
        INIT, NORMAL, FROZING, FROZEN, FAILED, DELETING, DELETE;

        public static State parseByStatus(int status) {
            SourceState sourceState = SourceState.forCode(status);
            switch (sourceState) {
                case SOURCE_NEW:
                case TO_BE_ISSUED_ADD:
                case BEEN_ISSUED_ADD:
                case TO_BE_ISSUED_ACTIVE:
                case BEEN_ISSUED_ACTIVE:
                    return INIT;
                case SOURCE_NORMAL:
                    return NORMAL;
                case TO_BE_ISSUED_FROZEN:
                case BEEN_ISSUED_FROZEN:
                    return FROZING;
                case SOURCE_FROZEN:
                    return FROZEN;
                case SOURCE_FAILED:
                    return FAILED;
                case TO_BE_ISSUED_DELETE:
                case BEEN_ISSUED_DELETE:
                    return DELETING;
                case SOURCE_DISABLE:
                    return DELETE;
                default:
                    throw new IllegalStateException(
                            String.format("Unsupported source state=%s for Inlong", sourceState));
            }
        }
    }

    public enum SyncType {
        FULL, INCREMENT
    }

    @ApiModelProperty(value = "DataSource name", required = true)
    private String sourceName;

    @ApiModelProperty("Ip of the agent running the task")
    private String agentIp;

    @ApiModelProperty("Mac uuid of the agent running the task")
    private String uuid = "";

    @ApiModelProperty("State of stream source")
    private State state;

    public abstract SourceType getSourceType();

    public abstract SyncType getSyncType();

    public abstract DataFormat getDataFormat();
}
