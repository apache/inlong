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

import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;

@Data
public class InlongGroupContext implements Serializable {

    private String groupId;

    private String groupName;

    private InlongGroupConf groupConf;

    private Map<String, InlongStream> inlongStreamMap;

    //k->taskName v->errorMsg
    private Map<String, String> errMsg;

    private InlongGroupState state;

    public InlongGroupContext(InnerGroupContext groupContext, InlongGroupConf streamGroupConf) {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        AssertUtil.notNull(groupInfo);
        this.groupId = groupInfo.getInlongGroupId();
        this.groupName = groupInfo.getName();
        this.groupConf = streamGroupConf;
        this.inlongStreamMap = groupContext.getStreamMap();
        this.errMsg = Maps.newHashMap();
        this.state = InlongGroupState.parseByBizStatus(groupInfo.getStatus());
    }

    public enum InlongGroupState {
        CREATE, REJECTED, INITIALIZING, OPERATING, STARTED, FAILED, STOPPED, FINISHED, DELETED;

        // Reference to  org.apache.inlong.manager.common.enums.GroupState code
        public static InlongGroupState parseByBizStatus(int bizCode) {

            switch (bizCode) {
                case 0:
                case 100:
                    return CREATE;
                case 41:
                case 141:
                case 151:
                    return OPERATING;
                case 102:
                    return REJECTED;
                case 101:
                case 103:
                case 110:
                    return INITIALIZING;
                case 120:
                    return FAILED;
                case 130:
                case 150:
                    return STARTED;
                case 140:
                    return STOPPED;
                case 131:
                    return FINISHED;
                case 40:
                    return DELETED;
                default:
                    throw new IllegalArgumentException(String.format("Unsupported status %s for group", bizCode));
            }
        }
    }

}
