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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;

@Data
public class DataStreamGroupInfo implements Serializable {

    private String groupId;

    private String groupName;

    private DataStreamGroupConf groupConf;

    private Map<String, DataStream> dataStreamMap;

    private List<String> errMsg;

    private GroupState state;

    public enum GroupState {
        INIT, FAIL, START, SUSPEND, RESTART, DELETE;

        //Reference to  org.apache.inlong.manager.common.enums.EntityStatus code
        public static GroupState parseByBizStatus(int bizCode) {

            switch (bizCode) {
                case 100:
                case 101:
                case 103:
                case 110:
                    return INIT;
                case 102:
                case 120:
                    return FAIL;
                case 130:
                    return START;
                case 150:
                    return RESTART;
                case 140:
                    return SUSPEND;
                case 40:
                    return DELETE;
                default:
                    throw new IllegalArgumentException(String.format("Unsupport status:%s for business", bizCode));
            }
        }
    }

    public DataStreamGroupInfo(InnerGroupContext groupContext, DataStreamGroupConf groupConf) {
        BusinessInfo businessInfo = groupContext.getBusinessInfo();
        AssertUtil.notNull(businessInfo);
        this.groupId = businessInfo.getInlongGroupId();
        this.groupName = businessInfo.getName();
        this.groupConf = groupConf;
        this.dataStreamMap = groupContext.getStreamMap();
        this.errMsg = Lists.newArrayList();
        this.state = GroupState.parseByBizStatus(businessInfo.getStatus());
    }

}
