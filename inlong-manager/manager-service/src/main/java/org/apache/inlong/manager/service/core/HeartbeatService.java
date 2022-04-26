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

package org.apache.inlong.manager.service.core;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.heartbeat.ComponentHeartbeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.GroupHeartbeatResponse;
import org.apache.inlong.manager.common.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.common.pojo.heartbeat.StreamHeartbeatResponse;

/**
 * Heartbeat Service
 */
public interface HeartbeatService {

    /**
     * report Heartbeat
     * @param request request
     * @return
     */
    String reportHeartbeatInfo(HeartbeatReportRequest request);

    /**
     * get Component HeartbeatInfo
     * @param component component
     * @param instance instance
     * @return ComponentHeartBeatResponse
     */
    ComponentHeartbeatResponse getComponentHeartbeatInfo(String component,
            String instance);

    /**
     * get Group HeartbeatInfo
     * @param component component
     * @param instance instance
     * @param inlongGroupId inlongGroupId
     * @return  GroupHeartbeatResponse
     */
    GroupHeartbeatResponse getGroupHeartbeatInfo(String component,
            String instance, String inlongGroupId);

    /**
     * get Stream HeartbeatInfo
     * @param component component
     * @param instance instance
     * @param inlongGroupId inlongGroupId
     * @param inlongStreamId inlongStreamId
     * @return StreamHeartBeatResponse
     */
    StreamHeartbeatResponse getStreamHeartbeatInfo(String component,
            String instance, String inlongGroupId, String inlongStreamId);

    /**
     * get Component HeartbeatInfos by page
     * @param component  component
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return one page of ComponentHeartBeatResponse
     */
    PageInfo<ComponentHeartbeatResponse> getComponentHeartbeatInfos(String component, int pageNum,
            int pageSize);

    /**
     * get Group HeartbeatInfos by page
     * @param component component
     * @param instance instance
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return one page of GroupHeartbeatResponse
     */
    PageInfo<GroupHeartbeatResponse> getGroupHeartbeatInfos(String component,
            String instance, int pageNum, int pageSize);

    /**
     * get Stream HeartbeatInfos
     * @param component component
     * @param instance instance
     * @param inlongGroupId inlongGroupId
     * @param pageNum pageNum
     * @param pageSize pageSize
     * @return one page of GroupHeartbeatResponse
     */
    PageInfo<StreamHeartbeatResponse> getStreamHeartbeatInfos(String component,
            String instance, String inlongGroupId, int pageNum, int pageSize);

}
