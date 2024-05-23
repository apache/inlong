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

package org.apache.inlong.manager.client.api.inner.client;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.InLongScheduleApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;

public class InLongScheduleClient {

    private InLongScheduleApi scheduleApi;

    public InLongScheduleClient(ClientConfiguration clientConfiguration) {
        scheduleApi = ClientUtils.createRetrofit(clientConfiguration).create(InLongScheduleApi.class);
    }

    public Integer createScheduleInfo(ScheduleInfoRequest request) {
        Response<Integer> response = ClientUtils.executeHttpCall(scheduleApi.createSchedule(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public Boolean scheduleInfoExist(String groupId) {
        Preconditions.expectNotBlank(groupId, ErrorCodeEnum.GROUP_ID_IS_EMPTY);
        Response<Boolean> response = ClientUtils.executeHttpCall(scheduleApi.exist(groupId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public Boolean updateScheduleInfo(ScheduleInfoRequest request) {
        Response<Boolean> response = ClientUtils.executeHttpCall(scheduleApi.update(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public ScheduleInfo getScheduleInfo(String groupId) {
        Response<ScheduleInfo> response = ClientUtils.executeHttpCall(scheduleApi.get(groupId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public Boolean deleteScheduleInfo(String groupId) {
        Response<Boolean> response = ClientUtils.executeHttpCall(scheduleApi.delete(groupId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }
}
