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
import org.apache.inlong.manager.client.api.service.OperationLogApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.operationLog.OperationLogRequest;
import org.apache.inlong.manager.pojo.operationLog.OperationLogResponse;

public class OperationLogClient {

    private final OperationLogApi operationLogApi;

    public OperationLogClient(ClientConfiguration configuration) {
        operationLogApi = ClientUtils.createRetrofit(configuration).create(OperationLogApi.class);
    }

    /**
     * create us task by sink id
     *
     * @return task id
     */
    public PageResult<OperationLogResponse> list(OperationLogRequest request) {
        Response<PageResult<OperationLogResponse>> response =
                ClientUtils.executeHttpCall(operationLogApi.list(request));
        ClientUtils.assertRespSuccess(response);
        if (response.isSuccess()) {
            return response.getData();
        }
        throw new RuntimeException(response.getErrMsg());
    }

}
