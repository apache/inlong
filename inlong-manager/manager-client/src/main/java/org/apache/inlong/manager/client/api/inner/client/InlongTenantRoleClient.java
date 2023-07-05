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
import org.apache.inlong.manager.client.api.service.InlongTenantRoleApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.user.TenantRoleInfo;
import org.apache.inlong.manager.pojo.user.TenantRolePageRequest;
import org.apache.inlong.manager.pojo.user.TenantRoleRequest;

import com.github.pagehelper.PageInfo;

public class InlongTenantRoleClient {

    private final InlongTenantRoleApi inlongTenantRoleApi;

    public InlongTenantRoleClient(ClientConfiguration configuration) {
        this.inlongTenantRoleApi = ClientUtils.createRetrofit(configuration).create(InlongTenantRoleApi.class);
    }

    /**
     * List all tenant role by paginating
     *
     *@param request tenant page info
     *@return {@link PageInfo<TenantRoleInfo>}
     */
    public PageInfo<TenantRoleInfo> listByCondition(TenantRolePageRequest request) {
        Response<PageInfo<TenantRoleInfo>> pageInfoResponse = ClientUtils.executeHttpCall(
                inlongTenantRoleApi.listByCondition(request));
        ClientUtils.assertRespSuccess(pageInfoResponse);
        return pageInfoResponse.getData();
    }

    /**
     * Save one tenant role
     *
     * @param record tenant role info
     *
     * @return tenant id
     */
    public int save(TenantRoleRequest record) {
        Response<Integer> saveResult = ClientUtils.executeHttpCall(inlongTenantRoleApi.save(record));
        ClientUtils.assertRespSuccess(saveResult);
        return saveResult.getData();
    }

    /**
     * Update one tenant role
     *
     * @param  record tenant role info
     * @return true/false
     */
    public boolean update(TenantRoleRequest record) {
        Response<Boolean> updateResult = ClientUtils.executeHttpCall(inlongTenantRoleApi.update(record));
        ClientUtils.assertRespSuccess(updateResult);
        return updateResult.getData();
    }

    /**
     * Get one tenant role by id
     */
    public TenantRoleInfo get(int id) {
        Response<TenantRoleInfo> tenantRoleInfoResponse = ClientUtils.executeHttpCall(inlongTenantRoleApi.get(id));
        ClientUtils.assertRespSuccess(tenantRoleInfoResponse);
        return tenantRoleInfoResponse.getData();
    }

}
