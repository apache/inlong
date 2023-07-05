package org.apache.inlong.manager.client.api.inner.client;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.InlongTenantApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tenant.InlongTenantInfo;
import org.apache.inlong.manager.pojo.tenant.InlongTenantPageRequest;
import org.apache.inlong.manager.pojo.tenant.InlongTenantRequest;
import org.checkerframework.checker.units.qual.C;

/**
 * Client for {@link InlongTenantApi}.
 */
public class InlongTenantClient {

    private final InlongTenantApi inlongTenantApi;

    public InlongTenantClient(ClientConfiguration configuration) {
        inlongTenantApi = ClientUtils.createRetrofit(configuration).create(InlongTenantApi.class);
    }


    /**
     * get inlong tenant by tenant name
     *
     * @param name tenant name
     * @return {@link InlongTenantInfo}
     */
    public InlongTenantInfo getTenantByName(String name) {
        Response<InlongTenantInfo> inlongTenantInfoResponse = ClientUtils.executeHttpCall(inlongTenantApi.get(name));
        ClientUtils.assertRespSuccess(inlongTenantInfoResponse);
        return inlongTenantInfoResponse.getData();
    }


    /**
     * create inlong tenant
     *
     * @param inlongTenantRequest tenant info
     * @return inlong tenant id
     */
    public Integer save(InlongTenantRequest inlongTenantRequest) {
        Response<Integer> saveInlongTenantResult = ClientUtils.executeHttpCall(
                inlongTenantApi.createInLongTenant(inlongTenantRequest));
        ClientUtils.assertRespSuccess(saveInlongTenantResult);
        return saveInlongTenantResult.getData();
    }


    /**
     * Paging query stream sink info based on conditions.
     *
     * @param inlongTenantPageRequest paging request
     * @return tenant page list
     */
    public PageResult<InlongTenantInfo> listByCondition(InlongTenantPageRequest inlongTenantPageRequest) {
        Response<PageResult<InlongTenantInfo>> pageResultResponse = ClientUtils.executeHttpCall(
                inlongTenantApi.listByCondition(inlongTenantPageRequest));
        ClientUtils.assertRespSuccess(pageResultResponse);
        return pageResultResponse.getData();
    }

    /**
     * Update one tenant
     *
     * @param request tenant request that needs to be modified
     * @return whether succeed
     */
    public Boolean update(InlongTenantRequest request) {
        Response<Boolean> updateResult = ClientUtils.executeHttpCall(inlongTenantApi.update(request));
        ClientUtils.assertRespSuccess(updateResult);
        return updateResult.getData();
    }
}
