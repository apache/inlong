package org.apache.inlong.manager.client.api.service;

import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tenant.InlongTenantInfo;
import org.apache.inlong.manager.pojo.tenant.InlongTenantPageRequest;
import org.apache.inlong.manager.pojo.tenant.InlongTenantRequest;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;
import retrofit2.http.Path;

public interface InlongTenantApi {


    @POST("tenant/save")
    Call<Response<Integer>> createInLongTenant(@Body InlongTenantRequest request);

    @POST("tenant/list")
    Call<Response<PageResult<InlongTenantInfo>>> listByCondition(@Body InlongTenantPageRequest request);


    @POST("tenant/update")
    Call<Response<Boolean>> update(@Body InlongTenantRequest request);

    @POST("tenant/get/{name}")
    Call<Response<InlongTenantInfo>> get(@Path("name") String name);

}
