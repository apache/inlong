package org.apache.inlong.manager.client.api.service;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tenant.InlongTenantRequest;
import org.apache.inlong.manager.pojo.user.TenantRoleInfo;
import org.apache.inlong.manager.pojo.user.TenantRolePageRequest;
import org.apache.inlong.manager.pojo.user.TenantRoleRequest;

import org.springframework.web.bind.annotation.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

public interface InlongTenantRoleApi {

    @GET("/role/tenant/get/{id}")
    Call<Response<TenantRoleInfo>> get(@Path("id") int id);

    @POST("/role/tenant/save")
    Call<Response<Integer>> save(@Body TenantRoleRequest request);

    @POST("/role/tenant/update")
    Call<Response<Boolean>> update(@Body TenantRoleRequest request);

    @POST("/role/tenant/list")
    Call<Response<PageInfo<TenantRoleInfo>>> listByCondition(@RequestBody TenantRolePageRequest request);
}
