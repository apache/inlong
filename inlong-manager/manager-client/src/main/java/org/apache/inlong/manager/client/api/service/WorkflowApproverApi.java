package org.apache.inlong.manager.client.api.service;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.workflow.ApproverRequest;
import org.apache.inlong.manager.pojo.workflow.ApproverResponse;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

import java.util.Map;

public interface WorkflowApproverApi {

    @POST("workflow/approver/save")
    Call<Response<Integer>> save(@Body ApproverRequest request);

    @GET("workflow/approver/get/{id}")
    Call<Response<ApproverResponse>> get(@Path("id") Integer id);

    @GET("workflow/approver/list")
    Call<Response<PageInfo<ApproverResponse>>> listByCondition(Map<String, Object> map);

    @POST("workflow/approver/update")
    Call<Response<Integer>> update(@Body ApproverRequest request);

    @POST("workflow/approver/delete/{id}")
    Call<Response<Boolean>> delete(@Path("id") Integer id);


}
