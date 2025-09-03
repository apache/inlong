package org.apache.inlong.audit.tool.result;
/**
 * The response info of API
 */
public class Response<T> {

    private boolean success;
    private String errMsg;
    private T data;

    public Response() {
    }

    public Response(boolean success, String errMsg, T data) {
        this.success = success;
        this.errMsg = errMsg;
        this.data = data;
    }

    public static <T> Response<T> success() {
        return new Response<>(true, null, null);
    }

    public static <T> Response<T> success(T data) {
        return new Response<>(true, null, data);
    }

    public static <T> Response<T> fail(String errMsg) {
        return new Response<>(false, errMsg, null);
    }

    public boolean isSuccess() {
        return success;
    }

    public Response<T> setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public Response<T> setErrMsg(String errMsg) {
        this.errMsg = errMsg;
        return this;
    }

    public T getData() {
        return data;
    }

    public Response<T> setData(T data) {
        this.data = data;
        return this;
    }
}