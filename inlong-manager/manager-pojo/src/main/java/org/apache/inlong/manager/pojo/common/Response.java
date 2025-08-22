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

package org.apache.inlong.manager.pojo.common;

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
