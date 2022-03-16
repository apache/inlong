/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.config.pojo;

/**
 * 
 * SortClusterResponse
 */
public class SortClusterResponse {

    public static final int SUCC = 0;
    public static final int NOUPDATE = 1;
    public static final int FAIL = -1;
    public static final int REQ_PARAMS_ERROR = -101;

    private String msg;
    private int code;
    private String md5;
    private SortClusterConfig data;

    /**
     * get msg
     *
     * @return Response message.
     */
    public String getMsg() {
        return this.msg;
    }

    /**
     * set msg
     * 
     * @param msg The msg of response
     */
    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * get errCode
     * 
     * @return the errCode
     */
    public int getErrCode() {
        return code;
    }

    /**
     * set errCode
     * 
     * @param code the errCode to set
     */
    public void setErrCode(int code) {
        this.code = code;
    }

    /**
     * get md5
     * 
     * @return the md5
     */
    public String getMd5() {
        return md5;
    }

    /**
     * set md5
     * 
     * @param md5 the md5 to set
     */
    public void setMd5(String md5) {
        this.md5 = md5;
    }

    /**
     * get data
     * 
     * @return the data
     */
    public SortClusterConfig getData() {
        return data;
    }

    /**
     * set data
     * 
     * @param data the data to set
     */
    public void setData(SortClusterConfig data) {
        this.data = data;
    }
}
