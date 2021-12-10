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

package org.apache.inlong.sort.entity;

import java.io.Serializable;

public class ManagerResponse implements Serializable {

    private boolean result;
    private int errCode;
    private String md5;
    private CacheZoneConfig data;

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public int getErrCode() {
        return errCode;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public CacheZoneConfig getData() {
        return data;
    }

    public void setData(CacheZoneConfig data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Response{"
                + "result=" + result
                + ", errCode=" + errCode
                + ", md5='" + md5 + '\''
                + ", data=" + data
                + '}';
    }
}
