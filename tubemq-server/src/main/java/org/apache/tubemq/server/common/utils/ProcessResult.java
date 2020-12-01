/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.common.utils;

import org.apache.tubemq.corebase.TErrCodeConstants;

public class ProcessResult {
    public boolean success = true;
    public int errCode = TErrCodeConstants.SUCCESS;
    public String errInfo = "";
    public Object retData1 = null;

    public ProcessResult() {

    }

    public ProcessResult(Object retData) {
        this.success = true;
        this.retData1 = retData;
    }

    public ProcessResult(int errCode, String errInfo) {
        this.success = false;
        this.errCode = errCode;
        this.errInfo = errInfo;
    }

    public void setFailResult(int errCode, final String errMsg) {
        this.success = false;
        this.errCode = errCode;
        this.errInfo = errMsg;
    }

    public void setSuccResult(Object retData) {
        this.success = true;
        this.retData1 = retData;
    }
}
