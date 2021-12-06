/**
 * Tencent is pleased to support the open source community by making Tars available.
 * <p>
 * Copyright (C) 2015,2016 THL A29 Limited, a Tencent company. All rights reserved.
 * <p>
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * https://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.inlong.audit.util;

public class SenderResult {

    public final IpPort ipPort;
    public boolean result;

    /**
     * Constructor
     *
     * @param ipPort
     * @param result
     */
    public SenderResult(IpPort ipPort, boolean result) {
        this.ipPort = ipPort;
        this.result = result;
    }

    /**
     *
     * Constructor
     *
     * @param sendIp
     * @param sendPort
     * @param result
     */
    public SenderResult(String sendIp, int sendPort, boolean result) {
        this.ipPort = new IpPort(sendIp, sendPort);
        this.result = result;
    }
}
