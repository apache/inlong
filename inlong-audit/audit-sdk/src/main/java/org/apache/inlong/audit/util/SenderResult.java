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

package org.apache.inlong.audit.util;

import java.net.InetSocketAddress;

public class SenderResult {

    public final InetSocketAddress addr;
    public boolean result;

    /**
     * Constructor
     *
     * @param addr
     * @param result
     */
    public SenderResult(InetSocketAddress addr, boolean result) {
        this.addr = addr;
        this.result = result;
    }

    /**
     * Constructor
     *
     * @param sendIp
     * @param sendPort
     * @param result
     */
    public SenderResult(String sendIp, int sendPort, boolean result) {
        this.addr = new InetSocketAddress(sendIp, sendPort);
        this.result = result;
    }
}
