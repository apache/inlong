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

package org.apache.inlong.sdk.dataproxy.network;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.config.HostInfo;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Client Manager interface
 *
 * Used to Manager network client
 */
public interface ClientMgr {

    /**
     * Start network client manager
     *
     * @param procResult the start result, return detail error infos if sending fails
     * @return  true if successful, false return indicates failure.
     */
    boolean start(ProcessResult procResult);

    /**
     * Stop network client manager
     *
     */
    void stop();

    /**
     * Get the number of proxy nodes currently in use
     *
     * @return  Number of nodes in use
     */
    int getActiveNodeCnt();

    /**
     * Get the number of in-flight messages
     *
     * @return  Number of in-flight messages
     */
    int getInflightMsgCnt();

    /**
     * Update cached proxy nodes
     *
     * @param nodeChanged whether the updated node has changed
     * @param hostInfoMap  the new proxy nodes
     */
    void updateProxyInfoList(boolean nodeChanged, ConcurrentHashMap<String, HostInfo> hostInfoMap);
}
