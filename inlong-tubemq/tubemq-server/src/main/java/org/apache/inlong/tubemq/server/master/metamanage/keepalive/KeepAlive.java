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

package org.apache.inlong.tubemq.server.master.metamanage.keepalive;

import java.net.InetSocketAddress;
import org.apache.inlong.tubemq.server.master.bdbstore.MasterGroupStatus;
import org.apache.inlong.tubemq.server.master.web.model.ClusterGroupVO;

public interface KeepAlive {

    /**
     * Whether this node is the master role
     *
     * @return true if is master role or else
     */
    boolean isMasterNow();

    long getMasterSinceTime();

    InetSocketAddress getMasterAddress();

    /**
     * Whether the primary node in active
     *
     * @return  true for active, false for inactive
     */
    boolean isPrimaryNodeActive();

    void transferMaster() throws Exception;

    /**
     * Register node role switching event observer
     *
     * @param eventObserver  the event observer
     */
    void registerObserver(AliveObserver eventObserver);

    ClusterGroupVO getGroupAddressStrInfo();

    MasterGroupStatus getMasterGroupStatus(boolean isFromHeartbeat);
}
