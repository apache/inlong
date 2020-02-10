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

package com.tencent.tubemq.server.master.web.action.screen;

import com.tencent.tubemq.server.master.TMaster;
import com.tencent.tubemq.server.master.nodemanage.nodebroker.BrokerConfManage;
import com.tencent.tubemq.server.master.web.simplemvc.Action;
import com.tencent.tubemq.server.master.web.simplemvc.RequestContext;
import java.net.InetSocketAddress;


public class Tubeweb implements Action {

    private TMaster master;

    public Tubeweb(TMaster master) {
        this.master = master;
    }

    @Override
    public void execute(RequestContext context) {
        BrokerConfManage brokerConfManage = this.master.getMasterTopicManage();
        InetSocketAddress masterAddr = brokerConfManage.getMasterAddress();
        if (masterAddr == null) {
            context.put("tubemqRemoteAddr", ":");
        } else {
            context.put("tubemqRemoteAddr", masterAddr.getAddress().getHostAddress() + ":"
                    + master.getMasterConfig().getWebPort());
        }
    }
}
