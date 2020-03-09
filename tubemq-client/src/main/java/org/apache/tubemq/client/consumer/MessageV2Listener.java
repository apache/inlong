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

package org.apache.tubemq.client.consumer;

import java.util.List;
import org.apache.tubemq.client.common.PeerInfo;
import org.apache.tubemq.corebase.Message;


public interface MessageV2Listener extends  MessageListener {
    /*
        NOTICE: The addition of the MessageV2Listener modification is mainly to solve the problem
                of compatibility between previous and subsequent versions of the interface.If you think that
                this modification is too redundant when you use it, you can manually modify it and
                return the PeerInfo in the MessageListener's receiveMessages interface
                and remove the MessageV2Listener class.
     */
    void receiveMessages(PeerInfo peerInfo, final List<Message> messages) throws InterruptedException;
}
