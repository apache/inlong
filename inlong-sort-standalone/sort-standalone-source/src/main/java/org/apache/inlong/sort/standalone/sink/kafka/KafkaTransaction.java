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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.inlong.sort.standalone.channel.ProfileEvent;

import lombok.Builder;
import lombok.Data;
import org.apache.flume.Transaction;

import java.util.Set;

/**
 * KafkaTransaction
 * 
 */
@Data
@Builder
public class KafkaTransaction {

    private Transaction tx;
    private ProfileEvent profileEvent;
    private Set<String> dataFlowIds;

    /**
     * ack
     */
    public synchronized void ack(String dataFlowId) {
        if (dataFlowIds != null) {
            dataFlowIds.remove(dataFlowId);
            if (dataFlowIds.size() > 0) {
                return;
            }
            if (profileEvent != null) {
                profileEvent.ack();
            }
            if (tx != null) {
                tx.commit();
                tx.close();
            }
        }
    }

    /**
     * negativeAck
     */
    public synchronized void negativeAck() {
        if (profileEvent != null) {
            profileEvent.negativeAck();
        }
        if (tx != null) {
            tx.commit();
            tx.close();
        }
        dataFlowIds = null;
        profileEvent = null;
        tx = null;
    }
}
