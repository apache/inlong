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

package org.apache.tubemq.server.master.metamanage.online;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;




public class BrokerRunData {

    private final ConcurrentHashMap<Integer, String> brokersMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> brokersTLSMap =
            new ConcurrentHashMap<>();
    private AtomicLong brokerInfoChkId = new AtomicLong(System.currentTimeMillis());
    private long lastBrokerUpdatedTime = System.currentTimeMillis();

    public BrokerRunData() {

    }

    public Tuple2<Long, ConcurrentHashMap<Integer, String>> getBrokerCurRunData(
            boolean isOverTLS) {
        if (isOverTLS) {
            return new Tuple2<>(brokerInfoChkId.get(), brokersTLSMap);
        } else {
            return new Tuple2<>(brokerInfoChkId.get(), brokersMap);
        }
    }

    public void updateBrokerRunData(List<BrokerConfEntity> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        for (BrokerConfEntity entity : entities) {
            updBrokerRunData(entity);
        }
    }

    public void updBrokerRunData(BrokerConfEntity entity) {
        if (entity == null) {
            return;
        }
        String brokerReg =
                this.brokersMap.putIfAbsent(entity.getBrokerId(),
                        entity.getSimpleBrokerInfo());
        String brokerTLSReg =
                this.brokersTLSMap.putIfAbsent(entity.getBrokerId(),
                        entity.getSimpleTLSBrokerInfo());
        if (brokerReg == null
                || brokerTLSReg == null
                || !brokerReg.equals(entity.getSimpleBrokerInfo())
                || !brokerTLSReg.equals(entity.getSimpleTLSBrokerInfo())) {
            if (brokerReg != null
                    && !brokerReg.equals(entity.getSimpleBrokerInfo())) {
                this.brokersMap.put(entity.getBrokerId(), entity.getSimpleBrokerInfo());
            }
            if (brokerTLSReg != null
                    && !brokerTLSReg.equals(entity.getSimpleTLSBrokerInfo())) {
                this.brokersTLSMap.put(entity.getBrokerId(), entity.getSimpleTLSBrokerInfo());
            }
            this.lastBrokerUpdatedTime = System.currentTimeMillis();
            this.brokerInfoChkId.set(this.lastBrokerUpdatedTime);
        }
    }

    public void delBrokerRunData(int brokerId) {
        if (brokerId == TBaseConstants.META_VALUE_UNDEFINED) {
            return;
        }
        String brokerReg = this.brokersMap.remove(brokerId);
        String brokerTLSReg = this.brokersTLSMap.remove(brokerId);
        if (brokerReg != null || brokerTLSReg != null) {
            this.lastBrokerUpdatedTime = System.currentTimeMillis();
            this.brokerInfoChkId.set(this.lastBrokerUpdatedTime);
        }
    }
}
