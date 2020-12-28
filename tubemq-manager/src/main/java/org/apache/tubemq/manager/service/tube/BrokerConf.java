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

package org.apache.tubemq.manager.service.tube;
import lombok.Data;


@Data
public class BrokerConf {

    public String brokerIp;
    public Integer brokerPort;
    public Integer brokerId;
    public String deleteWhen;
    public Integer numPartitions;
    public Integer unflushThreshold;
    public Integer unflushIntegererval;
    public Integer unflushDataHold;
    public boolean acceptPublish;
    public boolean acceptSubscribe;
    public String createUser;
    public Integer brokerTLSPort;
    public Integer numTopicStores;
    public Integer memCacheMsgCntInK;
    public Integer memCacheMsgSizeInMB;
    public Integer memCacheFlushIntegervl;
    public String deletePolicy;

    public BrokerConf(BrokerConf other) {
        this.brokerIp = other.brokerIp;
        this.brokerPort = other.brokerPort;
        this.brokerId = other.brokerId;
        this.deleteWhen = other.deleteWhen;
        this.numPartitions = other.numPartitions;
        this.unflushThreshold = other.unflushThreshold;
        this.unflushIntegererval = other.unflushIntegererval;
        this.unflushDataHold = other.unflushDataHold;
        this.acceptPublish = other.acceptPublish;
        this.acceptSubscribe = other.acceptSubscribe;
        this.createUser = other.createUser;
        this.brokerTLSPort = other.brokerTLSPort;
        this.numTopicStores = other.numTopicStores;
        this.memCacheMsgCntInK = other.memCacheMsgCntInK;
        this.memCacheMsgSizeInMB = other.memCacheMsgSizeInMB;
        this.memCacheFlushIntegervl = other.memCacheFlushIntegervl;
        this.deletePolicy = other.deletePolicy;
    }


}


