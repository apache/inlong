/*
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

public class TubeHttpBrokerCfgInfo {
    public boolean acceptPublish;
    public boolean acceptSubscribe;
    public Integer brokerId;
    public String brokerIp;
    public Integer brokerPort;
    public Integer brokerTLSPort;
    public String createDate;
    public String createUser;
    public String deletePolicy;
    public String deleteWhen;
    public String modifyDate;
    public String modifyUser;
    public Integer memCacheFlushIntvl;
    public Integer memCacheMsgCntInK;
    public Integer memCacheMsgSizeInMB;
    public Integer numPartitions;
    public Integer numTopicStores;
    public Integer unflushDataHold;
    public Integer unflushInterval;
    public Integer unflushThreshold;
    public boolean hasTLSPort;
}
