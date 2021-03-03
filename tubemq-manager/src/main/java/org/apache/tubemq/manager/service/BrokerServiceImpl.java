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


package org.apache.tubemq.manager.service;

import static org.apache.tubemq.manager.service.TubeMQHttpConst.DEFAULT_REGION;

import com.sun.xml.bind.annotation.OverrideAnnotationOf;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.tubemq.manager.entry.BrokerEntry;
import org.apache.tubemq.manager.entry.RegionEntry;
import org.apache.tubemq.manager.repository.BrokerRepository;
import org.apache.tubemq.manager.service.interfaces.BrokerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BrokerServiceImpl implements BrokerService {

    @Autowired
    BrokerRepository brokerRepository;

    @Override
    public void resetBrokerRegions(long regionId, long clusterId) {
        List<BrokerEntry> brokerEntries = brokerRepository
            .findBrokerEntriesByRegionIdEqualsAndClusterIdEquals(regionId, clusterId);
        for (BrokerEntry brokerEntry : brokerEntries) {
            brokerEntry.setRegionId(DEFAULT_REGION);
            brokerRepository.save(brokerEntry);
        }
    }

    @Override
    public void updateBrokersRegion(List<Long> brokerIdList, Long regionId, Long clusterId) {
        List<BrokerEntry> brokerEntries = brokerRepository.
            findBrokerEntryByBrokerIdInAndClusterIdEquals(brokerIdList, clusterId);
        for (BrokerEntry brokerEntry : brokerEntries) {
            brokerEntry.setRegionId(regionId);
            brokerRepository.save(brokerEntry);
        }
    }

    @Override
    public boolean checkIfBrokersAllExsit(List<Long> brokerIdList, long clusterId) {
        List<BrokerEntry> brokerEntries = brokerRepository
            .findBrokerEntryByBrokerIdInAndClusterIdEquals(brokerIdList, clusterId);
        return brokerEntries.size() == brokerIdList.size();
    }

    @Override
    public List<Long> getBrokerIdListInRegion(long regionId, long clusterId) {
        List<BrokerEntry> brokerEntries = brokerRepository
            .findBrokerEntriesByRegionIdEqualsAndClusterIdEquals(regionId, clusterId);
        List<Long> regionBrokerIdList = brokerEntries.stream().map(BrokerEntry::getBrokerId).collect(
            Collectors.toList());
        return regionBrokerIdList;
    }
}
