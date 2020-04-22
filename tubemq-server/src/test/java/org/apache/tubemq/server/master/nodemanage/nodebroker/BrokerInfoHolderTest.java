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

package org.apache.tubemq.server.master.nodemanage.nodebroker;

import static org.mockito.Mockito.mock;
import org.apache.tubemq.corebase.cluster.BrokerInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BrokerInfoHolderTest {
    private BrokerInfoHolder brokerInfoHolder;
    private BrokerConfManager brokerConfManager;

    @Before
    public void setUp() throws Exception {
        brokerConfManager = mock(BrokerConfManager.class);
        brokerInfoHolder = new BrokerInfoHolder(10, brokerConfManager);
    }

    @Test
    public void testBrokerInfo() {
        BrokerInfo brokerInfo = mock(BrokerInfo.class);

        brokerInfoHolder.setBrokerInfo(1, brokerInfo);
        Assert.assertEquals(brokerInfo, brokerInfoHolder.getBrokerInfo(1));

        brokerInfoHolder.removeBroker(1);
        Assert.assertNull(brokerInfoHolder.getBrokerInfo(1));
    }
}
