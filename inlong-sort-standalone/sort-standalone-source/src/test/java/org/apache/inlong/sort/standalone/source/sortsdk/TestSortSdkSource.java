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

package org.apache.inlong.sort.standalone.source.sortsdk;

import org.apache.flume.Context;
import org.apache.inlong.commons.config.metrics.MetricRegister;
import org.apache.inlong.sort.standalone.config.holder.SortClusterConfigHolder;
import org.apache.inlong.sort.standalone.config.pojo.SortClusterConfig;
import org.apache.inlong.sort.standalone.config.pojo.SortTaskConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SortClusterConfigHolder.class, LoggerFactory.class, Logger.class, MetricRegister.class})
public class TestSortSdkSource {

    private Context mockContext;

    @Before
    public void setUp() {
        PowerMockito.mockStatic(LoggerFactory.class);
        Logger LOG = PowerMockito.mock(Logger.class);
        PowerMockito.when(LoggerFactory.getLogger(Mockito.any(Class.class))).thenReturn(LOG);
        PowerMockito.mockStatic(MetricRegister.class);

        PowerMockito.mockStatic(SortClusterConfigHolder.class);
        SortClusterConfig config = prepareSortClusterConfig(2);
        PowerMockito.when(SortClusterConfigHolder.getClusterConfig()).thenReturn(config);
        mockContext = PowerMockito.spy(new Context());
    }

    @Test
    public void testRun() {
        SortSdkSource testSource = new SortSdkSource();
        testSource.configure(mockContext);
        testSource.run();
        testSource.stop();
    }

    private SortClusterConfig prepareSortClusterConfig(final int size) {
        final SortClusterConfig testConfig = new SortClusterConfig();
        testConfig.setClusterName("testConfig");
        testConfig.setSortTasks(prepareSortTaskConfig(size));
        return testConfig;
    }

    private List<SortTaskConfig> prepareSortTaskConfig(final int size) {
        List<SortTaskConfig> configs = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            SortTaskConfig config = new SortTaskConfig();
            config.setName("testConfig" + i);
            configs.add(config);
        }
        Assert.assertEquals(size, configs.size());
        return configs;
    }

}