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

package org.apache.inlong.sdk.sort.impl;

import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InlongTopicFetcher;
import org.apache.inlong.sdk.sort.api.InlongTopicManager;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.api.SortClientConfig;
import org.apache.inlong.sdk.sort.entity.CacheZoneCluster;
import org.apache.inlong.sdk.sort.entity.InlongTopic;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ClientContext.class})
public class InlongTopicManagerImplTest {

    private InlongTopic inlongTopic;
    private ClientContext clientContext;
    private QueryConsumeConfig queryConsumeConfig;
    private InlongTopicManager inlongTopicManager;

    {
        System.setProperty("log4j2.disable.jmx", Boolean.TRUE.toString());

        inlongTopic = new InlongTopic();
        inlongTopic.setTopic("testTopic");
        inlongTopic.setPartitionId(0);
        inlongTopic.setTopicType("pulsar");
        inlongTopic.setProperties(new HashMap<>());

        CacheZoneCluster cacheZoneCluster = new CacheZoneCluster("clusterId", "bootstraps", "token");
        inlongTopic.setInlongCluster(cacheZoneCluster);

        clientContext = PowerMockito.mock(ClientContextImpl.class);

        SortClientConfig sortClientConfig = PowerMockito.mock(SortClientConfig.class);
        when(clientContext.getConfig()).thenReturn(sortClientConfig);
        when(sortClientConfig.getSortTaskId()).thenReturn("test");
        when(sortClientConfig.getUpdateMetaDataIntervalSec()).thenReturn(60);
        queryConsumeConfig = PowerMockito.mock(QueryConsumeConfigImpl.class);
        inlongTopicManager = new InlongTopicManagerImpl(clientContext, queryConsumeConfig);
    }

    @Test
    public void testAddFetcher() {
        InlongTopicManager inlongTopicManager = new InlongTopicManagerImpl(clientContext, queryConsumeConfig);

        InlongTopicFetcher inlongTopicFetcher = inlongTopicManager.addFetcher(inlongTopic);
        Assert.assertNull(inlongTopicFetcher);
    }

    @Test
    public void testRemoveFetcher() {

        InlongTopicFetcher inlongTopicFetcher = inlongTopicManager.removeFetcher(inlongTopic, true);
        Assert.assertNull(inlongTopicFetcher);

        ConcurrentHashMap<String, InlongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InlongTopicFetcher inlongTopicFetcherRmMock = PowerMockito.mock(InlongTopicFetcher.class);
        fetchers.put(inlongTopic.getTopicKey(), inlongTopicFetcherRmMock);

        Whitebox.setInternalState(inlongTopicManager, "fetchers", fetchers);

        inlongTopicFetcher = inlongTopicManager.removeFetcher(inlongTopic, true);
        Assert.assertNotNull(inlongTopicFetcher);

    }

    @Test
    public void testGetFetcher() {
        InlongTopicFetcher fetcher = inlongTopicManager.getFetcher(inlongTopic.getTopicKey());
        Assert.assertNull(fetcher);
        ConcurrentHashMap<String, InlongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InlongTopicFetcher inlongTopicFetcherRmMock = PowerMockito.mock(InlongTopicFetcher.class);
        fetchers.put(inlongTopic.getTopicKey(), inlongTopicFetcherRmMock);

        Whitebox.setInternalState(inlongTopicManager, "fetchers", fetchers);

        fetcher = inlongTopicManager.getFetcher(inlongTopic.getTopicKey());
        Assert.assertNotNull(fetcher);

    }

    @Test
    public void testGetManagedInlongTopics() {
        Set<String> managedInlongTopics = inlongTopicManager.getManagedInlongTopics();
        Assert.assertEquals(0, managedInlongTopics.size());

        ConcurrentHashMap<String, InlongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InlongTopicFetcher inlongTopicFetcherRmMock = PowerMockito.mock(InlongTopicFetcher.class);
        fetchers.put(inlongTopic.getTopicKey(), inlongTopicFetcherRmMock);
        Whitebox.setInternalState(inlongTopicManager, "fetchers", fetchers);
        managedInlongTopics = inlongTopicManager.getManagedInlongTopics();
        Assert.assertEquals(1, managedInlongTopics.size());

    }

    @Test
    public void testGetAllFetchers() {
        Collection<InlongTopicFetcher> allFetchers = inlongTopicManager.getAllFetchers();
        Assert.assertEquals(0, allFetchers.size());

        ConcurrentHashMap<String, InlongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InlongTopicFetcher inlongTopicFetcherRmMock = PowerMockito.mock(InlongTopicFetcher.class);
        fetchers.put(inlongTopic.getTopicKey(), inlongTopicFetcherRmMock);
        Whitebox.setInternalState(inlongTopicManager, "fetchers", fetchers);
        allFetchers = inlongTopicManager.getAllFetchers();
        Assert.assertEquals(1, allFetchers.size());
    }

    @Test
    public void testOfflineAllTp() {
        inlongTopicManager.offlineAllTopicsAndPartitions();
    }

    @Test
    public void testClean() {
        boolean clean = inlongTopicManager.clean();
        Assert.assertTrue(clean);
    }

    @Test
    public void testClose() {
        inlongTopicManager.close();
    }
}