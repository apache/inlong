package org.apache.inlong.sort.impl;

import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.sort.api.ClientContext;
import org.apache.inlong.sort.api.InLongTopicFetcher;
import org.apache.inlong.sort.api.InLongTopicManager;
import org.apache.inlong.sort.api.QueryConsumeConfig;
import org.apache.inlong.sort.api.SortClientConfig;
import org.apache.inlong.sort.entity.CacheZoneCluster;
import org.apache.inlong.sort.entity.InLongTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
public class InLongTopicManagerImplTest {

    private InLongTopic inLongTopic;
    private ClientContext clientContext;
    private SortClientConfig sortClientConfig;
    private QueryConsumeConfig queryConsumeConfig;

    @Before
    public void setUp() throws Exception {
        inLongTopic = new InLongTopic();
        inLongTopic.setTopic("testTopic");
        inLongTopic.setPartitionId(0);
        inLongTopic.setTopicType("pulsar");

        CacheZoneCluster cacheZoneCluster = new CacheZoneCluster("clusterId", "bootstraps", "token");
        inLongTopic.setInLongCluster(cacheZoneCluster);
        clientContext = PowerMockito.mock(ClientContext.class);
        sortClientConfig = PowerMockito.mock(SortClientConfig.class);

        when(clientContext.getConfig()).thenReturn(sortClientConfig);
        when(sortClientConfig.getSortTaskId()).thenReturn("test");

        queryConsumeConfig = PowerMockito.mock(QueryConsumeConfigImpl.class);

    }

    @Test
    public void testAddFetcher() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);

        InLongTopicFetcher inLongTopicFetcher = inLongTopicManager.addFetcher(inLongTopic);
        Assert.assertNull(inLongTopicFetcher);

    }

    @Test
    public void testRemoveFetcher() {

        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        InLongTopicFetcher inLongTopicFetcher = inLongTopicManager.removeFetcher(inLongTopic, true);
        Assert.assertNull(inLongTopicFetcher);

        ConcurrentHashMap<String, InLongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InLongTopicFetcher inLongTopicFetcherRmMock = PowerMockito.mock(InLongTopicFetcher.class);
        fetchers.put(inLongTopic.getTopicKey(), inLongTopicFetcherRmMock);

        Whitebox.setInternalState(inLongTopicManager, "fetchers", fetchers);

        inLongTopicFetcher = inLongTopicManager.removeFetcher(inLongTopic, true);
        Assert.assertNotNull(inLongTopicFetcher);

    }

    @Test
    public void testGetFetcher() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        InLongTopicFetcher fetcher = inLongTopicManager.getFetcher(inLongTopic.getTopicKey());
        Assert.assertNull(fetcher);
        ConcurrentHashMap<String, InLongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InLongTopicFetcher inLongTopicFetcherRmMock = PowerMockito.mock(InLongTopicFetcher.class);
        fetchers.put(inLongTopic.getTopicKey(), inLongTopicFetcherRmMock);

        Whitebox.setInternalState(inLongTopicManager, "fetchers", fetchers);

        fetcher = inLongTopicManager.getFetcher(inLongTopic.getTopicKey());
        Assert.assertNotNull(fetcher);

    }

    @Test
    public void testGetManagedInLongTopics() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        Set<String> managedInLongTopics = inLongTopicManager.getManagedInLongTopics();
        Assert.assertEquals(0, managedInLongTopics.size());

        ConcurrentHashMap<String, InLongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InLongTopicFetcher inLongTopicFetcherRmMock = PowerMockito.mock(InLongTopicFetcher.class);
        fetchers.put(inLongTopic.getTopicKey(), inLongTopicFetcherRmMock);
        Whitebox.setInternalState(inLongTopicManager, "fetchers", fetchers);
        managedInLongTopics = inLongTopicManager.getManagedInLongTopics();
        Assert.assertEquals(1, managedInLongTopics.size());

    }

    @Test
    public void testGetAllFetchers() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        Collection<InLongTopicFetcher> allFetchers = inLongTopicManager.getAllFetchers();
        Assert.assertEquals(0, allFetchers.size());

        ConcurrentHashMap<String, InLongTopicFetcher> fetchers = new ConcurrentHashMap<>();
        InLongTopicFetcher inLongTopicFetcherRmMock = PowerMockito.mock(InLongTopicFetcher.class);
        fetchers.put(inLongTopic.getTopicKey(), inLongTopicFetcherRmMock);
        Whitebox.setInternalState(inLongTopicManager, "fetchers", fetchers);
        allFetchers = inLongTopicManager.getAllFetchers();
        Assert.assertEquals(1, allFetchers.size());
    }

    @Test
    public void testOfflineAllTp() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        inLongTopicManager.offlineAllTp();
    }

    @Test
    public void testClean() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        boolean clean = inLongTopicManager.clean();
        Assert.assertTrue(clean);
    }
}