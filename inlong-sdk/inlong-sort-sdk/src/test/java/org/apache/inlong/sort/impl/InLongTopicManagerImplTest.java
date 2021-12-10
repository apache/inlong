package org.apache.inlong.sort.impl;

import static org.powermock.api.mockito.PowerMockito.when;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.inlong.sort.api.ClientContext;
import org.apache.inlong.sort.api.InLongTopicFetcher;
import org.apache.inlong.sort.api.InLongTopicManager;
import org.apache.inlong.sort.api.QueryConsumeConfig;
import org.apache.inlong.sort.api.SortClientConfig;
import org.apache.inlong.sort.entity.CacheZoneCluster;
import org.apache.inlong.sort.entity.InLongTopic;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SortClientConfig.class, ClientContext.class})
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

    }

    @Test
    public void testGetFetcher() {
        InLongTopicManager inLongTopicManager = new InLongTopicManagerImpl(clientContext, queryConsumeConfig);
        InLongTopicFetcher fetcher = inLongTopicManager.getFetcher(inLongTopic.getTopicKey());
        Assert.assertNull(fetcher);

    }

    public void testGetManagedInLongTopics() {
    }

    public void testGetAllFetchers() {
    }

    public void testOfflineAllTp() {
    }

    public void testClean() {
    }
}