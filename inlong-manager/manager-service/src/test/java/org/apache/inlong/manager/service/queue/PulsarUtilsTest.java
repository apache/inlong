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

package org.apache.inlong.manager.service.queue;

import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarLookupTopicInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarNamespacePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarPersistencePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarRetentionPolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTenantInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicMetadata;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test class for Pulsar utils.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PulsarUtilsTest {

    private static final Gson GSON = new GsonBuilder().create(); // thread safe

    private static final String DEFAULT_SERVICE_URL = "http://127.0.0.1:8080";

    private static final String DEFAULT_TENANT = "public";

    private static final String DEFAULT_NAMESPACE = "default";

    private static final String DEFAULT_TOPIC = "testtopic";

    private static final String DEFAULT_NON_PRTITIONED_TOPIC = "testtopic_np";

    private static final int DEFAULT_PARTITIONS_NUM = 3;

    private static final String DEFAULT_TOPIC_PATH = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + DEFAULT_TOPIC;

    private static final String DEFAULT_NON_PRTITIONED_TOPIC_PATH =
            DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + DEFAULT_NON_PRTITIONED_TOPIC;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ResponseEntity<String> exchange;

    @Mock
    private ResponseEntity<byte[]> byteExchange;

    @Mock
    private PulsarClusterInfo pulsarClusterInfo;

    private static PulsarNamespacePolicies policies;

    @BeforeAll
    public static void beforeAll() {
        policies = new PulsarNamespacePolicies();
        policies.setMessageTtlInSeconds(10);
        PulsarPersistencePolicies persistencePolicies = new PulsarPersistencePolicies();
        persistencePolicies.setBookkeeperEnsemble(1);
        persistencePolicies.setBookkeeperAckQuorum(2);
        persistencePolicies.setBookkeeperWriteQuorum(3);
        persistencePolicies.setManagedLedgerMaxMarkDeleteRate(4.0);
        policies.setPersistence(persistencePolicies);
        PulsarRetentionPolicies retentionPolicies = new PulsarRetentionPolicies();
        retentionPolicies.setRetentionSizeInMB(2048l);
        retentionPolicies.setRetentionTimeInMinutes(500);
        policies.setRetentionPolicies(retentionPolicies);
    }

    @BeforeEach
    public void before() {
        when(pulsarClusterInfo.getAdminUrl()).thenReturn(DEFAULT_SERVICE_URL);
        when(pulsarClusterInfo.getToken()).thenReturn("testtoken");

    }
    /**
     * Test cases for {@link PulsarUtils#getPulsarClusters}.
     */
    @Test
    public void testGetPulsarClusters() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_CLUSTERS_PATH;
        final String result = "[\"standalone\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        List<String> clusters = PulsarUtils.getPulsarBrokers(restTemplate, pulsarClusterInfo);
        assertEquals(expected.size(), clusters.size());
        assertEquals(expected, clusters);
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarBrokers}.
     */
    @Test
    public void testGetPulsarBrokers() throws Exception {
        final String clusters = "[\"standalone\",\"standalone1\"]";
        final String brokers1 = "[\"localhost:8080\"]";
        final String brokers2 = "[\"localhost:8081\"]";
        when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class),
                eq(String.class))).thenReturn(exchange);
        when(exchange.getBody()).thenReturn(clusters, clusters, brokers1, brokers1, brokers2, brokers2);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK, HttpStatus.OK, HttpStatus.OK);

        List<String> brokers = PulsarUtils.getPulsarBrokers(restTemplate, pulsarClusterInfo);
        assertEquals(2, brokers.size());
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarTenants}.
     */
    @Test
    public void testGetPulsarTenants() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_TENANTS_PATH;
        final String result = "[\"public\",\"pulsar\",\"sample\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        List<String> tenants = PulsarUtils.getPulsarTenants(restTemplate, pulsarClusterInfo);
        assertEquals(expected.size(), tenants.size());
        assertEquals(expected, tenants);
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarNamespaces}.
     */
    @Test
    public void testGetPulsarNamespaces() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_NAMESPACE_PATH + "/public";
        final String result = "[\"public/default\",\"public/functions\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        List<String> namespaces = PulsarUtils.getPulsarNamespaces(restTemplate, pulsarClusterInfo, DEFAULT_TENANT);
        assertEquals(expected.size(), namespaces.size());
        assertEquals(expected, namespaces);
    }

    /**
     * Test cases for {@link PulsarUtils#createTenant}.
     */
    @Test
    public void testCreateTenant() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_TENANTS_PATH + "/" + DEFAULT_TENANT;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        PulsarTenantInfo pulsarTenantInfo = new PulsarTenantInfo();
        pulsarTenantInfo.setAdminRoles(Sets.newHashSet());
        pulsarTenantInfo.setAllowedClusters(Sets.newHashSet());
        PulsarUtils.createTenant(restTemplate, pulsarClusterInfo, DEFAULT_TENANT, pulsarTenantInfo);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class));
    }

    /**
     * Test cases for {@link PulsarUtils#createNamespace}.
     */
    @Test
    public void testCreateNamespace() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_NAMESPACE_PATH + "/" + DEFAULT_NAMESPACE;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        String param = GSON.toJson(policies);
        param = param.replaceAll("messageTtlInSeconds", "message_ttl_in_seconds")
                .replaceAll("retentionPolicies", "retention_policies");
        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());

        PulsarUtils.createNamespace(restTemplate, pulsarClusterInfo, DEFAULT_NAMESPACE, policies);

        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(notNullValue(), is(param)));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarTopics}.
     */
    @Test
    public void testGetPulsarTopics() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/public/default";
        final String result = "[\"persistent://public/default/testtopic-partition-0\","
                + "\"persistent://public/default/testtopic-partition-1\","
                + "\"persistent://public/default/testtopic-partition-2\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        List<String> topics = PulsarUtils.getPulsarTopics(restTemplate, pulsarClusterInfo, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertEquals(expected.size(), topics.size());
        assertEquals(expected, topics);
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarPartitionedTopics}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getList</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/partitioned
     * method is: GET
     * request body: none
     * response : ["string"]
     */
    @Test
    public void testGetPulsarPartitionedTopics() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/public/default/partitioned";
        final String result = "[\"persistent://public/default/testtopic\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        List<String> topics = PulsarUtils.getPulsarPartitionedTopics(restTemplate, pulsarClusterInfo, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertEquals(expected.size(), topics.size());
        assertEquals(expected, topics);
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarPartitionedTopics}.
     */
    @Test
    public void testCreateNonPartitionedTopic() throws Exception {
        final String url =
                DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_NON_PRTITIONED_TOPIC_PATH;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.createNonPartitionedTopic(restTemplate, pulsarClusterInfo, DEFAULT_NON_PRTITIONED_TOPIC_PATH);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(nullValue()));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#createPartitionedTopic}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_createPartitionedTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: PUT
     * request body: integer <int32> (The number of partitions for the topic)
     */
    @Test
    public void testCreatePartitionedTopic() throws Exception {
        final String url =
                DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH + "/partitions";
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.createPartitionedTopic(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH, DEFAULT_PARTITIONS_NUM);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(),
                allOf(notNullValue(), is(String.valueOf(DEFAULT_PARTITIONS_NUM))));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarStatsPartitionedTopics}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getPartitionedStatsInternal</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitioned-internalStats
     * method is: GET
     * request body: none
     * response : json
     */
    @Test
    public void testGetPulsarStatsPartitionedTopics() throws Exception {
        String json = "{\"metadata\": {\"deleted\": true,\"partitions\": 0},\"partitions\": {\"property1\": "
                + "{\"currentLedgerEntries\": 0,\"currentLedgerSize\": 0}}}";
        JsonObject expected = GSON.fromJson(json, JsonObject.class);
        String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH
                + "/partitioned-internalStats";
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(exchange.getBody()).thenReturn(json);

        JsonObject stats = PulsarUtils.getPulsarStatsPartitionedTopics(restTemplate, pulsarClusterInfo,
                DEFAULT_TOPIC_PATH);
        assertEquals(expected.toString(), stats.toString());
    }

    /**
     * Test cases for {@link PulsarUtils#getPulsarPartitionedTopicMetadata}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getPartitionedMetadata</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: GET
     * request body: none
     * response : json
     */
    @Test
    public void testGetPulsarPartitionedTopicMetadata() throws Exception {
        String json = "{\n  \"deleted\": true,\n  \"partitions\": 0,\n  \"properties\": {\n"
                + "    \"property1\": \"string\",\n    \"property2\": \"string\"\n  }\n}";
        PulsarTopicMetadata expected = GSON.fromJson(json, PulsarTopicMetadata.class);
        String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH + "/partitions";
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(exchange.getBody()).thenReturn(json);

        PulsarTopicMetadata metadata = PulsarUtils.getPulsarPartitionedTopicMetadata(restTemplate, pulsarClusterInfo,
                DEFAULT_TOPIC_PATH);
        assertEquals(expected, metadata);
    }

    /**
     * Test cases for {@link PulsarUtils#deleteNonPartitionedTopic}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deleteTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}
     * method is: DELETE
     * request body: none
     */
    @Test
    public void testDeleteNonPartitionedTopic() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.DELETE), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.deleteNonPartitionedTopic(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.DELETE), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(nullValue()));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#deleteNonPartitionedTopic}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deleteTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}
     * method is: DELETE
     * request body: none
     */
    @Test
    public void testForceDeleteNonPartitionedTopic() throws Exception {
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.DELETE), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.forceDeleteNonPartitionedTopic(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        Map<String, Boolean> uriVariables = new HashMap<>();
        uriVariables.put("force", true);
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.DELETE), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(notNullValue(), is(uriVariables)));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#deleteNonPartitionedTopic}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deletePartitionedTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: DELETE
     * request body: none
     */
    @Test
    public void testDeletePartitionedTopic() throws Exception {
        final String url =
                DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH + "/partitions";
        when(restTemplate.exchange(eq(url), eq(HttpMethod.DELETE), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.deletePartitionedTopic(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.DELETE), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(nullValue()));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#forceDeletePartitionedTopic}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_deletePartitionedTopic</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions
     * method is: DELETE
     * request body: none
     */
    @Test
    public void testForceDeletePartitionedTopic() throws Exception {
        final String url =
                DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH + "/partitions";
        when(restTemplate.exchange(eq(url), eq(HttpMethod.DELETE), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.forceDeletePartitionedTopic(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        Map<String, Boolean> uriVariables = new HashMap<>();
        uriVariables.put("force", true);
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.DELETE), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(notNullValue(), is(uriVariables)));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#lookupTopic}.
     */
    @Test
    public void testLookupTopic() throws Exception {
        final String result = "{\"brokerUrl\":\"pulsar://localhost:6650\",\"httpUrl\":\"http://localhost:8080\","
                + "\"nativeUrl\":\"pulsar://localhost:6650\",\"brokerUrlSsl\":\"\"}";
        final String url = DEFAULT_SERVICE_URL + PulsarUtils.LOOKUP_TOPIC_PATH + "/persistent/" + DEFAULT_TOPIC_PATH;
        PulsarLookupTopicInfo expected = GSON.fromJson(result, PulsarLookupTopicInfo.class);

        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        String actual = PulsarUtils.lookupTopic(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH);
        assertEquals(expected.getBrokerUrl(), actual);
    }

    /**
     * Test cases for {@link PulsarUtils#lookupPartitionedTopic}.
     */
    @Test
    public void testLookupPartitionedTopic() throws Exception {
        final int partitionsNum = 3;
        final String json = "{\n \"deleted\": true,\n \"partitions\":" + partitionsNum
                + " ,\n  \"properties\": {\n    \"property1\": \"string\",\n    \"property2\": \"string\"\n  }\n}";
        final String result = "{\"brokerUrl\":\"pulsar://localhost:6650\",\"httpUrl\":\"http://localhost:8080\","
                + "\"nativeUrl\":\"pulsar://localhost:6650\",\"brokerUrlSsl\":\"\"}";
        when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class),
                eq(String.class))).thenReturn(exchange);
        when(exchange.getBody()).thenReturn(json, json, result, result, result, result, result, result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK, HttpStatus.OK, HttpStatus.OK, HttpStatus.OK);

        Map<String, String> actual = PulsarUtils.lookupPartitionedTopic(restTemplate, pulsarClusterInfo,
                DEFAULT_TOPIC_PATH);
        assertEquals(partitionsNum, actual.size());
    }

    /**
     * Test cases for {@link PulsarUtils#getSubscriptions}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_getSubscriptions</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscriptions
     * method is: GET
     * response body: ["string"]
     */
    @Test
    public void testGetSubscriptions() throws Exception {
        final String url =
                DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH + "/subscriptions";
        final String result = "[\"test_subscriptions\"]";
        List<String> expected = GSON.fromJson(result, ArrayList.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(result);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        List<String> subscriptions = PulsarUtils.getSubscriptions(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH);
        assertEquals(expected.size(), subscriptions.size());
        assertEquals(expected, subscriptions);
    }

    /**
     * Test cases for {@link PulsarUtils#createSubscription}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_createSubscription</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{subscriptionName}
     * method is: PUT
     * response body: json
     */
    @Test
    public void testCreateSubscription() throws Exception {
        final String subscriptionName = "test_subscription";
        final String url =
                DEFAULT_SERVICE_URL + PulsarUtils.QUERY_PERSISTENT_PATH + "/" + DEFAULT_TOPIC_PATH + "/subscription/"
                        + subscriptionName;
        final String requestBody =
                "{\"entryId\":9223372036854775807,\"ledgerId\":9223372036854775807,\"partitionIndex\":-1}";

        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        PulsarUtils.createSubscription(restTemplate, pulsarClusterInfo, DEFAULT_TOPIC_PATH, subscriptionName);

        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(pulsarClusterInfo.getToken())) {
            headers.add("Authorization", "Bearer " + pulsarClusterInfo.getToken());
        }
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(notNullValue(), is(requestBody)));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(headers)));
    }

    /**
     * Test cases for {@link PulsarUtils#examineMessage}.
     * href: <a>https://pulsar.apache.org/admin-rest-api/#operation/PersistentTopics_examineMessage</a>
     * url like this: http://{host}:{port}/admin/v2/persistent/{tenant}/{namespace}/{topic}/examinemessage
     * method is: GET
     * request parameters:
     *   - initialPosition: Relative start position to examine message.It can be 'latest' or 'earliest'
     *   - messagePosition: The position of messages (default 1)
     *   - authoritative: Whether leader broker redirected this call to this broker. For internal use.
     * response body: byteArray
     */
    @Test
    public void testExamineMessage() throws Exception {
        final String messageType = "latest";
        final int messagePosition = 1;
        String topicPath = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + "testtopic-partition-1";
        StringBuilder urlBuilder = new StringBuilder().append(DEFAULT_SERVICE_URL)
                .append(PulsarUtils.QUERY_PERSISTENT_PATH).append("/").append(topicPath).append("/examinemessage")
                .append("?initialPosition=").append(messageType).append("&messagePosition=").append(messagePosition);
        final String expected = "test message!";

        when(restTemplate.exchange(eq(urlBuilder.toString()), eq(HttpMethod.GET), any(HttpEntity.class),
                eq(byte[].class))).thenReturn(byteExchange);
        when(byteExchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(byteExchange.getBody()).thenReturn(expected.getBytes(StandardCharsets.UTF_8));

        ResponseEntity<byte[]> response = PulsarUtils.examineMessage(restTemplate, pulsarClusterInfo, topicPath,
                messageType, messagePosition);
        assertEquals(expected, new String(response.getBody()));
    }
}
