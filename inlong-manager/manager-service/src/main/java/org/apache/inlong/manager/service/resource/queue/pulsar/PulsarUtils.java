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

package org.apache.inlong.manager.service.resource.queue.pulsar;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarBrokerEntryMetadata;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarLookupTopicInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarMessageInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarMessageMetadata;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarNamespacePolicies;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTenantInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicMetadata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo.HTTP_PREFIX;

/**
 * Pulsar connection utils
 */
@Slf4j
public class PulsarUtils {

    public static final String QUERY_CLUSTERS_PATH = "/admin/v2/clusters";
    public static final String QUERY_BROKERS_PATH = "/admin/v2/brokers";
    public static final String QUERY_TENANTS_PATH = "/admin/v2/tenants";
    public static final String QUERY_NAMESPACE_PATH = "/admin/v2/namespaces";
    public static final String QUERY_PERSISTENT_PATH = "/admin/v2/persistent";
    public static final String LOOKUP_TOPIC_PATH = "/lookup/v2/topic";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(
            ZoneId.systemDefault());

    private PulsarUtils() {
    }

    /**
     * Get http headers by token.
     *
     * @param token pulsar token info
     * @return add http headers for token info
     */
    private static HttpHeaders getHttpHeaders(String token) {
        HttpHeaders headers = new HttpHeaders();
        if (StringUtils.isNotEmpty(token)) {
            headers.add("Authorization", "Bearer " + token);
        }
        return headers;
    }

    /**
     * Get pulsar cluster info list.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @return list of pulsar cluster infos
     * @throws Exception any exception if occurred
     */
    public static List<String> getClusters(RestTemplate restTemplate, PulsarClusterInfo clusterInfo)
            throws Exception {
        return HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_CLUSTERS_PATH), HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                ArrayList.class);
    }

    /**
     * Get the list of active brokers.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @return list of pulsar broker infos
     * @throws Exception any exception if occurred
     */
    public static List<String> getBrokers(RestTemplate restTemplate, PulsarClusterInfo clusterInfo)
            throws Exception {
        List<String> clusters = getClusters(restTemplate, clusterInfo);
        List<String> brokers = new ArrayList<>();
        for (String brokerName : clusters) {
            brokers.addAll(HttpUtils.request(restTemplate,
                    clusterInfo.getAdminUrls(QUERY_BROKERS_PATH + "/" + brokerName), HttpMethod.GET, null,
                    getHttpHeaders(clusterInfo.getToken()), ArrayList.class));
        }
        return brokers;
    }

    /**
     * Get pulsar tenant info list.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @return list of pulsar tenant infos
     * @throws Exception any exception if occurred
     */
    public static List<String> getTenants(RestTemplate restTemplate, PulsarClusterInfo clusterInfo)
            throws Exception {
        return HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_TENANTS_PATH), HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                ArrayList.class);
    }

    /**
     * Get pulsar namespace info list.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param tenant pulsar tenant name
     * @return list of pulsar namespace infos
     * @throws Exception any exception if occurred
     */
    public static List<String> getNamespaces(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String tenant) throws Exception {
        return HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_NAMESPACE_PATH + "/" + tenant),
                HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                ArrayList.class);
    }

    /**
     * Create a new pulsar tenant.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param tenant pulsar tenant name
     * @param tenantInfo pulsar tenant info
     * @throws Exception any exception if occurred
     */
    public static void createTenant(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String tenant,
            PulsarTenantInfo tenantInfo) throws Exception {
        HttpHeaders headers = getHttpHeaders(clusterInfo.getToken());
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        String param = JsonUtils.toJsonString(tenantInfo);
        HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_TENANTS_PATH + "/" + tenant), HttpMethod.PUT,
                param, headers);
    }

    /**
     * Creates a new pulsar namespace with the specified policies.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param tenant pulsar namespace name
     * @param namespaceName pulsar namespace name
     * @param policies pulsar namespace policies info
     * @throws Exception any exception if occurred
     */
    public static void createNamespace(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String tenant,
            String namespaceName, PulsarNamespacePolicies policies) throws Exception {
        HttpHeaders headers = getHttpHeaders(clusterInfo.getToken());
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        String param = JsonUtils.toJsonString(policies);
        param = param.replaceAll("messageTtlInSeconds", "message_ttl_in_seconds")
                .replaceAll("retentionPolicies", "retention_policies");
        HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_NAMESPACE_PATH + InlongConstants.SLASH + tenant
                + InlongConstants.SLASH + namespaceName), HttpMethod.PUT, param, headers);
    }

    /**
     * Get the list of topics under a namespace.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param tenant pulsar tenant name
     * @param namespace pulsar namespace name
     * @return list of pulsar topic infos
     * @throws Exception any exception if occurred
     */
    public static List<String> getTopics(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String tenant,
            String namespace) throws Exception {
        return HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + tenant + "/" + namespace), HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                ArrayList.class);
    }

    /**
     * Get the list of partitioned topics under a namespace.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param tenant pulsar tenant name
     * @param namespace pulsar namespace name
     * @return list of pulsar partitioned topic infos
     * @throws Exception any exception if occurred
     */
    public static List<String> getPartitionedTopics(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String tenant, String namespace) throws Exception {
        return HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + tenant + "/" + namespace
                        + "/partitioned"),
                HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                ArrayList.class);
    }

    /**
     * Create a non-partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @throws Exception any exception if occurred
     */
    public static void createNonPartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath),
                HttpMethod.PUT, null, getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Create a partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @throws Exception any exception if occurred
     */
    public static void createPartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath, Integer numPartitions) throws Exception {
        HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/partitions"), HttpMethod.PUT,
                numPartitions.toString(),
                getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Get the stats-internal for the partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @return pulsar internal stat info of partitioned topic
     * @throws Exception any exception if occurred
     */
    public static JsonObject getInternalStatsPartitionedTopics(RestTemplate restTemplate,
            PulsarClusterInfo clusterInfo, String topicPath) throws Exception {
        return HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath
                + "/partitioned-internalStats"), HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                JsonObject.class);
    }

    /**
     * Get partitioned topic metadata.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @return pulsar topic metadata info
     * @throws Exception any exception if occurred
     */
    public static PulsarTopicMetadata getPartitionedTopicMetadata(RestTemplate restTemplate,
            PulsarClusterInfo clusterInfo, String topicPath) throws Exception {
        return HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/partitions"), HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()),
                PulsarTopicMetadata.class);
    }

    /**
     * Delete a topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @throws Exception any exception if occurred
     */
    public static void deleteNonPartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath),
                HttpMethod.DELETE, null, getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Force delete a topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @throws Exception any exception if occurred
     */
    public static void forceDeleteNonPartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        Map<String, Boolean> uriVariables = new HashMap<>();
        uriVariables.put("force", true);
        HttpUtils.request(restTemplate, clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath),
                HttpMethod.DELETE, uriVariables,
                getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Delete a partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @throws Exception any exception if occurred
     */
    public static void deletePartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/partitions"), HttpMethod.DELETE,
                null, getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Force delete a partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @throws Exception any exception if occurred
     */
    public static void forceDeletePartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        Map<String, Boolean> uriVariables = new HashMap<>();
        uriVariables.put("force", true);
        HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/partitions"), HttpMethod.DELETE,
                uriVariables,
                getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Delete a partitioned or non-partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @param isPartitioned pulsar is partitioned topic
     * @throws Exception any exception if occurred
     */
    public static void deleteTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String topicPath,
            boolean isPartitioned) throws Exception {
        if (isPartitioned) {
            deletePartitionedTopic(restTemplate, clusterInfo, topicPath);
        } else {
            deleteNonPartitionedTopic(restTemplate, clusterInfo, topicPath);
        }
    }

    /**
     * Force delete a partitioned or non-partitioned topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @param isPartitioned pulsar is partitioned topic
     * @throws Exception any exception if occurred
     */
    public static void forceDeleteTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String topicPath,
            boolean isPartitioned)
            throws Exception {
        if (isPartitioned) {
            forceDeletePartitionedTopic(restTemplate, clusterInfo, topicPath);
        } else {
            forceDeleteNonPartitionedTopic(restTemplate, clusterInfo, topicPath);
        }
    }

    /**
     * lookup persistent topic info.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @return pulsar broker url
     * @throws Exception any exception if occurred
     */
    public static String lookupTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String topicPath)
            throws Exception {
        PulsarLookupTopicInfo topicInfo = HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(LOOKUP_TOPIC_PATH + "/persistent/" + topicPath), HttpMethod.GET, null,
                getHttpHeaders(clusterInfo.getToken()), PulsarLookupTopicInfo.class);
        return topicInfo.getBrokerUrl();
    }

    /**
     * lookup persistent partitioned topic info.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @return map of partitioned topic info
     * @throws Exception any exception if occurred
     */
    public static Map<String, String> lookupPartitionedTopic(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        PulsarTopicMetadata metadata = getPartitionedTopicMetadata(restTemplate, clusterInfo, topicPath);
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < metadata.getPartitions(); i++) {
            String partitionTopicName = topicPath + "-partition-" + i;
            PulsarLookupTopicInfo topicInfo = HttpUtils.request(restTemplate,
                    clusterInfo.getAdminUrls(LOOKUP_TOPIC_PATH + "/persistent/" + partitionTopicName), HttpMethod.GET,
                    null,
                    getHttpHeaders(clusterInfo.getToken()), PulsarLookupTopicInfo.class);
            map.put(partitionTopicName, topicInfo.getBrokerUrl());
        }
        return map;
    }

    /**
     * Get the list of persistent subscriptions for a given topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @return list of pulsar topic subscription info
     * @throws Exception any exception if occurred
     */
    public static List<String> getSubscriptions(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath) throws Exception {
        return HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/subscriptions"), HttpMethod.GET,
                null,
                getHttpHeaders(clusterInfo.getToken()),
                ArrayList.class);
    }

    /**
     * Create a subscription on the topic.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPath pulsar topic path
     * @param subscription pulsar topic subscription info
     * @throws Exception any exception if occurred
     */
    public static void createSubscription(RestTemplate restTemplate, PulsarClusterInfo clusterInfo, String topicPath,
            String subscription) throws Exception {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("entryId", Long.MAX_VALUE);
        jsonObject.addProperty("ledgerId", Long.MAX_VALUE);
        jsonObject.addProperty("partitionIndex", -1);
        HttpHeaders headers = getHttpHeaders(clusterInfo.getToken());
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/subscription/"
                        + subscription),
                HttpMethod.PUT, jsonObject.toString(), headers);
    }

    /**
     * Examine a specific message on a topic by position relative to the earliest or the latest message.
     *
     * @param restTemplate spring framework RestTemplate
     * @param clusterInfo pulsar cluster info
     * @param topicPartition pulsar topic partition info
     * @param messageType pulsar message type info
     * @param messagePosition pulsar message position info
     * @return spring framework HttpEntity
     * @throws Exception any exception if occurred
     */
    public static ResponseEntity<byte[]> examineMessage(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPartition, String messageType, int messagePosition) throws Exception {
        String adminUrl = clusterInfo.getAdminUrl();
        String[] adminUrls = adminUrl.replace(HTTP_PREFIX, InlongConstants.EMPTY).split(InlongConstants.COMMA);
        for (int i = 0; i < adminUrls.length; i++) {
            try {
                StringBuilder urlBuilder = new StringBuilder().append(HTTP_PREFIX + adminUrls[i])
                        .append(QUERY_PERSISTENT_PATH)
                        .append("/")
                        .append(topicPartition)
                        .append("/examinemessage")
                        .append("?initialPosition=")
                        .append(messageType)
                        .append("&messagePosition=")
                        .append(messagePosition);
                ResponseEntity<byte[]> response = restTemplate.exchange(urlBuilder.toString(), HttpMethod.GET,
                        new HttpEntity<>(getHttpHeaders(clusterInfo.getToken())), byte[].class);
                if (!response.getStatusCode().is2xxSuccessful()) {
                    log.error("request error for {}, status code {}, body {}", urlBuilder.toString(),
                            response.getStatusCode(),
                            response.getBody());
                }
                return response;
            } catch (Exception e) {
                log.error("examine message for topic partition={} error, begin retry", topicPartition, e);
                if (i >= (adminUrls.length - 1)) {
                    log.error("after retry, examine message for topic partition={} still error", topicPartition, e);
                    throw e;
                }
            }
        }
        throw new Exception(String.format("examine message failed for topic partition=%s", topicPartition));
    }

    public static PulsarMessageInfo getMessageFromHttpResponse(ResponseEntity<byte[]> response, String topic) {
        List<PulsarMessageInfo> messages = PulsarUtils.getMessagesFromHttpResponse(response, topic);
        if (!messages.isEmpty()) {
            return messages.get(0);
        } else {
            return null;
        }
    }

    /**
     * Copy from getMessagesFromHttpResponse method of org.apache.pulsar.client.admin.internal.TopicsImpl class.
     *
     * @param response response info
     * @param topic topic name
     * @return list of pulsar message info
     */
    public static List<PulsarMessageInfo> getMessagesFromHttpResponse(ResponseEntity<byte[]> response, String topic) {
        HttpHeaders headers = response.getHeaders();
        String msgId = headers.getFirst("X-Pulsar-Message-ID");
        String brokerEntryTimestamp = headers.getFirst("X-Pulsar-Broker-Entry-METADATA-timestamp");
        String brokerEntryIndex = headers.getFirst("X-Pulsar-Broker-Entry-METADATA-index");
        PulsarBrokerEntryMetadata brokerEntryMetadata;
        if (brokerEntryTimestamp == null && brokerEntryIndex == null) {
            brokerEntryMetadata = null;
        } else {
            brokerEntryMetadata = new PulsarBrokerEntryMetadata();
            if (brokerEntryTimestamp != null) {
                brokerEntryMetadata.setBrokerTimestamp(parse(brokerEntryTimestamp));
            }
            if (brokerEntryIndex != null) {
                brokerEntryMetadata.setIndex(Long.parseLong(brokerEntryIndex));
            }
        }

        PulsarMessageMetadata messageMetadata = new PulsarMessageMetadata();
        Map<String, String> properties = Maps.newTreeMap();

        String tmp = headers.getFirst("X-Pulsar-publish-time");
        if (tmp != null) {
            messageMetadata.setPublishTime(parse(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-event-time");
        if (tmp != null) {
            messageMetadata.setEventTime(parse(tmp));
        }
        tmp = headers.getFirst("X-Pulsar-deliver-at-time");
        if (tmp != null) {
            messageMetadata.setDeliverAtTime(parse(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-null-value");
        if (tmp != null) {
            messageMetadata.setNullValue(Boolean.parseBoolean(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-producer-name");
        if (tmp != null) {
            messageMetadata.setProducerName(tmp);
        }

        tmp = headers.getFirst("X-Pulsar-sequence-id");
        if (tmp != null) {
            messageMetadata.setSequenceId(Long.parseLong(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-replicated-from");
        if (tmp != null) {
            messageMetadata.setReplicatedFrom(tmp);
        }

        tmp = headers.getFirst("X-Pulsar-partition-key");
        if (tmp != null) {
            messageMetadata.setPartitionKey(tmp);
        }

        tmp = headers.getFirst("X-Pulsar-compression");
        if (tmp != null) {
            messageMetadata.setCompression(tmp);
        }

        tmp = headers.getFirst("X-Pulsar-uncompressed-size");
        if (tmp != null) {
            messageMetadata.setUncompressedSize(Integer.parseInt(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-encryption-algo");
        if (tmp != null) {
            messageMetadata.setEncryptionAlgo(tmp);
        }

        tmp = headers.getFirst("X-Pulsar-partition-key-b64-encoded");
        if (tmp != null) {
            messageMetadata.setPartitionKeyB64Encoded(Boolean.parseBoolean(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-marker-type");
        if (tmp != null) {
            messageMetadata.setMarkerType(Integer.parseInt(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-txnid-least-bits");
        if (tmp != null) {
            messageMetadata.setTxnidLeastBits(Long.parseLong(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-txnid-most-bits");
        if (tmp != null) {
            messageMetadata.setTxnidMostBits(Long.parseLong(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-highest-sequence-id");
        if (tmp != null) {
            messageMetadata.setHighestSequenceId(Long.parseLong(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-uuid");
        if (tmp != null) {
            messageMetadata.setUuid(tmp);
        }

        tmp = headers.getFirst("X-Pulsar-num-chunks-from-msg");
        if (tmp != null) {
            messageMetadata.setNumChunksFromMsg(Integer.parseInt(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-total-chunk-msg-size");
        if (tmp != null) {
            messageMetadata.setTotalChunkMsgSize(Integer.parseInt(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-chunk-id");
        if (tmp != null) {
            messageMetadata.setChunkId(Integer.parseInt(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-null-partition-key");
        if (tmp != null) {
            messageMetadata.setNullPartitionKey(Boolean.parseBoolean(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-Base64-encryption-param");
        if (tmp != null) {
            messageMetadata.setEncryptionParam(Base64.getDecoder().decode(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-Base64-ordering-key");
        if (tmp != null) {
            messageMetadata.setOrderingKey(Base64.getDecoder().decode(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-Base64-schema-version-b64encoded");
        if (tmp != null) {
            messageMetadata.setSchemaVersion(Base64.getDecoder().decode(tmp));
        }

        tmp = headers.getFirst("X-Pulsar-Base64-encryption-param");
        if (tmp != null) {
            messageMetadata.setEncryptionParam(Base64.getDecoder().decode(tmp));
        }

        List<String> tmpList = headers.get("X-Pulsar-replicated-to");
        if (ObjectUtils.isNotEmpty(tmpList)) {
            if (ObjectUtils.isEmpty(messageMetadata.getReplicateTos())) {
                messageMetadata.setReplicateTos(Lists.newArrayList(tmpList));
            } else {
                messageMetadata.getReplicateTos().addAll(tmpList);
            }
        }

        tmp = headers.getFirst("X-Pulsar-batch-size");
        if (tmp != null) {
            properties.put("X-Pulsar-batch-size", (String) tmp);
        }

        for (Entry<String, List<String>> entry : headers.entrySet()) {
            if (entry.getKey().contains("X-Pulsar-PROPERTY-")) {
                String keyName = entry.getKey().substring("X-Pulsar-PROPERTY-".length());
                properties.put(keyName, (String) ((List<?>) entry.getValue()).get(0));
            }
        }

        tmp = headers.getFirst("X-Pulsar-num-batch-message");
        if (tmp != null) {
            properties.put("X-Pulsar-num-batch-message", (String) tmp);
        }
        boolean isEncrypted = false;
        tmp = headers.getFirst("X-Pulsar-Is-Encrypted");
        if (tmp != null) {
            isEncrypted = Boolean.parseBoolean(tmp);
        }

        if (!isEncrypted && headers.get("X-Pulsar-num-batch-message") != null) {
            return getIndividualMsgsFromBatch(topic, msgId, response.getBody(), properties, messageMetadata,
                    brokerEntryMetadata);
        }

        PulsarMessageInfo messageInfo = new PulsarMessageInfo();
        messageInfo.setTopic(topic);
        messageInfo.setMessageId(msgId);
        messageInfo.setProperties(messageMetadata.getProperties());
        messageInfo.setBody(response.getBody());
        messageInfo.setPulsarMessageMetadata(messageMetadata);
        if (brokerEntryMetadata != null) {
            messageInfo.setPulsarBrokerEntryMetadata(brokerEntryMetadata);
        }
        return Collections.singletonList(messageInfo);
    }

    private static long parse(String datetime) throws DateTimeParseException {
        Instant instant = Instant.from(DATE_FORMAT.parse(datetime));
        return instant.toEpochMilli();
    }

    /**
     * Copy from getIndividualMsgsFromBatch method of org.apache.pulsar.client.admin.internal.TopicsImpl class.
     *
     * @param topic topic name
     * @param msgId message id
     * @param data batch message data
     * @param properties message properties
     * @param metadata message metadata
     * @param brokerMetadata broker metadata
     * @return list of pulsar message info
     */
    private static List<PulsarMessageInfo> getIndividualMsgsFromBatch(String topic, String msgId, byte[] data,
            Map<String, String> properties, PulsarMessageMetadata metadata, PulsarBrokerEntryMetadata brokerMetadata) {
        List<PulsarMessageInfo> ret = new ArrayList<>();
        int batchSize = Integer.parseInt(properties.get("X-Pulsar-num-batch-message"));
        ByteBuffer buffer = ByteBuffer.wrap(data);
        for (int i = 0; i < batchSize; ++i) {
            String batchMsgId = msgId + ":" + i;
            PulsarMessageMetadata singleMetadata = new PulsarMessageMetadata();
            singleMetadata.setProperties(properties);
            ByteBuffer singleMessagePayload = deSerializeSingleMessageInBatch(buffer, singleMetadata, i, batchSize);
            PulsarMessageInfo messageInfo = new PulsarMessageInfo();
            messageInfo.setTopic(topic);
            messageInfo.setMessageId(batchMsgId);
            messageInfo.setProperties(singleMetadata.getProperties());
            messageInfo.setPulsarMessageMetadata(metadata);
            messageInfo.setBody(singleMessagePayload.array());
            if (brokerMetadata != null) {
                messageInfo.setPulsarBrokerEntryMetadata(brokerMetadata);
            }
            ret.add(messageInfo);
        }
        buffer.clear();
        return ret;
    }

    public static void resetCursor(RestTemplate restTemplate, PulsarClusterInfo clusterInfo,
            String topicPath, String subscription, Long resetTime) throws Exception {
        HttpUtils.request(restTemplate,
                clusterInfo.getAdminUrls(QUERY_PERSISTENT_PATH + "/" + topicPath + "/subscription/"
                        + subscription + "/resetcursor/" + resetTime),
                HttpMethod.POST, null,
                getHttpHeaders(clusterInfo.getToken()));
    }

    /**
     * Copy from deSerializeSingleMessageInBatch method of org.apache.pulsar.common.protocol.Commands class.
     *
     * @param uncompressedPayload byte buffer containing uncompressed payload
     * @param metadata message metadata
     * @param index index of the message in the batch
     * @param batchSize batch size
     * @return byte buffer
     */
    private static ByteBuffer deSerializeSingleMessageInBatch(ByteBuffer uncompressedPayload,
            PulsarMessageMetadata metadata, int index, int batchSize) {
        int singleMetaSize = (int) uncompressedPayload.getInt();
        metaDataParseFrom(metadata, uncompressedPayload, singleMetaSize);
        int singleMessagePayloadSize = metadata.getPayloadSize();
        int readerIndex = uncompressedPayload.position();
        byte[] singleMessagePayload = new byte[singleMessagePayloadSize];
        uncompressedPayload.get(singleMessagePayload);
        if (index < batchSize) {
            uncompressedPayload.position(readerIndex + singleMessagePayloadSize);
        }
        return ByteBuffer.wrap(singleMessagePayload);
    }

    /**
     * Copy from parseFrom method of org.apache.pulsar.common.api.proto.SingleMessageMetadata class.
     *
     * @param metadata message metadata
     * @param buffer byte buffer
     * @param size message size
     */
    private static void metaDataParseFrom(PulsarMessageMetadata metadata, ByteBuffer buffer, int size) {
        int endIdx = size + buffer.position();
        while (buffer.position() < endIdx) {
            int tag = readVarInt(buffer);
            switch (tag) {
                case 10:
                    int _propertiesSize = readVarInt(buffer);
                    parseFrom(metadata, buffer, _propertiesSize);
                    break;
                case 18:
                    int _partitionKeyBufferLen = readVarInt(buffer);
                    byte[] partitionKeyArray = new byte[_partitionKeyBufferLen];
                    buffer.get(partitionKeyArray);
                    metadata.setPartitionKey(new String(partitionKeyArray));
                    break;
                case 24:
                    int payloadSize = readVarInt(buffer);
                    metadata.setPayloadSize(payloadSize);
                    break;
                case 32:
                    boolean compactedOut = readVarInt(buffer) == 1;
                    metadata.setCompactedOut(compactedOut);
                    break;
                case 40:
                    long eventTime = readVarInt64(buffer);
                    metadata.setEventTime(eventTime);
                    break;
                case 48:
                    boolean partitionKeyB64Encoded = readVarInt(buffer) == 1;
                    metadata.setPartitionKeyB64Encoded(partitionKeyB64Encoded);
                    break;
                case 58:
                    int _orderingKeyLen = readVarInt(buffer);
                    byte[] orderingKeyArray = new byte[_orderingKeyLen];
                    metadata.setOrderingKey(orderingKeyArray);
                    break;
                case 64:
                    long sequenceId = readVarInt64(buffer);
                    metadata.setSequenceId(sequenceId);
                    break;
                case 72:
                    boolean nullValue = readVarInt(buffer) == 1;
                    metadata.setNullValue(nullValue);
                    break;
                case 80:
                    boolean nullPartitionKey = readVarInt(buffer) == 1;
                    metadata.setNullPartitionKey(nullPartitionKey);
                    break;
                default:
                    skipUnknownField(tag, buffer);
            }
        }
    }

    /**
     * Copy from readVarInt method of org.apache.pulsar.common.api.proto.LightProtoCodec class.
     *
     * @param buf byte buffer
     * @return value of int
     */
    private static int readVarInt(ByteBuffer buf) {
        byte tmp = buf.get();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if ((tmp = buf.get()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if ((tmp = buf.get()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if ((tmp = buf.get()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        result |= (tmp = buf.get()) << 28;
                        if (tmp < 0) {
                            for (int i = 0; i < 5; ++i) {
                                if (buf.get() >= 0) {
                                    return result;
                                }
                            }
                            throw new IllegalArgumentException("Encountered a malformed varint.");
                        }
                    }
                }
            }
            return result;
        }
    }

    /**
     * Copy from readVarInt64 method of org.apache.pulsar.common.api.proto.LightProtoCodec class.
     *
     * @param buf byte buffer
     * @return value of long
     */
    private static long readVarInt64(ByteBuffer buf) {
        int shift = 0;
        for (long result = 0L; shift < 64; shift += 7) {
            byte b = buf.get();
            result |= (long) (b & 127) << shift;
            if ((b & 128) == 0) {
                return result;
            }
        }
        throw new IllegalArgumentException("Encountered a malformed varint.");
    }

    /**
     * Copy from getTagType method of org.apache.pulsar.common.api.proto.LightProtoCodec class.
     *
     * @param tag tag number
     * @return tag type
     */
    private static int getTagType(int tag) {
        return tag & 7;
    }

    /**
     * Copy from skipUnknownField method of org.apache.pulsar.common.api.proto.LightProtoCodec class.
     *
     * @param tag tag number
     * @param buffer byte buffer
     */
    private static void skipUnknownField(int tag, ByteBuffer buffer) {
        int tagType = getTagType(tag);
        switch (tagType) {
            case 0:
                readVarInt(buffer);
                break;
            case 1:
                buffer.get(new byte[8]);
                break;
            case 2:
                int len = readVarInt(buffer);
                buffer.get(new byte[len]);
                break;
            case 5:
                buffer.get(new byte[4]);
                break;
            case 3:
            case 4:
            default:
                throw new IllegalArgumentException("Invalid tag type: " + tagType);

        }
    }

    /**
     * Copy from parseFrom method of org.apache.pulsar.common.api.proto.KeyValue class.
     *
     * @param metadata message metadata
     * @param buffer byte buffer
     * @param size message size
     */
    private static void parseFrom(PulsarMessageMetadata metadata, ByteBuffer buffer, int size) {
        if (ObjectUtils.isEmpty(metadata.getProperties())) {
            metadata.setProperties(new HashMap<>());
        }
        Map<String, String> properties = metadata.getProperties();
        int endIdx = buffer.position() + size;
        String key = null;
        String value = null;
        while (buffer.position() < endIdx) {
            int tag = readVarInt(buffer);
            if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
                properties.put(key, value);
                key = null;
                value = null;
            }
            switch (tag) {
                case 10:
                    int keyBufferLen = readVarInt(buffer);
                    byte[] keyArray = new byte[keyBufferLen];
                    buffer.get(keyArray);
                    key = new String(keyArray);
                    break;
                case 18:
                    int valueBufferLen = readVarInt(buffer);
                    byte[] valueArray = new byte[valueBufferLen];
                    buffer.get(valueArray);
                    value = new String(valueArray);
                    break;
                default:
                    skipUnknownField(tag, buffer);
            }
        }
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
            properties.put(key, value);
        }
    }
}
