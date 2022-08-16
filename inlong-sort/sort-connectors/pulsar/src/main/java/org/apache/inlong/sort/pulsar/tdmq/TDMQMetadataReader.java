/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.pulsar.tdmq;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.com.google.common.collect.Iterables;
import org.apache.pulsar.shade.com.google.common.collect.Sets;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.ENABLE_KEY_HASH_RANGE_KEY;

/**
 * Copy from io.streamnative.connectors:pulsar-flink-connector_2.11:1.13.6.1-rc9,
 * From {@link org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader}
 * A Helper class that talks to Pulsar Admin API.
 * - getEarliest / Latest / Specific MessageIds
 * - guarantee message existence using subscription by setup, move and remove
 */
@Slf4j
public class TDMQMetadataReader implements AutoCloseable {

    @Getter
    private final String adminUrl;

    @Getter
    private final ClientConfigurationData clientConf;

    private final String subscriptionName;

    private final Map<String, String> caseInsensitiveParams;

    private final int indexOfThisSubtask;

    private final int numParallelSubtasks;

    private final PulsarAdmin admin;

    private volatile boolean closed = false;

    private Set<TopicRange> seenTopics = new HashSet<>();

    private final boolean useExternalSubscription;

    private final SerializableRange range;

    public TDMQMetadataReader(
            String adminUrl,
            ClientConfigurationData clientConf,
            String subscriptionName,
            Map<String, String> caseInsensitiveParams,
            int indexOfThisSubtask,
            int numParallelSubtasks,
            boolean useExternalSubscription) throws PulsarClientException {

        this.adminUrl = adminUrl;
        this.clientConf = clientConf;
        this.subscriptionName = subscriptionName;
        this.caseInsensitiveParams = caseInsensitiveParams;
        this.indexOfThisSubtask = indexOfThisSubtask;
        this.numParallelSubtasks = numParallelSubtasks;
        this.useExternalSubscription = useExternalSubscription;
        this.admin = PulsarClientUtils.newAdminFromConf(adminUrl, clientConf);
        this.range = buildRange(caseInsensitiveParams);
    }

    private SerializableRange buildRange(Map<String, String> caseInsensitiveParams) {
        if (numParallelSubtasks <= 0 || indexOfThisSubtask < 0){
            return SerializableRange.ofFullRange();
        }
        if (caseInsensitiveParams == null || caseInsensitiveParams.isEmpty() ||
                !caseInsensitiveParams.containsKey(ENABLE_KEY_HASH_RANGE_KEY)) {
            return SerializableRange.ofFullRange();
        }
        final String enableKeyHashRange = caseInsensitiveParams.get(ENABLE_KEY_HASH_RANGE_KEY);
        if (!Boolean.parseBoolean(enableKeyHashRange)){
            return SerializableRange.ofFullRange();
        }
        final Range range = SourceSinkUtils.distributeRange(numParallelSubtasks, indexOfThisSubtask);
        return SerializableRange.of(range);
    }

    public TDMQMetadataReader(
            String adminUrl,
            ClientConfigurationData clientConf,
            String subscriptionName,
            Map<String, String> caseInsensitiveParams,
            int indexOfThisSubtask,
            int numParallelSubtasks) throws PulsarClientException {

        this(adminUrl, clientConf, subscriptionName, caseInsensitiveParams, indexOfThisSubtask, numParallelSubtasks, false);
    }

    @Override
    public void close() {
        closed = true;
        admin.close();
    }

    public Set<TopicRange> discoverTopicChanges() throws PulsarAdminException, ClosedException {
        if (!closed) {
            Set<TopicRange> currentTopics = getTopicPartitionRanges();
            Set<TopicRange> addedTopics = Sets.difference(currentTopics, seenTopics);
            seenTopics = currentTopics;
            return addedTopics;
        } else {
            throw new ClosedException();
        }
    }

    public void createTenant(String tenant) throws PulsarAdminException {
        Set<String> clusters = new HashSet<>(admin.clusters().getClusters());
        admin.tenants().createTenant(tenant, TenantInfoImpl.builder().allowedClusters(clusters).build());
    }

    public boolean tenantExists(String tenant) throws PulsarAdminException {
        try {
            admin.tenants().getTenantInfo(tenant);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        }
        return true;
    }

    public List<String> listNamespaces() throws PulsarAdminException {
        List<String> tenants = admin.tenants().getTenants();
        List<String> namespaces = new ArrayList<String>();
        for (String tenant : tenants) {
            namespaces.addAll(admin.namespaces().getNamespaces(tenant));
        }
        return namespaces;
    }

    public boolean namespaceExists(String ns) throws PulsarAdminException {
        try {
            admin.namespaces().getTopics(ns);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        }
        return true;
    }

    public void createNamespace(String ns) throws PulsarAdminException {
            createNamespace(ns, false);
    }

    public void createNamespace(String ns, boolean retain) throws PulsarAdminException {
        String nsName = NamespaceName.get(ns).toString();
        admin.namespaces().createNamespace(nsName);
        if (retain) {
            // retain the topic infinitely to store the metadata
            admin.namespaces().setRetention(nsName, new RetentionPolicies(-1, -1));
        }
    }

    public void deleteNamespace(String ns) throws PulsarAdminException {
        String nsName = NamespaceName.get(ns).toString();
        admin.namespaces().deleteNamespace(nsName);
    }

    public List<String> getTopics(String ns) throws PulsarAdminException {
        List<String> nonPartitionedTopics = getNonPartitionedTopics(ns);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList(ns);
        List<String> allTopics = new ArrayList<>();
        Stream.of(partitionedTopics, nonPartitionedTopics).forEach(allTopics::addAll);
        return allTopics.stream().map(t -> TopicName.get(t).getLocalName()).collect(Collectors.toList());
    }

    public boolean topicExists(String topicName) throws PulsarAdminException {
        try {
            PartitionedTopicMetadata partitionedTopicMetadata = admin.topics().getPartitionedTopicMetadata(topicName);
            if (partitionedTopicMetadata.partitions > 0) {
                return true;
            }
        } catch (PulsarAdminException.NotFoundException e) {
        }
        return false;
    }

    public void deleteTopic(String topicName) throws PulsarAdminException {

        try {
            PartitionedTopicInternalStats partitionedInternalStats = admin.topics().getPartitionedInternalStats(topicName);
            final Optional<PersistentTopicInternalStats> any = partitionedInternalStats.partitions.entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .filter(p -> !p.cursors.isEmpty())
                .findAny();
            if (any.isPresent()) {
                throw new IllegalStateException(String.format("The topic[%s] cannot be deleted because there are subscribers", topicName));
            }
            admin.topics().deletePartitionedTopic(topicName, true);
        } catch (PulsarAdminException.NotFoundException e) {
            log.warn("topic<{}> is not exit, try delete force it", topicName);
            admin.topics().delete(topicName, true);
        }
    }

    public void createTopic(String topicName, int partitionNum) throws PulsarAdminException, IncompatibleSchemaException {
        if (partitionNum > 0) {
            admin.topics().createPartitionedTopic(topicName, partitionNum);
        } else {
            admin.topics().createNonPartitionedTopic(topicName);
        }
    }

    public void uploadSchema(String topicName, SchemaInfo schemaInfo) throws IncompatibleSchemaException {
        SchemaUtils.uploadPulsarSchema(admin, topicName, schemaInfo);
    }

    public void deleteSchema(String topicName) {
        SchemaUtils.deletePulsarSchema(admin, topicName);
    }

    public void setupCursor(Map<TopicRange, MessageId> offset, boolean failOnDataLoss) {
        // if failOnDataLoss is false, we could continue, and re-create the sub.
        if (!useExternalSubscription || !failOnDataLoss) {
            for (Map.Entry<TopicRange, MessageId> entry : offset.entrySet()) {
                String subscriptionName = subscriptionNameFrom(entry.getKey());
                try {
                    log.info("Setting up subscription {} on topic {} at position {}", subscriptionName, entry.getKey(), entry.getValue());
                    admin.topics().createSubscription(entry.getKey().getTopic(), subscriptionName, entry.getValue());
                    log.info("Subscription {} on topic {} at position {} finished", subscriptionName, entry.getKey(), entry.getValue());
                } catch (PulsarAdminException.ConflictException e) {
                    log.info("Subscription {} on topic {} already exists", subscriptionName, entry.getKey());
                } catch (PulsarAdminException e) {
                    throw new RuntimeException(
                            String.format("Failed to set up cursor for %s ", entry.getKey().toString()), e);
                }
            }
        }
    }

    public void setupCursor(Map<TopicRange, MessageId> offset) {
        setupCursor(offset, true);
    }

    public void commitOffsetToCursor(Map<TopicRange, MessageId> offset) {
        for (Map.Entry<TopicRange, MessageId> entry : offset.entrySet()) {
            TopicRange tp = entry.getKey();
            try {
                log.info("Committing offset {} to topic {}", entry.getValue(), tp);
                admin.topics().resetCursor(tp.getTopic(), subscriptionNameFrom(tp), entry.getValue(), true);
                log.info("Successfully committed offset {} to topic {}", entry.getValue(), tp);
            } catch (Throwable e) {
                if (e instanceof PulsarAdminException &&
                        (((PulsarAdminException) e).getStatusCode() == 404 ||
                                ((PulsarAdminException) e).getStatusCode() == 412)) {
                    log.info("Cannot commit cursor since the topic {} has been deleted during execution", tp);
                } else {
                    throw new RuntimeException(
                            String.format("Failed to commit cursor for %s", tp), e);
                }
            }
        }
    }

    public void removeCursor(Set<TopicRange> topics) throws ClosedException {
        if (closed) {
            throw new ClosedException();
        }

        if (!useExternalSubscription) {
            for (TopicRange topicRange : topics) {
                String subscriptionName = subscriptionNameFrom(topicRange);
                try {
                    log.info("Removing subscription {} from topic {}", subscriptionName, topicRange.getTopic());
                    admin.topics().deleteSubscription(topicRange.getTopic(), subscriptionName);
                    log.info("Successfully removed subscription {} from topic {}", subscriptionName, topicRange.getTopic());
                } catch (Throwable e) {
                    if (e instanceof PulsarAdminException && ((PulsarAdminException) e).getStatusCode() == 404) {
                        log.info("Cannot remove cursor since the topic {} has been deleted during execution", topicRange.getTopic());
                    } else {
                        throw new RuntimeException(
                                String.format("Failed to remove cursor for %s", topicRange.toString()), e);
                    }
                }
            }
        }
    }

    private String subscriptionNameFrom(TopicRange topicRange) {
        return topicRange.isFullRange() ? subscriptionName : subscriptionName + topicRange.getPulsarRange();
    }

    public MessageId getPositionFromSubscription(TopicRange topic, MessageId defaultPosition) {
        try {
            String subscriptionName = subscriptionNameFrom(topic);
            TopicStats topicStats = admin.topics().getStats(topic.getTopic());
            if (topicStats.getSubscriptions().containsKey(subscriptionName)) {
                SubscriptionStats subStats = topicStats.getSubscriptions().get(subscriptionName);
                if (subStats.getConsumers().size() != 0) {
                    throw new RuntimeException("Subscription been actively used by other consumers, " +
                            "in this situation, the exactly-once semantics cannot be guaranteed.");
                } else {
                    String encodedSubName = URLEncoder.encode(subscriptionName, StandardCharsets.UTF_8.toString());
                    PersistentTopicInternalStats.CursorStats c =
                            admin.topics().getInternalStats(topic.getTopic()).cursors.get(encodedSubName);
                    String[] ids = c.markDeletePosition.split(":", 2);
                    long ledgerId = Long.parseLong(ids[0]);
                    long entryIdInMarkDelete = Long.parseLong(ids[1]);
                    // we are getting the next mid from sub position, if the entryId is -1,
                    // it denotes we haven't read data from the ledger before,
                    // therefore no need to skip the current entry for the next position
                    long entryId = entryIdInMarkDelete == -1 ? -1 : entryIdInMarkDelete + 1;
                    int partitionIdx = TopicName.getPartitionIndex(topic.getTopic());
                    return new MessageIdImpl(ledgerId, entryId, partitionIdx);
                }
            } else {
                // create sub on topic
                admin.topics().createSubscription(topic.getTopic(), subscriptionName, defaultPosition);
                return defaultPosition;
            }
        } catch (PulsarAdminException | UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to get stats for topic " + topic, e);
        }
    }

    public SchemaInfo getPulsarSchema(List<String> topics) throws IncompatibleSchemaException {
        Set<SchemaInfo> schemas = new HashSet<>();
        if (topics.size() > 0) {
            topics.forEach(t -> schemas.add(getPulsarSchema(t)));

            if (schemas.size() != 1) {
                throw new IncompatibleSchemaException(
                        String.format("Topics to read must share identical schema, however we got %d distinct schemas [%s]",
                                schemas.size(),
                                String.join(",", schemas.stream().map(SchemaInfo::toString).collect(Collectors.toList()))),
                        null);
            }
            return Iterables.getFirst(schemas, SchemaUtils.emptySchemaInfo());
        } else {
            return SchemaUtils.emptySchemaInfo();
        }
    }

    public SchemaInfo getPulsarSchema(String topic) {
        try {
            return admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
        } catch (Throwable e) {
            if (e instanceof PulsarAdminException && ((PulsarAdminException) e).getStatusCode() == 404) {
                return BytesSchema.of().getSchemaInfo();
            } else {
                throw new RuntimeException(
                    String.format("Failed to get schema information for %s", TopicName.get(topic).toString()), e);
            }
        }
    }

    public SerializableRange getRange() {
        return range;
    }

    /**
     * Get all TopicRange that should be consumed by the subTask.
     *
     * @return set of topic ranges this subTask should consume
     * @throws PulsarAdminException
     */
    public Set<TopicRange> getTopicPartitionRanges() throws PulsarAdminException {
        Set<String> topics = getTopicPartitions();
        return topics.stream()
                .filter(
                        t ->
                                SourceSinkUtils.belongsTo(
                                        t, range, numParallelSubtasks, indexOfThisSubtask))
                .map(t -> new TopicRange(t, range.getPulsarRange()))
                .collect(Collectors.toSet());
    }

    /**
     * Get topic partitions all. If the topic does not exist, it is created automatically
     *
     * @return allTopicPartitions
     * @throws PulsarAdminException pulsarAdminException
     */
    public Set<String> getTopicPartitions() throws PulsarAdminException {
        List<String> topics = getTopics();
        HashSet<String> allTopics = new HashSet<>();
        for (String topic : topics) {
            int partNum = 1;
            try {
                partNum = admin.topics().getPartitionedTopicMetadata(topic).partitions;
            } catch (PulsarAdminException.NotFoundException e) {
                log.info(
                        "topic<{}> is not exit, auto create <{}> partition to <{}>",
                        topic,
                        partNum,
                        topic);
                try {
                    createTopic(topic, partNum);
                } catch (PulsarAdminException.ConflictException conflictException) {
                    // multi thread may cause concurrent creation
                }
            }
            // pulsar still has the situation of getting 0 partitions, non-partitions topic.
            if (partNum == 0) {
                allTopics.add(topic);
            } else {
                for (int i = 0; i < partNum; i++) {
                    allTopics.add(topic + PulsarOptions.PARTITION_SUFFIX + i);
                }
            }
        }
        return allTopics;
    }

    private List<String> getTopics() throws PulsarAdminException {
        for (Map.Entry<String, String> e : caseInsensitiveParams.entrySet()) {
            if (PulsarOptions.TOPIC_OPTION_KEYS.contains(e.getKey())) {
                switch (e.getKey()) {
                    case PulsarOptions.TOPIC_SINGLE_OPTION_KEY:
                        return Collections.singletonList(TopicName.get(e.getValue()).toString());
                    case PulsarOptions.TOPIC_MULTI_OPTION_KEY:
                        return Arrays.asList(e.getValue().split(",")).stream()
                                .filter(s -> !s.isEmpty())
                                .map(t -> TopicName.get(t).toString())
                                .collect(Collectors.toList());
                    case PulsarOptions.TOPIC_PATTERN_OPTION_KEY:
                        return getTopicsWithPattern(e.getValue());
                    default:
                        throw new IllegalArgumentException(
                                "Unknown pulsar topic option: " + e.getKey());
                }
            }
        }
        return Collections.emptyList();
    }

    private List<String> getTopicsWithPattern(String topicsPattern) throws PulsarAdminException {
        TopicName dest = TopicName.get(topicsPattern);
        List<String> allNonPartitionedTopics = getNonPartitionedTopics(dest.getNamespace());
        List<String> allPartitionedTopics = admin.topics().getPartitionedTopicList(dest.getNamespace());

        Pattern shortenedTopicsPattern = Pattern.compile(dest.toString().split("://")[1]);
        return Stream.concat(allNonPartitionedTopics.stream(), allPartitionedTopics.stream())
            .map(t -> TopicName.get(t).toString())
            .filter(t -> shortenedTopicsPattern.matcher(t.split("://")[1]).matches())
            .collect(Collectors.toList());
    }

    private List<String> getNonPartitionedTopics(String namespace) throws PulsarAdminException {
        return admin.topics().getList(namespace).stream()
                .filter(t -> !TopicName.get(t).isPartitioned())
                .collect(Collectors.toList());
    }

    /**
     * Designate the close of the metadata reader.
     */
    public static class ClosedException extends Exception {
    }

    public MessageId getLastMessageId(String topic) {
        try {
            return this.admin.topics().getLastMessageId(topic);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean checkCursorAvailable(String topic, MessageIdImpl startMessageId) {
        try {
            PersistentTopicInternalStats stats = this.admin.topics().getInternalStats(topic);
            long ledgerId = startMessageId.getLedgerId();
            // Pulsar's ledger is out of order and cannot be compared by obtaining the last ledger.
            // Therefore, it is a safer way to check whether the current ledger exists.
            final Optional<PersistentTopicInternalStats.LedgerInfo> ledgerInfo = stats.ledgers.stream()
                .filter(l -> l.ledgerId == ledgerId)
                .findAny();
            return !ledgerInfo.filter(info -> startMessageId.getEntryId() > info.entries).isPresent();
        } catch (Exception e) {
            String message = MessageFormat.format(
                "valid Cursor fail topic [{0}], messageId [{2}]",
                topic, startMessageId.toString());
            throw new RuntimeException(message, e);
        }
    }

    public void resetCursor(TopicRange topicRange, MessageId messageId) {
        try {
            this.admin.topics().resetCursor(topicRange.getTopic(), subscriptionNameFrom(topicRange), messageId);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }
}
