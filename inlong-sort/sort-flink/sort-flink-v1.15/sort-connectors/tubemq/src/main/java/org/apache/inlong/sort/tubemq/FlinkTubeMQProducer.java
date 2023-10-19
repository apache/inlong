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

package org.apache.inlong.sort.tubemq;

import org.apache.inlong.sort.tubemq.table.TubeMQOptions;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class FlinkTubeMQProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTubeMQProducer.class);

    private static final String SYSTEM_HEADER_TIME_FORMAT = "yyyyMMddHHmm";

    /**
     * The address of tubemq master, format eg: 127.0.0.1:8080,127.0.0.2:8081.
     */
    private final String masterAddress;

    /**
     * The topic name.
     */
    private final String topic;

    /**
     * The tubemq consumers use this streamId set to filter records reading from server.
     */
    private final TreeSet<String> streamIdSet;
    /**
     * The serializer for the records sent to tube.
     */
    private final SerializationSchema<T> serializationSchema;

    /**
     * The tubemq producer.
     */
    private transient MessageProducer producer;

    /**
     * The tubemq session factory.
     */
    private transient MessageSessionFactory sessionFactory;

    /**
     * The maximum number of retries.
     */
    private final int maxRetries;

    public FlinkTubeMQProducer(String topic,
            String masterAddress,
            SerializationSchema<T> serializationSchema,
            TreeSet<String> streamIdSet,
            Configuration configuration) {
        checkNotNull(topic, "The topic must not be null.");
        checkNotNull(masterAddress, "The master address must not be null.");
        checkNotNull(serializationSchema, "The serialization schema must not be null.");
        checkNotNull(streamIdSet, "The streamId set must not be null.");
        checkNotNull(configuration, "The configuration must not be null.");

        int max_retries = configuration.getInteger(TubeMQOptions.MAX_RETRIES);
        checkArgument(max_retries > 0);

        this.topic = topic;
        this.masterAddress = masterAddress;
        this.serializationSchema = serializationSchema;
        this.streamIdSet = streamIdSet;
        this.maxRetries = max_retries;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
        // Nothing to do.
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
        // Nothing to do.
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        TubeClientConfig tubeClientConfig = new TubeClientConfig(masterAddress);
        this.sessionFactory = new TubeSingleSessionFactory(tubeClientConfig);
        this.producer = sessionFactory.createProducer();
        HashSet<String> hashSet = new HashSet<>();
        hashSet.add(topic);
        producer.publish(hashSet);
    }

    @Override
    public void invoke(T in, SinkFunction.Context context) throws Exception {

        int retries = 0;
        Exception exception = null;

        while (maxRetries <= 0 || retries < maxRetries) {

            try {
                byte[] body = serializationSchema.serialize(in);
                Message message = new Message(topic, body);
                MessageSentResult sendResult = producer.sendMessage(message);
                if (sendResult.isSuccess()) {
                    return;
                } else {
                    LOG.warn("Send msg fail, error code: {}, error message: {}",
                            sendResult.getErrCode(), sendResult.getErrMsg());
                }
            } catch (Exception e) {
                LOG.warn("Could not properly send the message to tube "
                        + "(retries: {}).", retries, e);

                retries++;
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        throw new IOException("Could not properly send the message to tube.", exception);
    }

    @Override
    public void close() throws Exception {

        try {
            if (producer != null) {
                producer.shutdown();
            }
            if (sessionFactory != null) {
                sessionFactory.shutdown();
            }
        } catch (Throwable e) {
            LOG.error("Shutdown producer error", e);
        } finally {
            super.close();
        }
    }
}
