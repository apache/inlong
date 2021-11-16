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

package org.apache.inlong.tubemq.client.consumer;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.inlong.tubemq.client.common.ConfirmResult;
import org.apache.inlong.tubemq.client.common.ConsumeResult;
import org.apache.inlong.tubemq.client.common.QueryMetaResult;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.exception.TubeClientException;
import org.apache.inlong.tubemq.corebase.Shutdownable;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;

public interface ClientBalanceConsumer extends Shutdownable {

    String getClientVersion();

    String getConsumerId();

    boolean isShutdown();

    boolean isFilterConsume(String topic);

    ConsumerConfig getConsumerConfig();

    int getSourceCount();

    int getNodeId();

    /**
     * start client-balance service
     *
     * @param topicAndFilterCondMap    subscribed topic and need filtered condition items of topic
     *                                 if not need filter consume messages, set condition's set is empty or null
     * @param result    call result, the parameter is not allowed to be null
     * @throws TubeClientException    parameter abnormal
     *
     * @return  true if call success, false if failure
     */
    boolean start(Map<String, TreeSet<String>> topicAndFilterCondMap,
                  int sourceCount, int nodeId, ProcessResult result) throws TubeClientException;

    /**
     * Query partition configure information from Master
     *
     * @param result    call result, the parameter is not allowed to be null
     * @throws TubeClientException    parameter abnormal
     *
     * @return  true if call success, false if failure
     */
    boolean getPartitionMetaInfo(QueryMetaResult result) throws TubeClientException;

    boolean isPartitionsReady(long maxWaitTime);

    /**
     * Get current registered partitionKey set
     *
     * @return  the partition key set registered
     */
    Set<String> getCurRegisteredPartSet();

    /**
     * Connect to the partition's broker for consumption
     *
     * @param partitionKey    partition key
     * @param boostrapOffset     boostrap offset for consumption, if value:
     *                           < 0, broker will not change stored offset,
     *                           >= 0, broker will replace the stored offset with the specified value
     * @param result    call result, the parameter is not allowed to be null
     * @throws TubeClientException    parameter abnormal
     *
     * @return  true if call success, false if failure
     */
    boolean connect2Partition(String partitionKey, long boostrapOffset,
                              ProcessResult result) throws TubeClientException;

    /**
     * Disconnect from the registered partition for partition release
     *
     * @param partitionKey    partition key
     * @param result    call result, the parameter is not allowed to be null
     * @throws TubeClientException    parameter abnormal
     *
     * @return  true if call success, false if failure
     */
    boolean disconnectFromPartition(String partitionKey,
                                    ProcessResult result) throws TubeClientException;

    /**
     * Get consume offset information of the current registered partitions
     *
     * @return  consume offset information
     */
    Map<String, ConsumeOffsetInfo> getCurPartitionOffsetInfos();

    /**
     * Consume from messages from server
     *
     * @param result    call result, the parameter is not allowed to be null
     * @throws TubeClientException    parameter abnormal
     *
     * @return  true if call success, false if failure
     */
    boolean getMessage(ConsumeResult result) throws TubeClientException;

    /**
     * Confirm whether the messages has been consumed
     *
     * @param confirmContext    confirm context, from the corresponding field value
     *                         in the result of getting the message
     * @param isConsumed    whether the data has been successfully processed, if
     *                     true, tell the server to continue processing the next batch of data;
     *                     false, tell the server that the data has not been processed successfully
     *                            and need to be re-pulled for processing
     * @throws TubeClientException    parameter abnormal
     *
     * @return  true if call success, false if failure
     */
    boolean confirmConsume(String confirmContext, boolean isConsumed,
                           ConfirmResult result) throws TubeClientException;

}
