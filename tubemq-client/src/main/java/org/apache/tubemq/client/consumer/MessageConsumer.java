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

package org.apache.tubemq.client.consumer;

import java.util.List;
import java.util.Map;
import org.apache.tubemq.client.config.ConsumerConfig;
import org.apache.tubemq.client.exception.TubeClientException;
import org.apache.tubemq.corebase.Shutdownable;


public interface MessageConsumer extends Shutdownable {

    String getClientVersion();

    String getConsumerId();

    boolean isShutdown();

    ConsumerConfig getConsumerConfig();

    boolean isFilterConsume(String topic);

    Map<String, ConsumeOffsetInfo> getCurConsumedPartitions() throws TubeClientException;

    /**
     * freeze partitions, the specified partition will no longer
     * consume data until the partition is unfrozen or
     * rebalanced to other clients in the same group
     *
     * @return void
     */
    void freezePartitions(List<String> partitionKeys) throws TubeClientException;

    /**
     * unfreeze frozen partitions, the specified partition will
     * resume data consumption until the partition is frozen again
     *
     * @return void
     */
    void unfreezePartitions(List<String> partitionKeys) throws TubeClientException;

    /**
     * unfreeze all frozen partitions, the unfreeze partition will
     * resume data consumption until the partition is frozen again
     *
     * @return void
     */
    void relAllFrozenPartitions();

    /**
     * get all local frozen partitions, if the frozen partition is on this client,
     * data consumption will only be restored after unfreezing;
     * if other consumers in the same group and other consumers
     * have not frozen the partition, the freezing operation will
     * not affect the consumption of other consumers
     *
     * @return local frozen partitions
     */
    Map<String, Long> getFrozenPartInfo();

    void completeSubscribe() throws TubeClientException;

    void completeSubscribe(final String sessionKey,
                           final int sourceCount,
                           final boolean isSelectBig,
                           final Map<String, Long> partOffsetMap) throws TubeClientException;

}
