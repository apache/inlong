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

import com.google.common.base.Preconditions;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.inlong.sdk.sort.api.ReadCallback;
import org.apache.inlong.sdk.sort.entity.MessageRecord;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Implementation of {@link ReadCallback}.
 *
 * TODO: Sort sdk should deliver one object which is held by {@link ProfileEvent} and used to ack upstream data store
 * The code should be like :
 *
 *        public void onFinished(final MessageRecord messageRecord, ACKer acker) {
 *            doSomething();
 *            final ProfileEvent profileEvent = new ProfileEvent(result.getBody(), result.getHeaders(), acker);
 *             channelProcessor.processEvent(profileEvent);
 *        }
 *
 * The ACKer will be used to <b>ACK</b> upstream after that the downstream <b>ACKed</b> sort-standalone.
 * This process seems like <b>transaction</b> of the whole sort-standalone, and which
 * ensure <b>At Least One</b> semantics.
 */
public class FetchCallback implements ReadCallback {

    /** Logger of {@link FetchCallback}.*/
    private static final Logger LOG = LoggerFactory.getLogger(FetchCallback.class);

    /** SortId of fetch message.*/
    private final String sortId;

    /** ChannelProcessor that put message in specific channel.*/
    private final ChannelProcessor channelProcessor;

    /**
     * Private constructor of {@link FetchCallback}.
     * <p> The construction of FetchCallback should be initiated by {@link FetchCallback.Factory}.</p>
     *
     * @param sortId SortId of fetch message.
     * @param channelProcessor ChannelProcessor that message put in.
     */
    private FetchCallback(
            final String sortId,
            final ChannelProcessor channelProcessor) {
        this.sortId = sortId;
        this.channelProcessor = channelProcessor;
    }

    /**
     * The callback function that SortSDK invoke when fetch messages.
     *
     * @param messageRecord message
     */
    @Override
    public void onFinished(final MessageRecord messageRecord) {
        try {
            Preconditions.checkNotNull(messageRecord);
            final SubscribeFetchResult result = SubscribeFetchResult.Factory.create(sortId, messageRecord);
            final ProfileEvent profileEvent = new ProfileEvent(result.getBody(), result.getHeaders());
            channelProcessor.processEvent(profileEvent);

        } catch (NullPointerException npe) {
            LOG.error("Fetch one NULL message from sortId {}.", sortId);
        }
    }

    /**
     * Factory of {@link FetchCallback}
     */
    public static class Factory {

        /**
         * Create one {@link FetchCallback}.
         * <p> Validate sortId and channelProcessor before the construction of FetchCallback.</p>
         *
         * @param sortId The sortId of fetched message.
         * @param channelProcessor The channelProcessor that put message in specific channel.
         *
         * @return One FetchCallback.
         */
        public static FetchCallback create(
                @NotBlank(message = "sortId should not be null or empty.") final String sortId,
                @NotNull(message = "channelProcessor should not be null.") final ChannelProcessor channelProcessor) {
            return new FetchCallback(sortId, channelProcessor);
        }
    }
}
