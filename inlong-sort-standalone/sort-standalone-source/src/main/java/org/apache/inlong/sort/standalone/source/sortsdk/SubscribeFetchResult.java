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

import org.apache.inlong.sdk.sort.entity.MessageRecord;
import org.apache.inlong.sort.standalone.utils.Constants;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SubscribeFetchResult is the wrapper of {@link MessageRecord}.
 * <p> SubscribeFetchResult integrate message key, offset and message time in to the header map.</p>
 */
public class SubscribeFetchResult {

    // The sortId of fetched message.
    private final String sortId;

    // Important metrics called headers of {@link MessageRecord}, including message key.
    private final Map<String, String> headers = new ConcurrentHashMap<>();

    // Row data in binary format.
    private final byte[] body;

    /**
     * Private constructor of SubscribeFetchResult.
     * <p> The construction of SubscribeFetchResult should be initiated by {@link SubscribeFetchResult.Factory}.</p>
     *
     * @param sortId The sortId of fetched message.
     * @param message Message that fetched from upstream data storage.
     */
    private SubscribeFetchResult(
            final String sortId,
            final MessageRecord message) {
        this.sortId = sortId;
        this.headers.put(Constants.HEADER_KEY_MESSAGE_KEY, message.getMsgKey());
        this.headers.put(Constants.HEADER_KEY_MSG_OFFSET, message.getOffset());
        this.headers.put(Constants.HEADER_KEY_MSG_TIME, String.valueOf(message.getRecTime()));
        this.headers.putAll(message.getMsgHeader());
        this.body = message.getMessage();
    }

    /**
     * Get row data that in binary format.
     * @return Row data.
     */
    public byte[] getBody() {
        return body;
    }

    /**
     * Get important metrics in Map format called headers.
     * @return headers.
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Get sortId of fetched message.
     * @return SortId of message.
     */
    public String getSortId() {
        return sortId;
    }

    /**
     * The factory of {@link SubscribeFetchResult}.
     */
    public static class Factory {

        /**
         * Create one {@link SubscribeFetchResult}.
         * <p> Validate sortId and message before the construction of SubscribeFetchResult.</p>
         *
         * @param sortId The sortId of fetched message.
         * @param messageRecord Message that fetched from upstream data storage.
         *
         * @return One SubscribeFetchResult.
         */
        public static SubscribeFetchResult create(
                @NotBlank(message = "SortId should not be null or empty.") final String sortId,
                @NotNull(message = "MessageRecord should not be null.") final MessageRecord messageRecord) {
            return new SubscribeFetchResult(sortId, messageRecord);
        }
    }
}
