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

package org.apache.inlong.sdk.dataproxy.sender.http;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.MessageSender;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;

/**
 * HTTP Message Sender interface
 *
 * Used to define the HTTP message sender methods
 */
public interface HttpMsgSender extends MessageSender {

    /**
     * Synchronously send message and wait for the final sending result
     *
     * <p>Attention: if return false, the caller can choose to wait for a period of time before trying again, or
     *    discard the event after multiple retries and failures.</p>
     *
     * @param eventInfo the event information need to send
     * @param procResult The send result, including the detail error infos if failed
     * @return  true if successful, false if failed for some reason.
     */
    boolean syncSendMessage(HttpEventInfo eventInfo, ProcessResult procResult);

    /**
     * Asynchronously send message
     *
     * <p>Attention: if return false, the caller can choose to wait for a period of time before trying again, or
     *    discard the event after multiple retries and failures.</p>
     *
     * @param eventInfo the event information need to send
     * @param callback  the callback that returns the response from DataProxy or
     *                 an exception that occurred while waiting for the response.
     * @param procResult The send result, including the detail error infos if the event not accepted
     * @return  true if successful, false if the event not accepted for some reason.
     */
    boolean asyncSendMessage(HttpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult);
}
