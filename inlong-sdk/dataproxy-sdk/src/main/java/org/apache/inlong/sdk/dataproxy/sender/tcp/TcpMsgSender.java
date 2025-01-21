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

package org.apache.inlong.sdk.dataproxy.sender.tcp;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;
import org.apache.inlong.sdk.dataproxy.sender.MessageSender;
import org.apache.inlong.sdk.dataproxy.sender.MsgSendCallback;

/**
 * TCP Message Sender interface
 *
 * Used to define the TCP message sender methods
 */
public interface TcpMsgSender extends MessageSender {

    /**
     * Send message without response
     *
     * <p>Attention:
     * 1. if return false, the caller can choose to wait for a period of time before trying again, or
     *    discard the event after multiple retries and failures.
     * 2. this method may lose messages. It is suitable for situations where the reporting volume is very large,
     *    the business does not pay attention to the final reporting results, and
     *    the message loss is tolerated in the event of an exception.
     * </p>
     *
     * @param eventInfo the event information need to send
     * @param procResult The send result, include the detail error infos if the eventInfo is not accepted
     * @return  true if successful, false return indicates that the eventInfo was not accepted for some reason.
     */
    boolean sendMessageWithoutAck(TcpEventInfo eventInfo, ProcessResult procResult);

    /**
     * Synchronously send message and wait for the final sending result
     *
     * <p>Attention:
     * 1. if return false, the caller can choose to wait for a period of time before trying again, or
     *    discard the event after multiple retries and failures.
     * 2. this method, with sendInB2B = true, tries to ensure that messages are delivered, but there
     *    may be duplicate messages or message loss scenarios. It is suitable for scenarios with
     *    a very large number of reports, very low reporting time requirements, and
     *    the need to return the sending results.
     * 3. this method, with sendInB2B = false, ensures that the message is delivered only once and
     *    will not be repeated. It is suitable for businesses with a small amount of reports and
     *    no requirements on the reporting time, but require DataProxy to forward messages with high reliability.
     * </p>
     *
     * @param sendInB2B indicates the DataProxy message service mode, true indicates DataProxy returns
     *                 as soon as it receives the request and forwards the message in B2B mode until it succeeds;
     *                 false indicates DataProxy returns after receiving the request and forwarding it successfully,
     *                 and DataProxy does not retry on failure
     * @param eventInfo the event information need to send
     * @param procResult The send result, including the detail error infos if failed
     * @return  true if successful, false if failed for some reason.
     */
    boolean syncSendMessage(boolean sendInB2B,
            TcpEventInfo eventInfo, ProcessResult procResult);

    /**
     * Asynchronously send message
     *
     * <p>Attention:
     * 1. if return false, the caller can choose to wait for a period of time before trying again, or
     *    discard the event after multiple retries and failures.
     * 2. this method, with sendInB2B = true, tries to ensure that messages are delivered, but there
     *    may be duplicate messages or message loss scenarios. It is suitable for scenarios with
     *    a very large number of reports, very low reporting time requirements, and
     *    the need to return the sending results.
     * 3. this method, with sendInB2B = false, ensures that the message is delivered only once and
     *    will not be repeated. It is suitable for businesses with a small amount of reports and
     *    no requirements on the reporting time, but require DataProxy to forward messages with high reliability.
     * </p>
     *
     * @param sendInB2B indicates the DataProxy message service mode, true indicates DataProxy returns
     *                 as soon as it receives the request and forwards the message in B2B mode until it succeeds;
     *                 false indicates DataProxy returns after receiving the request and forwarding it successfully,
     *                 and DataProxy does not retry on failure
     * @param eventInfo the event information need to send
     * @param callback  the callback that returns the response from DataProxy or
     *                 an exception that occurred while waiting for the response.
     * @param procResult The send result, including the detail error infos if the event not accepted
     * @return  true if successful, false if the event not accepted for some reason.
     */
    boolean asyncSendMessage(boolean sendInB2B,
            TcpEventInfo eventInfo, MsgSendCallback callback, ProcessResult procResult);
}
