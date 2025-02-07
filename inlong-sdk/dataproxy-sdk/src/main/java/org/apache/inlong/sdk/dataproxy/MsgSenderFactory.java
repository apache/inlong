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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.sdk.dataproxy.exception.ProxySdkException;
import org.apache.inlong.sdk.dataproxy.sender.BaseSender;
import org.apache.inlong.sdk.dataproxy.sender.http.HttpMsgSenderConfig;
import org.apache.inlong.sdk.dataproxy.sender.http.InLongHttpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.InLongTcpMsgSender;
import org.apache.inlong.sdk.dataproxy.sender.tcp.TcpMsgSenderConfig;

import java.util.concurrent.ThreadFactory;

/**
 * Message Sender Factory interface
 *
 * Used to define the sender factory common methods
 */
public interface MsgSenderFactory {

    /**
     * Shutdown all senders at the factory
     *
     */
    void shutdownAll();

    /**
     * Remove the specified sender from the factory
     *
     * @param msgSender the specified sender
     */
    void removeClient(BaseSender msgSender);

    /**
     * Remove the sender number int the factory
     *
     * @return the number of senders currently in use
     */
    int getMsgSenderCount();

    /**
     * Get or generate a sender from the factory according to groupId
     *
     * @param configure  the sender configure
     * @return the sender
     */
    InLongTcpMsgSender genTcpSenderByGroupId(
            TcpMsgSenderConfig configure) throws ProxySdkException;

    /**
     * Get or generate a sender from the factory according to groupId
     *
     * @param configure  the sender configure
     * @param selfDefineFactory  the self defined network threads factory
     * @return the sender
     */
    InLongTcpMsgSender genTcpSenderByGroupId(
            TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws ProxySdkException;

    /**
     * Get or generate a sender from the factory according to clusterId
     *
     * @param configure  the sender configure
     * @return the sender
     */
    InLongTcpMsgSender genTcpSenderByClusterId(
            TcpMsgSenderConfig configure) throws ProxySdkException;

    /**
     * Get or generate a sender from the factory according to clusterId
     *
     * @param configure  the sender configure
     * @param selfDefineFactory  the self defined network threads factory
     * @return the sender
     */
    InLongTcpMsgSender genTcpSenderByClusterId(
            TcpMsgSenderConfig configure, ThreadFactory selfDefineFactory) throws ProxySdkException;

    /**
     * Get or generate a http sender from the factory according to groupId
     *
     * @param configure  the sender configure
     * @return the sender
     */
    InLongHttpMsgSender genHttpSenderByGroupId(
            HttpMsgSenderConfig configure) throws ProxySdkException;

    /**
     * Get or generate a http sender from the factory according to clusterId
     *
     * @param configure  the sender configure
     * @return the sender
     */
    InLongHttpMsgSender genHttpSenderByClusterId(
            HttpMsgSenderConfig configure) throws ProxySdkException;
}
