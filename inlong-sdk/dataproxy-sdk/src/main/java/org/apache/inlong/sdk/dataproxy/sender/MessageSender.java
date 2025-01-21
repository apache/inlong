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

package org.apache.inlong.sdk.dataproxy.sender;

import org.apache.inlong.sdk.dataproxy.common.ProcessResult;

/**
 * Message Sender interface
 *
 * Used to define the sender common methods
 */
public interface MessageSender {

    /**
     * Start sender when the sender is built
     *
     * <p>Attention:
     *    if return false, the caller need to handle it based on the error code and
     *    error information returned by procResult, such as:
     *    prompting the user, retrying after some time, etc.
     * </p>
     *
     * @param procResult the start result, return detail error infos if sending fails
     * @return  true if successful, false return indicates that the sender fails to start.
     */
    boolean start(ProcessResult procResult);

    /**
     * Close the sender when need to stop the sender's sending service.
     */
    void close();
}
