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

package org.apache.inlong.manager.client.api;

import java.util.List;

public interface InlongGroup {

    /**
     * Create inlong stream
     *
     * @return inlong stream builder
     */
    InlongStreamBuilder createStream(InlongStreamConf streamConf) throws Exception;

    /**
     * Create snapshot for Inlong group
     * @return
     * @throws Exception
     */
    GroupInfo snapshot() throws Exception;

    /**
     * Init inlong group.
     * This operation will init all physical resources needed to start a stream group
     * Must be operated after all inlong streams were created;
     *
     * @return inlong group info
     */
    GroupInfo init() throws Exception;

    /**
     * Init inlong group on updated conf.
     * Must be invoked when group is rejected,failed or started
     *
     * @return inlong group info
     */
    GroupInfo initOnUpdate(InlongGroupConf conf) throws Exception;

    /**
     * Suspend the stream group and return group info.
     *
     * @return group info
     */
    GroupInfo suspend() throws Exception;

    /**
     * Restart the stream group and return group info.
     *
     * @return group info
     */
    GroupInfo restart() throws Exception;

    /**
     * delete the stream group and return group info
     *
     * @return group info
     */
    GroupInfo delete() throws Exception;

    /**
     * List all inlong streams in certain group
     *
     * @return inlong stream contained in this group
     */
    List<InlongStream> listStreams() throws Exception;
}
