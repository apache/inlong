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

public interface DataStreamGroup {

    /**
     * Create data stream
     *
     * @return data stream builder
     * @throws Exception
     */
    DataStreamBuilder createDataStream(DataStreamConf dataStreamConf) throws Exception;

    /**
     * Init data stream group.
     * This operation will init all physical resources needed to start a stream group
     * Must be operated after all data streams were created;
     *
     * @return data stream group info
     * @throws Exception
     */
    DataStreamGroupInfo init() throws Exception;

    /**
     * Suspend the stream group and return group info.
     *
     * @return group info
     * @throws Exception
     */
    DataStreamGroupInfo suspend() throws Exception;

    /**
     * Restart the stream group and return group info.
     *
     * @return group info
     * @throws Exception
     */
    DataStreamGroupInfo restart() throws Exception;

    /**
     * delete the stream group and return group info
     *
     * @return group info
     * @throws Exception
     */
    DataStreamGroupInfo delete() throws Exception;

    /**
     * List all data streams in certain group
     *
     * @return data stream contained in this group
     * @throws Exception
     */
    List<DataStream> listStreams() throws Exception;
}
