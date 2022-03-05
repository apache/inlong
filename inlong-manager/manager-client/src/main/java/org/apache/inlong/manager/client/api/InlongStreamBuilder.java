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

public abstract class InlongStreamBuilder {

    /**
     * Create source in stream.
     *
     * @return inlong stream builder
     */
    public abstract InlongStreamBuilder source(StreamSource source);

    /**
     * create sink in stream.
     * *
     * @return inlong stream builder
     */
    public abstract InlongStreamBuilder sink(StreamSink sink);

    /**
     * Create or update stream fields.
     *
     * @return inlong stream builder
     */
    public abstract InlongStreamBuilder fields(List<StreamField> fieldList);

    /**
     * Create data stream by builder
     *
     * @return inlong stream
     */
    public abstract InlongStream init();

    /**
     * Create or update data stream if exists by builder
     *
     * @return data stream
     */
    public abstract InlongStream initOrUpdate();
}



