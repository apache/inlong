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

import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.StreamTransform;

import java.util.List;
import java.util.Map;

/**
 * Inlong stream.
 */
public abstract class InlongStream {

    /**
     * Return name of stream.
     */
    public abstract String getName();

    /**
     * Return field definitions of stream.
     */
    public abstract List<StreamField> listFields();

    /**
     * Return data sources defined in stream, key is source name which must be unique within one stream scope.
     */
    public abstract Map<String, StreamSource> getSources();

    /**
     * Return data sinks defined in stream, key is sink name which must be unique within one stream scope.
     */
    public abstract Map<String, StreamSink> getSinks();

    /**
     * Return data transform node defined in stream(split,string replace etc)
     * key is transform name which must be unique within one stream scope.
     */
    public abstract Map<String, StreamTransform> getTransforms();

    /**
     * Add data source to stream, this method will throw exception when source name already exists in stream.
     */
    public abstract InlongStream addSource(StreamSource source);

    /**
     * Add data sink to stream, this method will throw exception when sink name already exists in stream.
     */
    public abstract InlongStream addSink(StreamSink sink);

    /**
     * Add data transform node to stream, this method will throw exception when transform name already exists in stream.
     */
    public abstract InlongStream addTransform(StreamTransform transform);

    /**
     * Delete data source by source name.
     */
    public abstract InlongStream deleteSource(String sourceName);

    /**
     * Delete data sink by sink name.
     */
    public abstract InlongStream deleteSink(String sinkName);

    /**
     * Delete data transform node by transform name.
     */
    public abstract InlongStream deleteTransform(String transformName);

    /**
     * Update data source by source name, add new one if source name not exists.
     */
    public abstract InlongStream updateSource(StreamSource source);

    /**
     * Update data sink by sink name, add new one if sink name not exists.
     */
    public abstract InlongStream updateSink(StreamSink sink);

    /**
     * Update data transform node by transform name, add new one if transform name not exists.
     */
    public abstract InlongStream updateTransform(StreamTransform transform);

    /**
     * Create stream pipeline by sources, transforms, sinks defined in stream.
     */
    public abstract StreamPipeline createPipeline();

    /**
     * Update stream definition in manager service, which must be invoked after add/delete/update source/sink/transform.
     */
    public abstract InlongStream update();
}
