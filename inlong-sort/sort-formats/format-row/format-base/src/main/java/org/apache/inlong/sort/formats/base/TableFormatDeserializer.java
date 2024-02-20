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

package org.apache.inlong.sort.formats.base;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * The deserializer for a given format.
 */
public abstract class TableFormatDeserializer extends RichFlatMapFunction<byte[], Row>
        implements
            ResultTypeQueryable<Row> {

    private static final long serialVersionUID = 1L;

    /**
     * Initialize the format deserializer.
     *
     * @param context the format context.
     */
    public void init(TableFormatContext context) {
    }

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    /**
     * The context to create instance of {@link TableFormatDeserializer}.
     */
    public interface TableFormatContext {

        Map<String, String> getFormatProperties();
    }
}