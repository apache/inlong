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

package org.apache.inlong.sort.singletenant.flink.deserialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.commons.msg.InLongMsg;
import org.apache.inlong.sort.singletenant.flink.SerializedRecord;

import java.util.Iterator;

public class DeserializationFunction extends ProcessFunction<SerializedRecord, Row> {

    private static final long serialVersionUID = 6245991845787657154L;

    private final DeserializationSchema<Row> deserializationSchema;

    public DeserializationFunction(
            DeserializationSchema<Row> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void processElement(
            SerializedRecord value,
            ProcessFunction<SerializedRecord, Row>.Context ctx,
            Collector<Row> out
    ) throws Exception {
        InLongMsg inLongMsg = InLongMsg.parseFrom(value.getData());

        for (String attr : inLongMsg.getAttrs()) {
            Iterator<byte[]> iterator = inLongMsg.getIterator(attr);
            if (iterator == null) {
                continue;
            }

            while (iterator.hasNext()) {

                byte[] bodyBytes = iterator.next();
                if (bodyBytes == null || bodyBytes.length == 0) {
                    continue;
                }

                deserializationSchema.deserialize(bodyBytes, out);
            }
        }
    }

}
