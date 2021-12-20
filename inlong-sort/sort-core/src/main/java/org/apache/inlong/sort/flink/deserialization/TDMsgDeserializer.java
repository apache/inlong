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

package org.apache.inlong.sort.flink.deserialization;

import org.apache.flink.util.Collector;
import org.apache.inlong.sort.flink.Record;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.formats.tdmsg.AbstractTDMsgFormatDeserializer;

public class TDMsgDeserializer implements Deserializer<SerializedRecord, Record> {
    private final AbstractTDMsgFormatDeserializer innerDeserializer;

    public TDMsgDeserializer(AbstractTDMsgFormatDeserializer innerDeserializer) {
        this.innerDeserializer = innerDeserializer;
    }

    @Override
    public void deserialize(SerializedRecord input, Collector<Record> collector) throws Exception {
        innerDeserializer.flatMap(input.getData(),  new CallbackCollector<>(
                row -> collector.collect(new Record(
                        input.getDataFlowId(),
                        input.getTimestampMillis(),
                        row))));
    }
}
