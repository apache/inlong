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

package org.apache.inlong.sort.flink.multitenant.pulsar;

import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.inlong.sort.flink.SerializedRecord;
import org.apache.inlong.sort.flink.pulsar.PulsarDeserializationSchema;
import org.apache.pulsar.client.api.Message;

public class SerializedRecordDeserializationSchema implements PulsarDeserializationSchema<SerializedRecord> {

    private static final long serialVersionUID = -374382043299290093L;

    private final long dataFlowId;

    public SerializedRecordDeserializationSchema(long dataFlowId) {
        this.dataFlowId = dataFlowId;
    }

    @Override
    public DeserializationResult<SerializedRecord> deserialize(@SuppressWarnings("rawtypes") Message message)
            throws IOException {
        final byte[] data = message.getData();
        return DeserializationResult.of(new SerializedRecord(dataFlowId, message.getEventTime(), data), data.length);
    }

    @Override
    public TypeInformation<SerializedRecord> getProducedType() {
        return TypeInformation.of(SerializedRecord.class);
    }
}
