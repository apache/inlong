/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.formats.inlongmsg;

import com.google.common.base.Objects;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.inlong.common.msg.InlongMsg;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class InlongMsgDeserializationSchema implements DeserializationSchema<RowData> {

    /** Inner {@link DeserializationSchema} to deserialize {@link InlongMsg} inner packaged
     *  data buffer message */
    private final DeserializationSchema<RowData> deserializationSchema;

    /** {@link MetadataConverter} of how to produce metadata from {@link InlongMsg}. */
    private final MetadataConverter[] metadataConverters;

    /** {@link TypeInformation} of the produced {@link RowData} (physical + meta data). */
    private final TypeInformation<RowData> producedTypeInfo;

    /** status of error */
    private final boolean ignoreErrors;

    public InlongMsgDeserializationSchema(
            DeserializationSchema<RowData> schema,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean ignoreErrors) {
        this.deserializationSchema = schema;
        this.metadataConverters = metadataConverters;
        this.producedTypeInfo = producedTypeInfo;
        this.ignoreErrors = ignoreErrors;
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        InlongMsg inlongMsg = InlongMsg.parseFrom(message);

        for (String attr : inlongMsg.getAttrs()) {
            InlongMsgHead head;
            try {
                head = InlongMsgUtils.parseHead(attr);
            } catch (Throwable t) {
                if (ignoreErrors) {
                    continue;
                }
                throw new IOException(
                        "Failed to deserialize InlongMsg row '" + new String(message) + "'.", t);
            }

            Iterator<byte[]> iterator = inlongMsg.getIterator(attr);
            if (iterator == null) {
                continue;
            }

            while (iterator.hasNext()) {

                byte[] bodyBytes = iterator.next();
                long bodyLength = bodyBytes == null ? 0 : bodyBytes.length;

                if (bodyLength == 0) {
                    continue;
                }

                List<RowData> list = new ArrayList<>();
                ListCollector<RowData> collector = new ListCollector<>(list);
                deserializationSchema.deserialize(bodyBytes, collector);
                list.stream().forEach(rowdata -> emitRow(head, (GenericRowData) rowdata, out));
            }
        }

    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InlongMsgDeserializationSchema)) {
            return false;
        }
        InlongMsgDeserializationSchema that = (InlongMsgDeserializationSchema) o;
        return ignoreErrors == that.ignoreErrors
                && Objects.equal(Arrays.stream(metadataConverters).collect(Collectors.toList()),
                        Arrays.stream(that.metadataConverters).collect(Collectors.toList()))
                && Objects.equal(deserializationSchema, that.deserializationSchema)
                && Objects.equal(producedTypeInfo, that.producedTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(deserializationSchema, metadataConverters, producedTypeInfo, ignoreErrors);
    }

    interface MetadataConverter extends Serializable {
        Object read(InlongMsgHead head);
    }

    /** add metadata column */
    private void emitRow(InlongMsgHead head, GenericRowData physicalRow, Collector<RowData> out) {
        if (metadataConverters.length == 0) {
            out.collect(physicalRow);
            return;
        }
        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos, metadataConverters[metadataPos].read(head));
        }
        out.collect(producedRow);
    }
}
