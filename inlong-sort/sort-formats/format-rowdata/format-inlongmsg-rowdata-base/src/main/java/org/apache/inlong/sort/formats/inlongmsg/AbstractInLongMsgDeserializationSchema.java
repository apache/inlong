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

package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.metrics.FormatMetricGroup;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * AbstractInLongMsgDeserializationSchema.
 */
public abstract class AbstractInLongMsgDeserializationSchema implements DeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInLongMsgDeserializationSchema.class);

    private final TypeInformation<RowData> typeInformation;

    private final AbstractInLongMsgFormatDeserializer formatDeserializer;

    public AbstractInLongMsgDeserializationSchema(AbstractInLongMsgFormatDeserializer formatDeserializer) {
        this.formatDeserializer = formatDeserializer;
        this.typeInformation = formatDeserializer.getProducedType();
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        try {
            MetricGroup metricGroup = context.getMetricGroup();
            checkArgument(metricGroup instanceof FormatMetricGroup,
                    "Expecting FormatMetricGroup, but got " + metricGroup.getClass().getName());

            formatDeserializer.setFormatMetricGroup((FormatMetricGroup) metricGroup);
        } catch (Exception ignore) {
            LOG.warn("FormatGroup initialization error, no format metric will be accumulated", ignore);
        }
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        throw new IOException("A InLongMsg may contain multiple inner messages and this function" +
                " can only return one message! Use function " +
                "deserialize(byte[] message, Collector<RowData> out) instead please!");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) {
        try {
            formatDeserializer.flatMap(message, out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deserializeFormatMsg(byte[] message, Collector<FormatMsg> out) {
        try {
            formatDeserializer.flatFormatMsgMap(message, out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<InLongMsgWrap> preParse(byte[] bytes) throws Exception {
        return formatDeserializer.preParse(bytes);
    }

    public void parse(
            InLongMsgWrap inLongMsgWrap,
            Collector<RowData> collector) throws Exception {
        formatDeserializer.parse(inLongMsgWrap, collector);
    }

    public void parseFormatMsg(InLongMsgWrap inLongMsgWrap,
            Collector<FormatMsg> collector) throws Exception {
        formatDeserializer.parseFormatMsg(inLongMsgWrap, collector);
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInformation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractInLongMsgDeserializationSchema that = (AbstractInLongMsgDeserializationSchema) o;
        return Objects.equals(typeInformation, that.typeInformation) &&
                Objects.equals(formatDeserializer, that.formatDeserializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInformation, formatDeserializer);
    }
}
