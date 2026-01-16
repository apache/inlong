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

import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.sort.formats.base.FormatMsg;
import org.apache.inlong.sort.formats.metrics.FormatMetricGroup;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The base for all InLongMsg format deserializers.
 */
public abstract class AbstractInLongMsgFormatDeserializer implements ResultTypeQueryable<RowData>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInLongMsgFormatDeserializer.class);

    protected int fieldNameSize = 0;

    protected FailureHandler failureHandler;

    /**
     * The format metric group.
     */
    protected transient FormatMetricGroup formatMetricGroup;

    @Deprecated
    public AbstractInLongMsgFormatDeserializer(@Nonnull Boolean ignoreErrors) {
        this(InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public AbstractInLongMsgFormatDeserializer(@Nonnull FailureHandler failureHandler) {
        this.failureHandler = Preconditions.checkNotNull(failureHandler);
    }

    /**
     * Parses the head of the InLongMsg record.
     */
    protected abstract InLongMsgHead parseHead(String attr) throws Exception;

    /**
     * Parses the body of the list InLongMsg record.
     */
    protected abstract List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception;

    /**
     * Converts the InLongMsg record into a row.
     */
    protected abstract List<RowData> convertRowDataList(InLongMsgHead head, InLongMsgBody body) throws Exception;

    protected abstract List<FormatMsg> convertFormatMsgList(InLongMsgHead head, InLongMsgBody body) throws Exception;

    public void flatMap(
            byte[] bytes,
            Collector<RowData> collector) throws Exception {
        for (InLongMsgWrap inLongMsgWrap : preParse(bytes)) {
            parse(inLongMsgWrap, collector);
        }
    }

    public void flatFormatMsgMap(
            byte[] bytes,
            Collector<FormatMsg> collector) throws Exception {
        for (InLongMsgWrap inLongMsgWrap : preParse(bytes)) {
            parseFormatMsg(inLongMsgWrap, collector);
        }
    }

    public List<InLongMsgWrap> preParse(byte[] bytes) throws Exception {
        final List<InLongMsgWrap> result = new ArrayList<>();

        InLongMsg inLongMsg = InLongMsg.parseFrom(bytes);
        if (inLongMsg == null) {
            failureHandler.onParsingMsgFailure(bytes, new IOException(
                    String.format("Could not parse InLongMsg from bytes. bytes={}.",
                            StringUtils.join(bytes))));
            return result;
        }
        try {
            Set<String> set = inLongMsg.getAttrs();
        } catch (Exception e) {
            failureHandler.onParsingMsgFailure(bytes,
                    new IOException("Parse InLongMsg from bytes has exception.", e));
            return result;
        }
        for (String attr : inLongMsg.getAttrs()) {
            Iterator<byte[]> iterator = inLongMsg.getIterator(attr);
            if (iterator == null) {
                continue;
            }

            InLongMsgHead head;
            try {
                head = parseHead(attr);
            } catch (Exception e) {
                reportDeSerializeErrorMetrics();
                failureHandler.onParsingHeadFailure(attr, e);
                continue;
            }
            if (formatMetricGroup != null) {
                formatMetricGroup.getEventTimeDelayMillis().gauge(
                        System.currentTimeMillis() - head.getTime().getTime());
            }

            while (iterator.hasNext()) {
                byte[] bodyBytes = iterator.next();
                if (bodyBytes == null || bodyBytes.length == 0) {
                    continue;
                }

                List<InLongMsgBody> bodyList;
                try {
                    bodyList = parseBodyList(bodyBytes);
                } catch (Exception e) {
                    reportDeSerializeErrorMetrics();
                    failureHandler.onParsingBodyFailure(head, bodyBytes, e);
                    continue;
                }

                result.add(new InLongMsgWrap(head, bodyList, bodyBytes));
            }
        }

        return result;
    }

    public void parse(InLongMsgWrap inLongMsgWrap, Collector<RowData> collector) throws Exception {
        InLongMsgHead inLongMsgHead = inLongMsgWrap.getInLongMsgHead();

        for (InLongMsgBody inLongMsgBody : inLongMsgWrap.getInLongMsgBodyList()) {
            List<RowData> rowDataList;
            try {
                rowDataList = convertRowDataList(inLongMsgHead, inLongMsgBody);
            } catch (Exception e) {
                reportDeSerializeErrorMetrics();
                failureHandler.onConvertingRowFailure(inLongMsgHead, inLongMsgBody, e);
                continue;
            }
            for (RowData rowData : rowDataList) {
                collector.collect(rowData);
            }
        }
    }

    public void parseFormatMsg(InLongMsgWrap inLongMsgWrap, Collector<FormatMsg> collector) throws Exception {
        InLongMsgHead inLongMsgHead = inLongMsgWrap.getInLongMsgHead();

        for (InLongMsgBody inLongMsgBody : inLongMsgWrap.getInLongMsgBodyList()) {
            List<FormatMsg> formatMsgList;
            try {
                formatMsgList = convertFormatMsgList(inLongMsgHead, inLongMsgBody);
            } catch (Exception e) {
                reportDeSerializeErrorMetrics();
                failureHandler.onConvertingRowFailure(inLongMsgHead, inLongMsgBody, e);
                continue;
            }
            if (formatMsgList != null) {
                for (FormatMsg formatMsg : formatMsgList) {
                    collector.collect(formatMsg);
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractInLongMsgFormatDeserializer that = (AbstractInLongMsgFormatDeserializer) o;
        return Objects.equals(failureHandler, that.failureHandler);
    }

    private void reportDeSerializeErrorMetrics() {
        if (formatMetricGroup != null) {
            formatMetricGroup.getNumRecordsDeserializeError().inc();
            if (failureHandler instanceof IgnoreFailureHandler) {
                formatMetricGroup.getNumRecordsDeserializeErrorIgnored().inc();
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(failureHandler);
    }

    public void setFormatMetricGroup(FormatMetricGroup formatMetricGroup) {
        this.formatMetricGroup = formatMetricGroup;
    }

    protected void checkFieldNameSize(InLongMsgHead head, InLongMsgBody body,
            int actualNumFields, int fieldNameSize,
            FailureHandler failureHandler) {
        if (actualNumFields != fieldNameSize) {
            if (failureHandler != null) {
                failureHandler.onFieldNumError(StringUtils.join(head.getPredefinedFields(), ","),
                        body.getDataBytes(), body.getData(),
                        actualNumFields, fieldNameSize);
            } else {
                LOG.warn("The number of fields mismatches: {}"
                        + ",expected, but was {}. origin text: {}, PredefinedFields: {}",
                        fieldNameSize, actualNumFields, body,
                        StringUtils.join(head.getPredefinedFields(), ","));
            }
        }
    }
}
