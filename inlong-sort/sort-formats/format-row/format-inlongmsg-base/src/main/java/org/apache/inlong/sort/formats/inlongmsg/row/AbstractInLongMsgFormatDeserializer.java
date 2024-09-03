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

package org.apache.inlong.sort.formats.inlongmsg.row;

import org.apache.inlong.common.msg.InLongMsg;
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * The base for all inlongmsg format deserializers.
 */
public abstract class AbstractInLongMsgFormatDeserializer extends TableFormatDeserializer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInLongMsgFormatDeserializer.class);

    /**
     * True if ignore errors in the deserialization.
     */
    @Nonnull
    protected FailureHandler failureHandler;

    @Deprecated
    public AbstractInLongMsgFormatDeserializer(@Nonnull Boolean ignoreErrors) {
        this(InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public AbstractInLongMsgFormatDeserializer(@Nonnull FailureHandler failureHandler) {
        this.failureHandler = Preconditions.checkNotNull(failureHandler);
    }

    /**
     * Parses the head of the inlongmsg record.
     */
    protected abstract InLongMsgHead parseHead(String attr) throws Exception;

    /**
     * Parses the body of the list InLongMsg record.
     */
    protected abstract List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception;

    /**
     * Converts the InLongMsg record into row list.
     */
    protected abstract List<Row> convertRows(InLongMsgHead head,
            InLongMsgBody body) throws Exception;

    @Override
    public void flatMap(
            byte[] bytes,
            Collector<Row> collector) throws Exception {
        InLongMsg inLongMsg = InLongMsg.parseFrom(bytes);

        for (String attr : inLongMsg.getAttrs()) {
            Iterator<byte[]> iterator = inLongMsg.getIterator(attr);
            if (iterator == null) {
                continue;
            }

            InLongMsgHead head;
            try {
                head = parseHead(attr);
            } catch (Exception e) {
                failureHandler.onParsingHeadFailure(attr, e);
                continue;
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
                    failureHandler.onParsingBodyFailure(head, bodyBytes, e);
                    continue;
                }

                for (InLongMsgBody body : bodyList) {
                    List<Row> rows;
                    try {
                        rows = convertRows(head, body);
                    } catch (Exception e) {
                        failureHandler.onConvertingRowFailure(head, body, e);
                        continue;
                    }

                    if (rows != null) {
                        for (Row row : rows) {
                            collector.collect(row);
                        }
                    }
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

    @Override
    public int hashCode() {
        return Objects.hash(failureHandler);
    }
}
