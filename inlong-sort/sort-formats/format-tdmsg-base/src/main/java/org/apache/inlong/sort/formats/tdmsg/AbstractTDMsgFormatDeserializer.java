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

package org.apache.inlong.sort.formats.tdmsg;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.commons.msg.TDMsg1;
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base for all tdmsg format deserializers.
 */
public abstract class AbstractTDMsgFormatDeserializer implements TableFormatDeserializer {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTDMsgFormatDeserializer.class);

    /**
     * True if ignore errors in the deserialization.
     */
    @Nonnull
    protected final Boolean ignoreErrors;

    public AbstractTDMsgFormatDeserializer(@Nonnull Boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }

    /**
     * Parses the head of the tdmsg record.
     */
    protected abstract TDMsgHead parseHead(String attr) throws Exception;

    /**
     * Parses the body of the tdmsg record.
     */
    protected abstract TDMsgBody parseBody(byte[] bytes) throws Exception;

    /**
     * Converts the tdmsg record into a row.
     */
    protected abstract Row convertRow(TDMsgHead head, TDMsgBody body) throws Exception;

    @Override
    public void flatMap(
            byte[] bytes,
            Collector<Row> collector
    ) throws Exception {
        TDMsg1 tdMsg = TDMsg1.parseFrom(bytes);

        for (String attr : tdMsg.getAttrs()) {
            Iterator<byte[]> iterator = tdMsg.getIterator(attr);
            if (iterator == null) {
                continue;
            }

            TDMsgHead head;
            try {
                head = parseHead(attr);
            } catch (Exception e) {
                if (ignoreErrors) {
                    LOG.warn("Cannot properly parse the head {}.", attr, e);
                    continue;
                } else {
                    throw e;
                }
            }

            while (iterator.hasNext()) {

                byte[] bodyBytes = iterator.next();
                if (bodyBytes == null || bodyBytes.length == 0) {
                    continue;
                }

                TDMsgBody body;
                try {
                    body = parseBody(bodyBytes);
                } catch (Exception e) {
                    if (ignoreErrors) {
                        LOG.warn("Cannot properly parse the body {}.",
                                Arrays.toString(bodyBytes), e);
                        continue;
                    } else {
                        throw e;
                    }
                }

                Row row;
                try {
                    row = convertRow(head, body);
                } catch (Exception e) {
                    if (ignoreErrors) {
                        LOG.warn("Cannot properly convert the tdmsg ({}, {}) " + "to row.", head, body, e);
                        continue;
                    } else {
                        throw e;
                    }
                }

                if (row != null) {
                    collector.collect(row);
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

        AbstractTDMsgFormatDeserializer that = (AbstractTDMsgFormatDeserializer) o;
        return ignoreErrors.equals(that.ignoreErrors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreErrors);
    }
}
