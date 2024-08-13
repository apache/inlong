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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgMixedFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;

public class InLongMsgBinlogMixedFormatDeserializer extends AbstractInLongMsgMixedFormatDeserializer {

    private static final long serialVersionUID = 1L;

    public InLongMsgBinlogMixedFormatDeserializer(Boolean ignoreErrors) {
        this(InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgBinlogMixedFormatDeserializer() {
        this(DEFAULT_IGNORE_ERRORS);
    }

    public InLongMsgBinlogMixedFormatDeserializer(@Nonnull FailureHandler failureHandler) {
        super(failureHandler);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.MIXED_ROW_TYPE;
    }

    @Override
    protected InLongMsgHead parseHead(String attr) {
        return InLongMsgBinlogUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) {
        return Collections.singletonList(InLongMsgBinlogUtils.parseBody(bytes));
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) throws IOException {
        Row row = InLongMsgUtils.buildMixedRow(head, body, head.getStreamId());
        return Collections.singletonList(row);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        InLongMsgBinlogMixedFormatDeserializer that = (InLongMsgBinlogMixedFormatDeserializer) o;
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }
}
