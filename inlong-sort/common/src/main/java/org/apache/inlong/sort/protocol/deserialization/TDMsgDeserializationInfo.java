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

package org.apache.inlong.sort.protocol.deserialization;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * .
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @Type(value = TDMsgCsvDeserializationInfo.class, name = "tdmsg_csv"),
        @Type(value = TDMsgCsv2DeserializationInfo.class, name = "tdmsg_csv2"),
        @Type(value = TDMsgKvDeserializationInfo.class, name = "tdmsg_kv"),
        @Type(value = TDMsgTlogCsvDeserializationInfo.class, name = "tdmsg_tlog_csv"),
        @Type(value = TDMsgTlogKvDeserializationInfo.class, name = "tdmsg_tlog_kv"),
})
public abstract class TDMsgDeserializationInfo implements DeserializationInfo {

    private static final long serialVersionUID = 3707412713264864315L;

    private final String tid;

    public TDMsgDeserializationInfo(@JsonProperty("tid") String tid) {
        this.tid = checkNotNull(tid);
    }

    @JsonProperty("tid")
    public String getTid() {
        return tid;
    }
}
