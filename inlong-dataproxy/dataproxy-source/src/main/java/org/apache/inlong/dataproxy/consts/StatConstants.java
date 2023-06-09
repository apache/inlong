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

package org.apache.inlong.dataproxy.consts;

public class StatConstants {

    public static final java.lang.String EVENT_SERVICE_CLOSED = "service.closed";
    public static final java.lang.String EVENT_SERVICE_SINK_UNREADY = "service.sink.unready";
    // visit
    public static final java.lang.String EVENT_VISIT_ILLEGAL = "visit.illegal";
    public static final java.lang.String EVENT_VISIT_OVERMAX = "visit.overmax";
    public static final java.lang.String EVENT_VISIT_LINKIN = "visit.linkin";
    public static final java.lang.String EVENT_VISIT_LINKOUT = "visit.linkout";
    public static final java.lang.String EVENT_VISIT_EXCEPTION = "visit.exception";
    // configure
    public static final java.lang.String EVENT_CONFIG_TOPIC_MISSING = "config.topic.missing";
    // source
    public static final java.lang.String EVENT_MSG_DECODE_FAIL = "msg.decode.failure";
    public static final java.lang.String EVENT_MSG_METHOD_INVALID = "msg.method.invalid";
    public static final java.lang.String EVENT_MSG_PATH_INVALID = "msg.path.invalid";
    public static final java.lang.String EVENT_MSG_CONTYPE_INVALID = "msg.content.invalid";
    public static final java.lang.String EVENT_MSG_GROUPID_MISSING = "msg.groupid.missing";
    public static final java.lang.String EVENT_MSG_STREAMID_MISSING = "msg.streamid.missing";
    public static final java.lang.String EVENT_MSG_BODY_MISSING = "msg.body.missing";
    public static final java.lang.String EVENT_MSG_BODY_BLANK = "msg.body.blank";
    public static final java.lang.String EVENT_MSG_BODY_OVERMAX = "msg.body.overmax";
    public static final java.lang.String EVENT_MSG_HB_SUCCESS = "msg.hb.success";
    public static final java.lang.String EVENT_MSG_POST_SUCCESS = "msg.post.success";
    public static final java.lang.String EVENT_MSG_POST_FAILURE = "msg.post.failure";

    public static final java.lang.String EVENT_EMPTY = "socketmsg.empty";
    public static final java.lang.String EVENT_OVERMAXLEN = "socketmsg.overmaxlen";
    public static final java.lang.String EVENT_NOTEQUALLEN = "socketmsg.notequallen";
    public static final java.lang.String EVENT_MSGUNKNOWN_V0 = "socketmsg.unknownV0";
    public static final java.lang.String EVENT_MSGUNKNOWN_V1 = "socketmsg.unknownV1";
    public static final java.lang.String EVENT_MALFORMED = "socketmsg.malformed";
    public static final java.lang.String EVENT_NOBODY = "socketmsg.nobody";
    public static final java.lang.String EVENT_NEGBODY = "socketmsg.negbody";
    public static final java.lang.String EVENT_NEGATTR = "socketmsg.negattr";
    public static final java.lang.String EVENT_INVALIDATTR = "socketmsg.invattr";
    public static final java.lang.String EVENT_UNSUPMSG = "socketmsg.unsupmsg";
    public static final java.lang.String EVENT_UNPRESSEXP = "socketmsg.upressexp";
    public static final java.lang.String EVENT_WITHOUTGROUPID = "socketmsg.wogroupid";
    public static final java.lang.String EVENT_INCONSGROUPORSTREAMID = "socketmsg.inconsids";
    public static final java.lang.String EVENT_CHANNEL_NOT_WRITABLE = "socketch.notwritable";
    public static final java.lang.String EVENT_POST_SUCCESS = "socketmsg.success";
    public static final java.lang.String EVENT_POST_DROPPED = "socketmsg.dropped";
    // http

    public static final java.lang.String EVENT_SINK_NOUID = "sink.nouid";
    public static final java.lang.String EVENT_SINK_NOTOPIC = "sink.notopic";
    public static final java.lang.String EVENT_SINK_NOPRODUCER = "sink.noproducer";
    public static final java.lang.String EVENT_SINK_SENDEXCEPT = "sink.sendexcept";
    public static final java.lang.String EVENT_SINK_FAILRETRY = "sink.retry";
    public static final java.lang.String EVENT_SINK_FAILDROPPED = "sink.dropped";
    public static final java.lang.String EVENT_SINK_SUCCESS = "sink.success";
    public static final java.lang.String EVENT_SINK_RECEIVEEXCEPT = "sink.rcvexcept";

    public static final java.lang.String METASINK_OTHEREXP = "metasink.otherexp";
    public static final java.lang.String METASINK_NOSLAVE = "metasink.noslave";
    public static final java.lang.String METASINK_MSG_NOTOPIC = "metasink.msgnotopic";
    public static final java.lang.String METASINK_PROCESS_SPEED = "metasink.process.speed";
    public static final java.lang.String EVENT_OTHEREXP = "socketmsg.otherexp";
    public static final java.lang.String EVENT_INVALID = "socketmsg.invalid";

    public static final java.lang.String AGENT_MESSAGES_SENT_SUCCESS = "agent.messages.success";
    public static final java.lang.String AGENT_PACKAGES_SENT_SUCCESS = "agent.packages.success";
    public static final java.lang.String MSG_COUNTER_KEY = "msgcnt";
    public static final java.lang.String MSG_PKG_TIME_KEY = "msg.pkg.time";
    public static final java.lang.String AGENT_MESSAGES_COUNT_PREFIX = "agent.messages.count.";
}
