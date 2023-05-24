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

    public static final java.lang.String EVENT_SERVICE_CLOSED = "source.srvclosed";
    public static final java.lang.String EVENT_SERVICE_UNREADY = "sink.unready";
    public static final java.lang.String EVENT_VISITIP_ILLEGAL = "links.illegal";
    public static final java.lang.String EVENT_NOTOPIC = "config.notopic";

    public static final java.lang.String METASINK_SUCCESS = "metasink.success";
    public static final java.lang.String METASINK_DROPPED = "metasink.dropped";
    public static final java.lang.String METASINK_RETRY = "metasink.retry";
    public static final java.lang.String METASINK_OTHEREXP = "metasink.otherexp";
    public static final java.lang.String METASINK_NOTOPIC = "metasink.notopic";
    public static final java.lang.String METASINK_NOSLAVE = "metasink.noslave";
    public static final java.lang.String METASINK_MSG_NOTOPIC = "metasink.msgnotopic";
    public static final java.lang.String METASINK_PROCESS_SPEED = "metasink.process.speed";
    public static final java.lang.String EVENT_OTHEREXP = "socketmsg.otherexp";
    public static final java.lang.String EVENT_INVALID = "socketmsg.invalid";
    // source
    public static final java.lang.String EVENT_LINKS_OVERMAX = "links.overmax";
    public static final java.lang.String EVENT_LINKS_IN = "links.linkin";
    public static final java.lang.String EVENT_LINKS_OUT = "links.linkout";
    public static final java.lang.String EVENT_LINKS_EXCEPTION = "links.exception";
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
    public static final java.lang.String EVENT_HTTP_DECFAIL = "httpmsg.decfailure";
    public static final java.lang.String EVENT_HTTP_INVALIDMETHOD = "httpmsg.invmethod";
    public static final java.lang.String EVENT_HTTP_BLANKURI = "httpmsg.blankuri";
    public static final java.lang.String EVENT_HTTP_URIDECFAIL = "httpmsg.decurifail";
    public static final java.lang.String EVENT_HTTP_INVALIDURI = "httpmsg.invuri";
    public static final java.lang.String EVENT_HTTP_ILLEGAL_VISIT = "httpmsg.illegal";
    public static final java.lang.String EVENT_HTTP_HB_SUCCESS = "httphb.success";
    public static final java.lang.String EVENT_HTTP_WITHOUTGROUPID = "httpmsg.wogroupid";
    public static final java.lang.String EVENT_HTTP_WITHOUTSTREAMID = "httpmsg.wostreamid";
    public static final java.lang.String EVENT_HTTP_NOBODY = "httpmsg.nobody";
    public static final java.lang.String EVENT_HTTP_EMPTYBODY = "httpmsg.emptybody";
    public static final java.lang.String EVENT_HTTP_BODYOVERMAXLEN = "httpmsg.bodyovermax";
    public static final java.lang.String EVENT_HTTP_POST_SUCCESS = "httpmsg.success";
    public static final java.lang.String EVENT_HTTP_POST_DROPPED = "httpmsg.dropped";

    public static final java.lang.String AGENT_MESSAGES_SENT_SUCCESS = "agent.messages.success";
    public static final java.lang.String AGENT_PACKAGES_SENT_SUCCESS = "agent.packages.success";
    public static final java.lang.String MSG_COUNTER_KEY = "msgcnt";
    public static final java.lang.String MSG_PKG_TIME_KEY = "msg.pkg.time";
    public static final java.lang.String AGENT_MESSAGES_COUNT_PREFIX = "agent.messages.count.";
}
