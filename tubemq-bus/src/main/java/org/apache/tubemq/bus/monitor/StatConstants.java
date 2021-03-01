/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tubemq.bus.monitor;

public class StatConstants {

    public static final String METASINK_SUCCESS = "metasink.success";
    public static final String METASINK_DROPPED = "metasink.dropped";
    public static final String METASINK_RETRY = "metasink.retry";
    public static final String METASINK_OTHEREXP = "metasink.otherexp";
    public static final String METASINK_NOTOPIC = "metasink.notopic";
    public static final String METASINK_NOSLAVE = "metasink.noslave";
    public static final String METASINK_MSG_NOTOPIC = "metasink.msgnotopic";
    public static final String METASINK_PROCESS_SPEED = "metasink.process.speed";

    public static final String EVENT_SUCCESS = "socketmsg.success";
    public static final String EVENT_DROPPED = "socketmsg.dropped";
    public static final String EVENT_EMPTY = "socketmsg.empty";
    public static final String EVENT_OTHEREXP = "socketmsg.otherexp";
    public static final String EVENT_INVALID = "socketmsg.invalid";

    public static final String AGENT_MESSAGES_SENT_SUCCESS = "agent.messages.success";
    public static final String AGENT_PACKAGES_SENT_SUCCESS = "agent.packages.success";
    //tdmsg counter in one event package
    public static final String MSG_COUNTER_KEY = "msgcnt";
    public static final String MSG_PKG_TIME_KEY = "msg.pkg.time";
    public static final String AGENT_MESSAGES_COUNT_PREFIX = "agent.messages.count.";
}
