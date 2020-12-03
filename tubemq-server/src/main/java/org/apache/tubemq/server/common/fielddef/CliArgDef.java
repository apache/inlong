/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.common.fielddef;



public enum CliArgDef {

    // Note: Due to compatibility considerations,
    //      the defined fields in the scheme are forbidden to be modified,
    //      only new fields can be added

    HELP("h", "help", "Print usage information."),
    VERSION("v", "version", "Display TubeMQ version."),
    MASTERSERVER("master-servers", "master-servers",
            "String: format is master1_ip:port[,master2_ip:port]",
            "The master address(es) to connect to."),
    MASTERURL("master-url", "master-url",
            "String: format is http://master_ip:master_webport/",
            "Master Service URL to which to connect.(default: http://localhost:8080/)"),
    BROKERURL("broker-url", "broker-url",
            "String: format is http://broker_ip:broker_webport/",
            "Broker Service URL to which to connect.(default: http://localhost:8081/)"),
    MESSAGES("messages", "messages",
            "Long: count",
            "The number of messages to send or consume, If not set, production or consumption is continual."),
    MSGDATASIZE("msg-data-size", "message-data-size",
            "Int: message size",
            "message's data size in bytes. Note that you must provide exactly"
                    + " one of --msg-data-size or --payload-file."),
    PAYLOADFILE("payload-file", "payload-file",
            "String: payload file path",
            "file to read the message payloads from. This works only for"
                    + " UTF-8 encoded text files. Payloads will be read from this"
                    + " file and a payload will be randomly selected when sending"
                    + " messages. Note that you must provide exactly one"
                    + " of --msg-data-size or --payload-file."),
    PAYLOADDELIM("payload-delimiter", "payload-delimiter",
            "String: payload data's delimiter",
            "provides delimiter to be used when --payload-file is provided."
                    + " Defaults to new line. Note that this parameter will be"
                    + " ignored if --payload-file is not provided. (default: \\n)"),
    PRDTOPIC("topic", "topicName",
            "String: topic, format is topic_1[,topic_2[:filterCond_2.1[;filterCond_2.2]]]",
            "The topic(s) to produce messages to."),
    CNSTOPIC("topic", "topicName",
            "String: topic, format is topic_1[[:filterCond_1.1[;filterCond_1.2]][,topic_2]]",
            "The topic(s) to consume on."),
    RPCTIMEOUT("timeout", "timeout",
            "Long: milliseconds",
            "The maximum duration between request and response in milliseconds. (default: 10000)"),
    GROUP("group", "groupName",
            "String: consumer group",
            "The consumer group name of the consumer."),
    CLIENTCOUNT("client-num", "client-num",
            "Int: client count",
            "Number of consumers to started."),
    PULLMODEL("pull-model", "pull-model",
            "Pull consumption model."),
    PUSHMODEL("push-model", "push-model",
            "Push consumption model."),
    FETCHTHREADS("num-fetch-threads", "num-fetch-threads",
            "Integer: count",
            "Number of fetch threads, default: num of cpu count."),
    FROMLATEST("from-latest", "from-latest",
            "Start to consume from the latest message present in the log."),
    FROMBEGINNING("from-beginning", "from-beginning",
            "If the consumer does not already have an established offset to consume from,"
                    + " start with the earliest message present in the log rather than the latest message."),
    OUTPUTINTERVAL("output-interval", "output-interval",
            "Integer: interval_ms",
            "Interval in milliseconds at which to print progress info. (default: 5000)");


    CliArgDef(String opt, String longOpt, String optDesc) {
        this(opt, longOpt, false, "", optDesc);
    }

    CliArgDef(String opt, String longOpt, String argDesc, String optDesc) {
        this(opt, longOpt, true, argDesc, optDesc);
    }

    CliArgDef(String opt, String longOpt, boolean hasArg, String argDesc, String optDesc) {
        this.opt = opt;
        this.longOpt = longOpt;
        this.hasArg = hasArg;
        this.argDesc = argDesc;
        this.optDesc = optDesc;
    }

    public final String opt;
    public final String longOpt;
    public final boolean hasArg;
    public final String argDesc;
    public final String optDesc;
}
