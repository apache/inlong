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

package org.apache.inlong.agent.constant;

/**
 * Basic config for a single job
 */
public class JobConstants extends CommonConstants {

    // job id
    public static final String JOB_ID = "job.id";
    public static final String JOB_INSTANCE_ID = "job.instance.id";
    public static final String JOB_GROUP_ID = "job.groupId";
    public static final String JOB_STREAM_ID = "job.streamId";

    public static final String JOB_SOURCE_CLASS = "job.source";
    public static final String JOB_CHANNEL = "job.channel";

    // sink config
    public static final String JOB_SINK = "job.sink";
    public static final String JOB_MQ_ClUSTERS = "job.mqClusters";
    public static final String JOB_MQ_TOPIC = "job.topicInfo";
}
