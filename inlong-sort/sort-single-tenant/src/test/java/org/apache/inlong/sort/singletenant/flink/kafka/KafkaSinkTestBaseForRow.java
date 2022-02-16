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

package org.apache.inlong.sort.singletenant.flink.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.KafkaSinkInfo;

import java.util.HashMap;

import static org.apache.inlong.sort.singletenant.flink.kafka.KafkaSinkBuilder.buildKafkaSink;

public abstract class KafkaSinkTestBaseForRow extends KafkaSinkTestBase<Row> {

    @Override
    protected void buildJob(StreamExecutionEnvironment env, TestingSource testingSource) {
        env.addSource(testingSource).addSink(
                buildKafkaSink(
                        new KafkaSinkInfo(new FieldInfo[]{}, brokerConnStr, topic, null),
                        new HashMap<>(),
                        serializationSchema,
                        new Configuration()
                )
        );
    }

}
