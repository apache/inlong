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

package org.apache.inlong.agent.plugin.sources;

import java.util.List;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.Reader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKafkaReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaReader.class);

    @Test
    public void testKafkaReader() {
        KafkaSource kafkaSource = new KafkaSource();
        JobProfile conf = JobProfile.parseJsonStr("{}");
        conf.set("job.kafkaJob.topic", "test3");
        conf.set("job.kafkaJob.bootstrap.servers", "127.0.0.1:9092");
        conf.set("job.kafkaJob.group.id", "test_group1");
        conf.set("job.kafkaJob.recordspeed.limit", "1");
        conf.set("job.kafkaJob.bytespeed.limit", "1");
        conf.set("job.kafkaJob.partition.offset", "0#0");
        conf.set("job.kafkaJob.autoOffsetReset", "latest");
        conf.set("proxy.inlongGroupId", "");
        conf.set("proxy.inlongStreamId", "");

        try {
            List<Reader> readers = kafkaSource.split(conf);
            LOGGER.info("total readers by split after:{}", readers.size());
            readers.forEach(reader -> {
                reader.init(conf);
                Runnable runnable = () -> {
                    while (!reader.isFinished()) {
                        Message msg = reader.read();
                        if (msg != null) {
                            LOGGER.info(new String(msg.getBody()));
                        }
                    }
                    LOGGER.info("reader is finished!");
                };
                // start thread
                new Thread(runnable).start();
            });
        } catch (Exception e) {
            LOGGER.error("get record failed:", e);
        }
    }
}
