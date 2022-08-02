/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.sink.zone;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.inlong.dataproxy.sink.kafkazone.KafkaZoneSink;
import org.apache.inlong.dataproxy.sink.pulsarzone.PulsarZoneSink;
import org.apache.inlong.dataproxy.sink.tubezone.TubeZoneSink;
import org.junit.Test;

public class TesZoneSink {

    @Test
    public void testPulsarZoneSinkProcess() {
        PulsarZoneSink sink = new PulsarZoneSink();
        MemoryChannel channel = new MemoryChannel();
        Context context = new Context();
        context.put("type", "org.apache.inlong.dataproxy.sink.pulsarzone.PulsarZoneSinkk");
        sink.setChannel(channel);

        channel.configure(context);
        Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);

        Transaction transaction = channel.getTransaction();
        transaction.begin();
        for (int i = 0; i < 10; i++) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();
    }

    @Test
    public void testTubeZoneSinkProcess() {

        TubeZoneSink sink = new TubeZoneSink();
        MemoryChannel channel = new MemoryChannel();
        Context context = new Context();
        context.put("type", "org.apache.inlong.dataproxy.sink.pulsarzone.TubeZoneSink");
        sink.setChannel(channel);

        channel.configure(context);
        Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);

        Transaction transaction = channel.getTransaction();
        transaction.begin();
        for (int i = 0; i < 10; i++) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();
    }

    @Test
    public void testKafkaZoneSinkProcess() {

        KafkaZoneSink sink = new KafkaZoneSink();
        MemoryChannel channel = new MemoryChannel();
        Context context = new Context();
        context.put("type", "org.apache.inlong.dataproxy.sink.pulsarzone.KafkaZoneSink");
        sink.setChannel(channel);

        channel.configure(context);
        Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);

        Transaction transaction = channel.getTransaction();
        transaction.begin();
        for (int i = 0; i < 10; i++) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();
    }
}
