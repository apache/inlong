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

package org.apache.inlong.sort.standalone.sink.http;

import org.apache.inlong.sort.standalone.sink.SinkContext;
import org.apache.inlong.sort.standalone.utils.BufferQueue;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HttpSink extends AbstractSink implements Configurable {

    public static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

    private Context parentContext;
    private BufferQueue<HttpRequest> dispatchQueue;
    private HttpSinkContext context;
    // workers
    private List<HttpChannelWorker> workers = new ArrayList<>();
    // output
    private HttpOutputChannel outputChannel;

    @Override
    public void start() {
        super.start();
        try {
            this.dispatchQueue = SinkContext.createBufferQueue();
            this.context = new HttpSinkContext(getName(), parentContext, getChannel(), dispatchQueue);
            this.context.start();
            for (int i = 0; i < context.getMaxThreads(); i++) {
                HttpChannelWorker worker = new HttpChannelWorker(context, i);
                this.workers.add(worker);
                worker.start();
            }
            this.outputChannel = HttpSinkFactory.createHttpOutputChannel(context);
            this.outputChannel.init();
            this.outputChannel.start();
        } catch (Exception e) {
            LOG.error("Failed to start HttpSink '{}': {}", this.getName(), e.getMessage());
        }
    }

    @Override
    public void stop() {
        super.stop();
        try {
            this.context.close();
            for (HttpChannelWorker worker : this.workers) {
                worker.close();
            }
            this.workers.clear();
            this.outputChannel.close();
        } catch (Exception e) {
            LOG.error("Failed to stop HttpSink '{}': {}", this.getName(), e.getMessage());
        }
    }

    @Override
    public void configure(Context context) {
        LOG.info("Start to configure:{}, context:{}.", this.getName(), context.toString());
        this.parentContext = context;
    }

    @Override
    public Status process() throws EventDeliveryException {
        return Status.BACKOFF;
    }
}
