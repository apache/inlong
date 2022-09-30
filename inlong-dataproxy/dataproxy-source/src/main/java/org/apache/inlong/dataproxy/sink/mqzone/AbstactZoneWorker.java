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

package org.apache.inlong.dataproxy.sink.mqzone;

import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstactZoneWorker extends Thread {
    public static final Logger LOG = LoggerFactory.getLogger(AbstactZoneWorker.class);

    protected final String workerName;
    protected final AbstractZoneSinkContext context;

    protected AbstractZoneProducer zoneProducer;
    protected LifecycleState status;

    protected int workerIndex;

    /**
     * Constructor
     *
     * @param sinkName
     * @param workerIndex
     * @param context
     */
    public AbstactZoneWorker(String sinkName, int workerIndex, AbstractZoneSinkContext context,
                             AbstractZoneProducer zoneProducer) {
        super();
        this.workerName = sinkName + "-worker-" + workerIndex;
        this.workerIndex = workerIndex;
        this.context = context;
        this.zoneProducer = zoneProducer;
        this.status = LifecycleState.IDLE;
    }

    /**
     * start
     */
    @Override
    public void start() {
        this.zoneProducer.start();
        this.status = LifecycleState.START;
        super.start();
    }

    /**
     *
     * close
     */
    public void close() {
        // close all producers
        this.zoneProducer.close();
        this.status = LifecycleState.STOP;
    }

    /**
     * run
     */
    @Override
    public void run() {
        while (status != LifecycleState.STOP) {
            try {
                DispatchProfile event = context.getDispatchQueues().get(workerIndex).poll();
                if (event == null) {
                    this.sleepOneInterval();
                    continue;
                }
                // metric
                context.addSendMetric(event, workerName);
                // send
                this.zoneProducer.send(event);
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
                this.sleepOneInterval();
            }
        }
    }

    /**
     * sleepOneInterval
     */
    private void sleepOneInterval() {
        try {
            Thread.sleep(context.getProcessInterval());
        } catch (InterruptedException e1) {
            LOG.error(e1.getMessage(), e1);
        }
    }

}
