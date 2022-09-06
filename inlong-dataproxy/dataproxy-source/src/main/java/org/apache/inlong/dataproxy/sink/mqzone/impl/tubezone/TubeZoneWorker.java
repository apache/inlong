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

package org.apache.inlong.dataproxy.sink.mqzone.impl.tubezone;

import org.apache.inlong.dataproxy.sink.mqzone.AbstactZoneWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TubeZoneWorker
 */
public class TubeZoneWorker extends AbstactZoneWorker {

    public static final Logger LOG = LoggerFactory.getLogger(TubeZoneWorker.class);

    /**
     * Constructor
     * 
     * @param sinkName
     * @param workerIndex
     * @param context
     */
    public TubeZoneWorker(String sinkName, int workerIndex, TubeZoneSinkContext context) {
        super(sinkName, workerIndex, context,
                new TubeZoneProducer(sinkName + "-worker-" + workerIndex, context));
    }

    /**
     * run
     */
    @Override
    public void run() {
        LOG.info(String.format("start TubeZoneWorker:%s", super.workerName));
        super.run();
    }
}
