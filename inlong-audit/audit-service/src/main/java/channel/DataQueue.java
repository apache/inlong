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

package channel;

import entities.StatData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Data queue. use in source and sink.
 */
public class DataQueue {

    private static final Logger LOG = LoggerFactory.getLogger(DataQueue.class);

    private final LinkedBlockingQueue<StatData> queue;

    public DataQueue(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
    }

    /**
     * Push data
     *
     * @param statDataPo
     */
    public void push(StatData statDataPo) {
        try {
            queue.put(statDataPo);
        } catch (InterruptedException ex) {
            LOG.error(ex.getMessage());
        }
    }

    /**
     * Pull data
     *
     * @param timeout
     * @param unit
     * @return
     */
    public StatData pull(long timeout, TimeUnit unit) {
        StatData message = null;
        try {
            message = queue.poll(timeout, unit);
        } catch (InterruptedException ex) {
            LOG.error(ex.getMessage());
        }
        return message;
    }

    /**
     * destroy
     */
    public void destroy() {
        if (queue != null) {
            queue.clear();
        }
        LOG.info("destroy channel!");
    }
}
