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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatRunner implements Runnable {

    private static final Logger logger = LoggerFactory
            .getLogger(StatRunner.class);
    private boolean shutDownFlag = false;
    private String name;
    private CounterGroup counterGroup;
    private CounterGroupExt counterGroupExt;
    private int intervalSec;
    private Set<String> moniterNames;

    public StatRunner(String name, CounterGroup counterGroup, CounterGroupExt counterGroupExt,
            int intervalSec,
            Set<String> moniterNames) {
        this.counterGroup = counterGroup;
        this.counterGroupExt = counterGroupExt;
        this.intervalSec = intervalSec;
        this.moniterNames = moniterNames;
        this.name = name;
    }

    public void run() {
        // TODO Auto-generated method stub
        HashMap<String, Long> counters = new HashMap<String, Long>();
        HashMap<String, Long> counterExt = new HashMap<String, Long>();

        while (!shutDownFlag) {
            try {
                Thread.sleep(intervalSec * 1000);

                for (String str : moniterNames) {
                    long cnt = 0;
                    synchronized (counterGroup) {
                        cnt = counterGroup.get(str);
                        counterGroup.set(str, 0L);
                        counters.put(str, cnt);
                    }
                }

                for (String str : moniterNames) {
                    long cnt = counters.get(str);
                    logger.info("{}.{}={}", new Object[]{name, str, cnt});
                }

                counters.clear();

                synchronized (counterGroupExt) {
                    for (String str : counterGroupExt.getCounters().keySet()) {
                        counterExt.put(str, counterGroupExt.get(str));
                    }
                    counterGroupExt.clear();
                }

                for (Map.Entry<String, Long> entrys : counterExt.entrySet()) {
                    logger.info(
                            "{}.{}={}",
                            new Object[]{name, entrys.getKey(),
                                    entrys.getValue()});
                }
                counterExt.clear();

            } catch (Exception e) {
                // TODO Auto-generated catch block
                logger.warn("statrunner interrupted");
            }
        }
    }

    public void shutDown() {
        this.shutDownFlag = true;
    }

}
