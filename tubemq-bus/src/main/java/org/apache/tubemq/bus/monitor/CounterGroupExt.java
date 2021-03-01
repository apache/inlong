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
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for counting events, collecting metrics, etc.
 */
public class CounterGroupExt {

    private static final Logger logger = LoggerFactory
            .getLogger(CounterGroupExt.class);
    private String name;
    private HashMap<String, AtomicLong> counters;
    private int maxCnt = 100000;

    public CounterGroupExt() {
        this.counters = new HashMap<String, AtomicLong>();
    }

    public CounterGroupExt(int maxCnt) {
        this.counters = new HashMap<String, AtomicLong>();
        this.maxCnt = maxCnt;
    }

    public synchronized Long get(String name) {
        return getCounter(name).get();
    }

    public synchronized Long incrementAndGet(String name) {
        return getCounter(name).incrementAndGet();
    }

    public synchronized Long addAndGet(String name, Long delta) {
        if (counters.size() < this.maxCnt) {
            return getCounter(name).addAndGet(delta);
        } else {
            //logger.error("counters is full and size={}", counters.size());
            return 0L;
        }
    }

    public synchronized void setValue(String name, Long newValue) {
        if (counters.size() < this.maxCnt) {
            getCounter(name).set(newValue);
        } else {
            //logger.error("counters is full and size={}", counters.size());
        }
    }

    public synchronized void add(CounterGroupExt counterGroup) {
        synchronized (counterGroup) {
            for (Entry<String, AtomicLong> entry : counterGroup.getCounters()
                    .entrySet()) {

                addAndGet(entry.getKey(), entry.getValue().get());
            }
        }
    }

    public synchronized void set(String name, Long value) {
        getCounter(name).set(value);
    }

    public synchronized AtomicLong getCounter(String name) {
        if (!counters.containsKey(name)) {
            counters.put(name, new AtomicLong());
        }

        return counters.get(name);
    }

    public synchronized void del(String name) {
        counters.remove(name);
    }

    public synchronized void clear() {
        counters.clear();
    }

    @Override
    public synchronized String toString() {
        return "{ name:" + name + " counters:" + counters + " }";
    }

    public synchronized String getName() {
        return name;
    }

    public synchronized void setName(String name) {
        this.name = name;
    }

    public synchronized HashMap<String, AtomicLong> getCounters() {
        return counters;
    }

    public synchronized void setCounters(HashMap<String, AtomicLong> counters) {
        this.counters = counters;
    }

}