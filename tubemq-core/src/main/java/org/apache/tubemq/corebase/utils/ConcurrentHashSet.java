/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.utils;


import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ConcurrentHashSet, construct the set collection through ConcurrentHashMap
 *  to complete the operation management of the concurrent set
 */
public class ConcurrentHashSet<E> implements Iterable<E> {

    private final ConcurrentHashMap<E, Long> keyValMap
            = new ConcurrentHashMap<>();

    public ConcurrentHashSet() {

    }

    public boolean add(E key) {
        Long value =
                keyValMap.putIfAbsent(key, System.currentTimeMillis());
        return (value == null);
    }

    public boolean contains(E key) {
        return keyValMap.containsKey(key);
    }

    public boolean remove(E key) {
        return keyValMap.remove(key) != null;
    }

    public void clear() {
        keyValMap.clear();
    }

    public int size() {
        return keyValMap.size();
    }

    public boolean isEmpty() {
        return keyValMap.isEmpty();
    }

    public Long getKeyAddTime(E key) {
        return keyValMap.get(key);
    }

    @Override
    public Iterator<E> iterator() {
        return new HashSet<>(keyValMap.keySet()).iterator();
    }
}
