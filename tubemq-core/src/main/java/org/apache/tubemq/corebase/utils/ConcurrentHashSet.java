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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Map-backed Set.
 *
 * Copied from <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class ConcurrentHashSet<E> extends MapBackedSet<E> {

    private static final long serialVersionUID = 8518578988740277828L;

    public ConcurrentHashSet() {
        super(new ConcurrentHashMap<E, Boolean>());
    }

    public ConcurrentHashSet(Collection<E> c) {
        super(new ConcurrentHashMap<E, Boolean>(), c);
    }

    @Override
    public boolean add(E o) {
        Boolean answer =
                map.putIfAbsent(o, Boolean.TRUE);
        return answer == null;
    }
}
