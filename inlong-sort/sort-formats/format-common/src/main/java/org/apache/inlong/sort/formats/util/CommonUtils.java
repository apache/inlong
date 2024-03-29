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

package org.apache.inlong.sort.formats.util;

import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Common utils.
 */
public class CommonUtils {

    /**
     * A simple collector which collects elements into a list.
     * @param <T> Type of elements to collect.
     */
    public static class SimpleListCollector<T> implements Collector<T> {

        private List<T> elements = new ArrayList<>();

        @Override
        public void collect(T element) {
            elements.add(element);
        }

        @Override
        public void close() {

        }

        public List<T> getElements() {
            return elements;
        }
    }
}
