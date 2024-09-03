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

package org.apache.inlong.common.util;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ListUtil {

    /**
     * Return a list that elements in list1 but not in list2 by a specified key
     * @param minuend list to subtract from
     * @param subtrahend list to subtract
     * @param keyExtractor Key extract function
     * @return List of elements in list1 but not in list 2 by a specified key
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> subtract(List<T> minuend, List<T> subtrahend, Function<T, R> keyExtractor) {
        if (CollectionUtils.isEmpty(subtrahend) || CollectionUtils.isEmpty(minuend)) {
            return minuend;
        }

        Map<R, T> minuMap = toMap(minuend, keyExtractor);
        Map<R, T> substraMap = toMap(subtrahend, keyExtractor);
        substraMap.forEach((k, v) -> minuMap.remove(k));
        return new ArrayList<>(minuMap.values());
    }

    /**
     * Return a list of elements in both list1 and list2 by a specified key
     * @param list1
     * @param list2
     * @param keyExtractor Key extract function
     * @return List of elements in both list1 and list2 by a specified key
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> intersection(List<T> list1, List<T> list2, Function<T, R> keyExtractor) {
        if (CollectionUtils.isEmpty(list1) || CollectionUtils.isEmpty(list2)) {
            return new ArrayList<>();
        }

        Map<R, T> map1 = toMap(list1, keyExtractor);
        Map<R, T> map2 = toMap(list2, keyExtractor);
        return map2.entrySet().stream()
                .filter(entry -> map1.containsKey(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    /**
     * Return a list of key in both list1 and list2
     * @param list1
     * @param list2
     * @param keyExtractor Key extract function
     * @return List of key in both list1 and list2
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<R> intersectionKey(List<T> list1, List<T> list2, Function<T, R> keyExtractor) {
        if (CollectionUtils.isEmpty(list1) || CollectionUtils.isEmpty(list2)) {
            return new ArrayList<>();
        }

        Map<R, T> map1 = toMap(list1, keyExtractor);
        Map<R, T> map2 = toMap(list2, keyExtractor);
        return map2.keySet().stream()
                .filter(map1::containsKey)
                .collect(Collectors.toList());
    }

    /**
     * Returns a union list of two lists
     * @param list1
     * @param list2
     * @return A union list of two lists
     * @param <T> Elements type
     */
    public static <T> List<T> union(List<T> list1, List<T> list2) {
        final ArrayList<T> result = new ArrayList<>(list1);
        result.addAll(list2);
        return result;
    }

    /**
     * Convert a list to map
     * @param list
     * @param keyExtractor Map key extractor
     * @return Map
     * @param <T> Elements type
     * @param <R> Key type
     */
    public static <T, R> Map<R, T> toMap(List<T> list, Function<T, R> keyExtractor) {
        if (CollectionUtils.isEmpty(list)) {
            return new HashMap<>();
        }
        return list.stream().collect(Collectors.toMap(keyExtractor, v -> v));
    }
}
