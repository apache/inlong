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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SortConfigUtil {

    /**
     * Check delete elements
     * @param last Last element list
     * @param current Current element list
     * @param keyExtractor Map key extractor
     * @return List of elements in last but not in current
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> checkDelete(List<T> last, List<T> current, Function<T, R> keyExtractor) {
        return ListUtil.subtract(last, current, keyExtractor);
    }

    /**
     * Check new elements
     * @param last Last element list
     * @param current Current element list
     * @param keyExtractor Map key extractor
     * @return List of elements in current but not in last
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> checkNew(List<T> last, List<T> current, Function<T, R> keyExtractor) {
        return ListUtil.subtract(current, last, keyExtractor);
    }

    /**
     * Check and return list of elements which have been updated by compare the specified key
     *
     * @param last Last elements list
     * @param current Current elements list
     * @param keyExtractor Map key extract function
     * @param versionExtractor Compare key extractor
     * @return List of elements which have been updated by compare the specified key
     * @param <T> Element type
     * @param <R> Map key type
     * @param <N> Compare key type
     */
    public static <T, R, N extends Comparable<? super N>> List<T> checkUpdate(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            Function<T, N> versionExtractor) {

        List<R> intersection = ListUtil.intersectionKey(last, current, keyExtractor);
        Map<R, T> lastMap = ListUtil.toMap(last, keyExtractor);
        Map<R, T> currentMap = ListUtil.toMap(current, keyExtractor);

        return intersection.stream()
                .map(key -> {
                    T lastElement = lastMap.get(key);
                    T currentElement = currentMap.get(key);
                    Comparator<T> comparator = Comparator.comparing(versionExtractor);
                    return comparator.compare(lastElement, currentElement) < 0 ? currentElement : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Check and return list of elements which are the latest version by compare the version
     *
     * @param last Last elements list
     * @param current Current elements list
     * @param keyExtractor Map key extract function
     * @param versionExtractor Compare key extractor
     * @return List of elements which are the latest ones
     * @param <T> Element type
     * @param <R> Map key type
     * @param <N> Compare key type
     */
    public static <T, R, N extends Comparable<? super N>> List<T> checkLatest(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            Function<T, N> versionExtractor) {

        return Stream.of(checkUpdate(last, current, keyExtractor, versionExtractor),
                checkNoUpdate(last, current, keyExtractor, versionExtractor),
                checkNew(last, current, keyExtractor))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * Check and return list of elements which have not been updated by compare the specified key
     *
     * @param last Last elements list
     * @param current Current elements list
     * @param keyExtractor Map key extract function
     * @param versionExtractor Compare key extractor
     * @return list of elements which have not been updated by compare the specified key
     * @param <T> Element type
     * @param <R> Map key type
     * @param <N> Compare key type
     */
    public static <T, R, N extends Comparable<? super N>> List<T> checkNoUpdate(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            Function<T, N> versionExtractor) {

        List<R> intersection = ListUtil.intersectionKey(last, current, keyExtractor);
        Map<R, T> lastMap = ListUtil.toMap(last, keyExtractor);
        Map<R, T> currentMap = ListUtil.toMap(current, keyExtractor);

        return intersection.stream()
                .map(key -> {
                    T lastElement = lastMap.get(key);
                    T currentElement = currentMap.get(key);
                    Comparator<T> comparator = Comparator.comparing(versionExtractor);
                    return comparator.compare(lastElement, currentElement) >= 0 ? lastElement : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Batch check and return a list of deleted elements by given delete check function
     * @param last Elements of last check
     * @param current Elements of current
     * @param keyExtractor Key extractor of elements
     * @param innerCheckDeleteFunction The single element delete check function
     * @return A list of deleted elements
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> batchCheckDeleteRecursive(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            BiFunction<T, T, T> innerCheckDeleteFunction) {
        if (CollectionUtils.isEmpty(last)) {
            return null;
        }
        if (CollectionUtils.isEmpty(current)) {
            return last;
        }

        List<T> deleteByKey = checkDelete(last, current, keyExtractor);
        List<T> deleteInner = batchCheckRecursive(last, current, keyExtractor, innerCheckDeleteFunction);
        return ListUtil.union(deleteInner, deleteByKey);
    }

    /**
     * Batch check and return a list of update elements by given update function
     * @param last Elements of last check
     * @param current Elements of current
     * @param keyExtractor Key extractor of elements
     * @param innerCheckUpdateFunction The single element update check function
     * @return A list of update elements
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> batchCheckUpdateRecursive(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            BiFunction<T, T, T> innerCheckUpdateFunction) {
        List<R> intersectionKey = ListUtil.intersectionKey(last, current, keyExtractor);
        if (CollectionUtils.isEmpty(intersectionKey)) {
            return null;
        }

        return batchCheckRecursive(last, current, keyExtractor, innerCheckUpdateFunction);
    }

    /**
     * Batch check and return a list of not update elements by given no update function
     * @param last Elements of last check
     * @param current Elements of current
     * @param keyExtractor Key extractor of elements
     * @param innerCheckNoUpdateFunction The single element no update check function
     * @return A list of no update elements
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> batchCheckNoUpdateRecursive(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            BiFunction<T, T, T> innerCheckNoUpdateFunction) {
        List<R> intersectionKey = ListUtil.intersectionKey(last, current, keyExtractor);
        if (CollectionUtils.isEmpty(intersectionKey)) {
            return null;
        }

        return batchCheckRecursive(last, current, keyExtractor, innerCheckNoUpdateFunction);
    }

    /**
     * Batch check and return a list of new elements by given delete check function
     * @param last Elements of last check
     * @param current Elements of current
     * @param keyExtractor Key extractor of elements
     * @param innerCheckNewFunction The single element new check function
     * @return A list of new elements
     * @param <T> Element type
     * @param <R> Key type
     */
    public static <T, R> List<T> batchCheckNewRecursive(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            BiFunction<T, T, T> innerCheckNewFunction) {
        if (CollectionUtils.isEmpty(last)) {
            return current;
        }
        if (CollectionUtils.isEmpty(current)) {
            return null;
        }

        List<T> newByKey = checkNew(last, current, keyExtractor);
        List<T> newInner = batchCheckRecursive(last, current, keyExtractor, innerCheckNewFunction);
        return ListUtil.union(newInner, newByKey);
    }

    public static <T, R, N extends Comparable<? super N>> List<T> batchCheckLatestRecursive(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            BiFunction<T, T, T> singleCheckLatestFunction) {
        if (CollectionUtils.isEmpty(last)) {
            return current;
        }
        if (CollectionUtils.isEmpty(current)) {
            return null;
        }

        List<T> newByKey = checkNew(current, last, keyExtractor);
        List<T> latestInner = batchCheckRecursive(last, current, keyExtractor, singleCheckLatestFunction);
        return ListUtil.union(latestInner, newByKey);
    }

    /**
     * Batch check and return a list of elements base on a specified check function
     * @param last Elements of last check
     * @param current Elements of current
     * @param keyExtractor Key extractor of elements
     * @param singleCheckFunction The single element check function
     * @return A list of elements
     * @param <T> Element type
     * @param <R> Key type
     */
    private static <T, R> List<T> batchCheckRecursive(
            List<T> last, List<T> current,
            Function<T, R> keyExtractor,
            BiFunction<T, T, T> singleCheckFunction) {

        List<R> intersectionKey = ListUtil.intersectionKey(last, current, keyExtractor);
        if (CollectionUtils.isEmpty(intersectionKey)) {
            return null;
        }

        Map<R, T> lastMap = ListUtil.toMap(last, keyExtractor);
        Map<R, T> currentMap = ListUtil.toMap(current, keyExtractor);
        return intersectionKey.stream()
                .map(tag -> singleCheckFunction.apply(lastMap.get(tag), currentMap.get(tag)))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
