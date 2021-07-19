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

package org.apache.inlong.sort.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;

/**
 * Some testing utils.
 */
public class TestingUtils {

    public static class EmptySourceInfo extends SourceInfo {

        private static final long serialVersionUID = 8115618579105217487L;

        public EmptySourceInfo() {
            super(new FieldInfo[0], new EmptyDeserializationInfo());
        }
    }

    public static class EmptyDeserializationInfo implements DeserializationInfo {

        private static final long serialVersionUID = -368432518671681334L;
    }

    public static class EmptySinkInfo extends SinkInfo {

        private static final long serialVersionUID = 14549089978580241L;

        public EmptySinkInfo() {
            super(new FieldInfo[0]);
        }
    }

    public static class TestingSourceInfo extends SourceInfo {

        public TestingSourceInfo(FieldInfo[] fields) {
            super(fields, new EmptyDeserializationInfo());
        }
    }

    public static class TestingSinkInfo extends SinkInfo {

        private static final long serialVersionUID = 1188072353957179645L;

        public TestingSinkInfo(FieldInfo[] fields) {
            super(fields);
        }
    }

    public static class TestingCollector<T> implements Collector<T> {

        public final List<T> results = new ArrayList<>();

        @Override
        public void collect(T t) {
            results.add(t);
        }

        @Override
        public void close() {

        }
    }
}
