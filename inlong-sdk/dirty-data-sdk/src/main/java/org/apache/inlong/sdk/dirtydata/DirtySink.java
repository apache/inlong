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

package org.apache.inlong.sdk.dirtydata;

import org.apache.inlong.sdk.dirtydata.sink.Configure;

import java.io.Serializable;

/**
 * The dirty sink base inteface
 *
 */
public interface DirtySink extends Serializable {

    /**
     * Open for dirty sink
     *
     * @param configuration The configuration that is used for dirty sink
     * @throws Exception The exception may be thrown when executing
     */
    default void open(Configure configuration) throws Exception {

    }

    /**
     * Invoke that is used to sink dirty data
     *
     * @param dirtyData The dirty data that will be written
     * @throws Exception The exception may be thrown when executing
     */
    void invoke(DirtyData dirtyData) throws Exception;

    /**
     * Close for dirty sink
     *
     * @throws Exception The exception may be thrown when executing
     */
    default void close() throws Exception {

    }

}
