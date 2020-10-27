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

package org.apache.tubemq.manager.service;

import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.tubemq.manager.entry.TopicEntry;

/**
 * topic business with future.
 */
public class TopicFuture {
    @Getter
    private int retryTime = 0;
    @Getter
    private final TopicEntry entry;
    @Getter
    private final CompletableFuture<TopicEntry> future;

    public TopicFuture(TopicEntry entry, CompletableFuture<TopicEntry> future) {
        this.entry = entry;
        this.future = future;
    }

    /**
     * record retry time.
     */
    public void increaseRetryTime() {
        retryTime += 1;
    }

    /**
     * when topic operation finished, complete it.
     */
    public void complete() {
        this.future.complete(this.entry);
    }

    public void completeExceptional() {
        this.future.completeExceptionally(new RuntimeException("exceed max retry "
                + retryTime +" adding"));
    }
}
