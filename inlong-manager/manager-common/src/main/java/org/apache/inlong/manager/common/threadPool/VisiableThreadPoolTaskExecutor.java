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

package org.apache.inlong.manager.common.threadPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class VisiableThreadPoolTaskExecutor extends ThreadPoolExecutor {

    private static final Logger logger =
            LoggerFactory.getLogger(VisiableThreadPoolTaskExecutor.class);

    public VisiableThreadPoolTaskExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
            RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    private void showThreadPoolInfo(String prefix) {
        logger.info(
                "Current thread pool class = {}, opType = {}, taskCount = {}, completedTaskCount = {}, activeCount = {}, poolSize = {}, queueSize = {}",
                this.getThreadFactory().getClass(),
                prefix,
                this.getTaskCount(),
                this.getCompletedTaskCount(),
                this.getActiveCount(),
                this.getPoolSize(),
                this.getQueue().size());
    }

    @Override
    public void execute(Runnable task) {
        showThreadPoolInfo("execute");
        super.execute(task);
    }

    @Override
    public Future<?> submit(Runnable task) {
        showThreadPoolInfo("submit");
        return super.submit(task);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        showThreadPoolInfo("submit");
        return super.submit(task);
    }

}
