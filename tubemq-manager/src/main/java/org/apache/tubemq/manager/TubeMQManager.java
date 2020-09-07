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

package org.apache.tubemq.manager;

import org.apache.tubemq.manager.backend.AbstractDaemon;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TubeMQManager extends AbstractDaemon {
    public static void main(String[] args) throws Exception {
        TubeMQManager manager = new TubeMQManager();
        manager.startThreads();
        SpringApplication.run(TubeMQManager.class);
        // web application stopped, then stop working threads.
        manager.stopThreads();
        manager.join();
    }

    @Override
    public void startThreads() throws Exception {

    }

    @Override
    public void stopThreads() throws Exception {

    }
}
