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

package org.apache.inlong.agent.plugin;

/**
 * @Author pengzirui
 * @Date 2021/10/14 2:18 下午
 * @Version 1.0
 */
public interface MessageFilter {

    /**
     * split a message to get tid string
     * used when the file is separated with different tid
     * @param message the input message
     * @param fieldSplitter fieldSplitter used when split a line
     * @return
     */
    String filterTid(Message message, byte[] fieldSplitter);
}
