/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.sink.mqzone.impl.tubezone;

import java.util.ArrayList;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.inlong.dataproxy.dispatch.DispatchProfile;
import org.apache.inlong.dataproxy.sink.mqzone.AbstractZoneSinkContext;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 
 * TubeZoneSinkContext
 */
public class TubeZoneSinkContext extends AbstractZoneSinkContext {
    /**
     * Constructor
     * 
     * @param context
     */
    public TubeZoneSinkContext(String sinkName, Context context, Channel channel,
                               ArrayList<LinkedBlockingQueue<DispatchProfile>> dispatchQueues) {
        super(sinkName, context, channel, dispatchQueues);
    }

}
