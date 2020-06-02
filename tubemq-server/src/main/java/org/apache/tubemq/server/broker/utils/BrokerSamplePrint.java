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

package org.apache.tubemq.server.broker.utils;

import java.io.IOException;
import org.apache.tubemq.corebase.utils.AbstractSamplePrint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Compressed print broker's statistics.
 */
public class BrokerSamplePrint extends AbstractSamplePrint {
    private final Logger logger = LoggerFactory.getLogger(BrokerSamplePrint.class);

    public BrokerSamplePrint() {
    }

    public BrokerSamplePrint(long sampleDetailDur, long sampleResetDur,
                             long maxDetailCount, long maxTotalCount) {
        super(sampleDetailDur, sampleResetDur, maxDetailCount, maxTotalCount);
    }

    @Override
    public void printExceptionCaught(Throwable e) {
        if (e != null) {
            if ((e instanceof IOException)) {
                final long now = System.currentTimeMillis();
                final long diffTime = now - lastLogTime.get();
                final long curPrintCnt = totalPrintCount.incrementAndGet();
                if (curPrintCnt < maxTotalCount) {
                    if (diffTime < sampleDetailDur && curPrintCnt < maxDetailCount) {
                        logger.error("[heartbeat failed] heartbeat to master exception", e);
                    } else {
                        logger.error("[heartbeat failed] heartbeat to master exception", e);
                    }
                }
                if (diffTime > sampleResetDur) {
                    if (this.lastLogTime.compareAndSet(now - diffTime, now)) {
                        totalPrintCount.set(0);
                    }
                }
            } else {
                final long curPrintCnt = totalUncheckCount.incrementAndGet();
                if (curPrintCnt < maxUncheckDetailCount) {
                    logger.error("[heartbeat failed] heartbeat to master exception", e);
                } else {
                    logger.error("[heartbeat failed] heartbeat to master exception", e);
                }
            }
        }
    }

    @Override
    public void printExceptionCaught(Throwable e, String hostName, String nodeName) {
        //
    }

}
