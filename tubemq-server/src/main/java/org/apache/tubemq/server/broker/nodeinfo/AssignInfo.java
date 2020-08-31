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

package org.apache.tubemq.server.broker.nodeinfo;

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.assign.RangeTuple;
import org.apache.tubemq.corebase.assign.RangeType;
import org.apache.tubemq.corebase.assign.TupleOpStatus;
import org.apache.tubemq.corebase.assign.TupleType;
import org.apache.tubemq.corebase.protobuf.generated.ClientBroker;

/***
 * Consumer request's consume task assign info
 */
public class AssignInfo {
    private boolean spAssign = false;
    private long taskId = TBaseConstants.META_VALUE_UNDEFINED;
    private long rebalanceId = TBaseConstants.META_VALUE_UNDEFINED;
    private RangeType rangeType = RangeType.RANGE_SET_UNDEFINED;
    private TupleType tupleType = TupleType.TUPLE_VALUE_TYPE_ALL_OFFSET;
    private RangeTuple origTuple = new RangeTuple();
    private TupleOpStatus rcvStatus = TupleOpStatus.TUPLE_STATUS_UNDEFINED;
    private RangeTuple targetTuple = new RangeTuple();
    private TupleOpStatus opStatus = TupleOpStatus.TUPLE_STATUS_UNDEFINED;
    private long leftOpTime = 0L;
    private long rightOpTime = 0L;

    public AssignInfo() {

    }

    public AssignInfo(ClientBroker.RegisterRequestC2B request) {
        this.spAssign = true;
        long reqOffset = request.hasCurrOffset()
                ? request.getCurrOffset() : TBaseConstants.META_VALUE_UNDEFINED;
        if (reqOffset >= 0) {
            rangeType = RangeType.RANGE_SET_LEFT_DEFINED;
            origTuple.setLeftValue(reqOffset);
            targetTuple.setLeftValue(reqOffset);
            tupleType = TupleType.TUPLE_VALUE_TYPE_ALL_OFFSET;
            opStatus = TupleOpStatus.TUPLE_STATUS_REACHED;
        }
    }

    public AssignInfo(ClientBroker.RegisterV2RequestC2B request) {
        this.spAssign = true;
        ClientBroker.AssignTask assignTask =
                request.hasAssignTask() ? request.getAssignTask() : null;
        if (assignTask == null) {
            return;
        }
        this.taskId = assignTask.getTaskId();
        this.rebalanceId = assignTask.getRebalanceId();
        this.rangeType = RangeType.valueOf(assignTask.getRangeType());
        if (assignTask.hasTupleValueType()) {
            this.tupleType = TupleType.valueOf(assignTask.getTupleValueType());
        }
        if (assignTask.hasLeftValue()) {
            this.origTuple.setLeftValue(assignTask.getLeftValue());
        }
        if (assignTask.hasRightValue()) {
            this.origTuple.setRightValue(assignTask.getRightValue());
        }
        if (assignTask.hasAssignOpStatus()) {
            rcvStatus = TupleOpStatus.valueOf(assignTask.getAssignOpStatus());
        }
    }

    public long getLeftOffset() {
        if (!spAssign || rangeType == RangeType.RANGE_SET_UNDEFINED
                || targetTuple.getLeftValue() == null) {
            return TBaseConstants.META_VALUE_UNDEFINED;
        }
        return targetTuple.getLeftValue();
    }

}
