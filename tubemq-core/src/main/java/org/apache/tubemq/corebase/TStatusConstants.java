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

package org.apache.tubemq.corebase;

public interface TStatusConstants {
    int STATUS_MANAGE_NOT_DEFINED = -2;
    int STATUS_MANAGE_APPLY = 1;
    int STATUS_MANAGE_ONLINE = 5;
    int STATUS_MANAGE_ONLINE_NOT_WRITE = 6;
    int STATUS_MANAGE_ONLINE_NOT_READ = 7;
    int STATUS_MANAGE_OFFLINE = 9;

    int STATUS_SERVICE_UNDEFINED = -2;
    int STATUS_SERVICE_TOONLINE_WAIT_REGISTER = 31;
    int STATUS_SERVICE_TOONLINE_PART_WAIT_REGISTER = 32;
    int STATUS_SERVICE_TOONLINE_WAIT_ONLINE = 33;
    int STATUS_SERVICE_TOONLINE_PART_WAIT_ONLINE = 34;
    int STATUS_SERVICE_TOONLINE_ONLY_READ = 35;
    int STATUS_SERVICE_TOONLINE_ONLY_WRITE = 36;
    int STATUS_SERVICE_TOONLINE_PART_ONLY_READ = 37;
    int STATUS_SERVICE_TOONLINE_READ_AND_WRITE = 38;
    int STATUS_SERVICE_TOOFFLINE_NOT_WRITE = 51;
    int STATUS_SERVICE_TOOFFLINE_NOT_READ = 52;
    int STATUS_SERVICE_TOOFFLINE_NOT_READ_WRITE = 53;
    int STATUS_SERVICE_TOOFFLINE_WAIT_REBALANCE = 54;

    int STATUS_TOPIC_OK = 0;
    int STATUS_TOPIC_SOFT_DELETE = 1;
    int STATUS_TOPIC_SOFT_REMOVE = 2;
    int STATUS_TOPIC_HARD_REMOVE = 3;
}
