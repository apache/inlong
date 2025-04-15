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

package org.apache.inlong.sort.protocol.constant;

/**
 * common options constant
 */
public class Constant {

    /**
     * The multiple enable of sink
     */
    public static final String SINK_MULTIPLE_ENABLE = "sink.multiple.enable";

    /**
     * The multiple format of sink
     */
    public static final String SINK_MULTIPLE_FORMAT = "sink.multiple.format";

    /**
     * The multiple database-pattern of sink
     */
    public static final String SINK_MULTIPLE_DATABASE_PATTERN = "sink.multiple.database-pattern";
    /**
     * The multiple table-pattern of sink
     */
    public static final String SINK_MULTIPLE_TABLE_PATTERN = "sink.multiple.table-pattern";

    public static final Boolean DELETE_ESCAPE_CHAR_WHILE_DESERIALIZE_DEFAULT = true;

    public static final Boolean AUTO_APPEND_ESCAPE_CHAR_AFTER_DESERIALIZE_DEFAULT = false;
}
