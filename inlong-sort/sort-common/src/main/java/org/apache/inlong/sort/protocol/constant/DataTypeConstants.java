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
 * The constants class of Data Type. All constants related to data types should be defined in this class.
 * For example, the length of the Char or Varchar, the precision and scale of the Decimal, etc.
 */
public class DataTypeConstants {

    /**
     * The default precision of Decima. It is used to control the default precision
     * when the source Decimal type precision cannot be obtained in the whole database migration.
     */
    public static final int DEFAULT_DECIMAL_PRECISION = 38;
    /**
     * The default scale of Decimal. It is used to control the default scale
     * when the Source Decimal type scale cannot be obtained in the whole database migration.
     */
    public static final int DEFAULT_DECIMAL_SCALE = 5;
    /**
     * The default length of Char. It is used to control the default length of Char
     * when the length of Source Char type cannot be obtained in the whole database migration.
     */
    public static final int DEFAULT_CHAR_LENGTH = 255;
    /**
     * The default length of Char format ty time such as 'yyyy-MM-dd HH:mm:ss' or 'HH:mm:ss', etc.
     * It is used to control the default length of Char when the length of Source Char type
     * cannot be obtained in the whole database migration.
     */
    public static final int DEFAULT_CHAR_TIME_LENGTH = 30;
    /**
     * The key of Timestamp with time_zone in Oracle. It is used in the whole database migration.
     */
    public static final Integer ORACLE_TIMESTAMP_TIME_ZONE = -101;

    private DataTypeConstants() {
    }
}
