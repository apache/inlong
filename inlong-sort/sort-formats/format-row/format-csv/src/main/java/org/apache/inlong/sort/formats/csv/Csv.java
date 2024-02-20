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

package org.apache.inlong.sort.formats.csv;

import org.apache.inlong.sort.formats.base.TextFormatDescriptor;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;

/**
 * Format descriptor for comma-separated values (CSV).
 *
 * <p>This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type
 * for Comma-Separated Values (CSV) Files") proposed by the Internet Engineering
 * Task Force (IETF).
 */
public class Csv extends TextFormatDescriptor<Csv> {

    public static final String FORMAT_TYPE_VALUE = "inlong-csv";

    public Csv() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the delimiter character (',' by default).
     *
     * @param delimiter the field delimiter character
     */
    public Csv delimiter(char delimiter) {
        internalProperties.putCharacter(FORMAT_DELIMITER, delimiter);
        return this;
    }
}
