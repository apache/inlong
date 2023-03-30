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

package org.apache.inlong.manager.common.tool.excel;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public enum ExcelCellDataTransfer {

    DATE {

        final String dateFormat = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * Parse the given object to text.
         *
         * @param o the object to parse
         * @return the parsed text
         */
        String parse2Text(Object o) {
            if (o == null) {
                return null;
            } else if (o instanceof Date) {
                Date date = (Date) o;
                return this.simpleDateFormat.format(date);
            } else {
                return String.valueOf(o);
            }
        }

        /**
         * Parse the given text to object.
         *
         * @param so the text to parse
         * @return the parsed object
         */
        Object parseFromText(String so) {
            if (so != null && so.length() > 0) {
                String s = so;
                Date date = null;

                try {
                    date = this.simpleDateFormat.parse(s);
                } catch (ParseException var5) {
                    var5.printStackTrace();
                }

                return date;
            } else {
                return so;
            }
        }
    },
    NONE {

        /**
         * Parse the given object to text.
         *
         * @param o theobject to parse
         * @return the parsed text
         */
        String parse2Text(Object o) {
            return String.valueOf(o);
        }

        /**
         * Parse the given text to object.
         *
         * @param s the text to parse
         * @return the parsed object
         */
        Object parseFromText(String s) {
            return s;
        }
    };

    private ExcelCellDataTransfer() {
    }
    /**
    * Parse the given object to text.
    *
    * @param o the object to parse
    * @return the parsed text
    */

    abstract String parse2Text(Object o);

    /**
     * Parse the given text to object.
     *
     * @param s the text to parse
     * @return the parsed object
     */
    abstract Object parseFromText(String s);
}
