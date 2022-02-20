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

package org.apache.inlong.sort.flink.clickhouse;

import org.apache.inlong.sort.formats.common.IntTypeInfo;
import org.apache.inlong.sort.formats.common.StringTypeInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClickHouseRowConverterTest {

    @Test
    public void testToObjectArray() {
        int[] testArray = new int[] {1, 2, 3};
        Object obj = ClickHouseRowConverter.toObjectArray(new IntTypeInfo(), testArray);
        assertTrue(obj instanceof Integer[]);
        Integer[] integers = (Integer[]) obj;
        assertEquals(Integer.valueOf(1), integers[0]);
        assertEquals(Integer.valueOf(2), integers[1]);
        assertEquals(Integer.valueOf(3), integers[2]);

        String[] strs = new String[] {"f1", "f2", "f3"};
        Object strsArray = ClickHouseRowConverter.toObjectArray(new StringTypeInfo(), strs);
        assertEquals(strs, strsArray);
    }
}
