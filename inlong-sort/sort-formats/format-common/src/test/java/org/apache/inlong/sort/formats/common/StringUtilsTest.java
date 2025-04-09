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

package org.apache.inlong.sort.formats.common;

import org.apache.inlong.sort.formats.util.StringUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.util.StringUtils.splitKv;
import static org.junit.Assert.assertEquals;

public class StringUtilsTest {

    @Test
    public void testSplitKvString() {

        String kvString1 = "name=n&age=10";
        List<Map<String, String>> listMap1 = StringUtils.splitKv(kvString1, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("n", listMap1.get(0).get("name"));
        assertEquals("10", listMap1.get(0).get("age"));

        String kvString2 = "name=&age=20&";
        List<Map<String, String>> listMap2 = StringUtils.splitKv(kvString2, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("", listMap2.get(0).get("name"));
        assertEquals("20&", listMap2.get(0).get("age"));

        String kvString3 = "name==&age=20&&&value=aaa&dddd&";
        List<Map<String, String>> listMap3 = StringUtils.splitKv(kvString3, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", listMap3.get(0).get("name"));
        assertEquals("20&&", listMap3.get(0).get("age"));
        assertEquals("aaa&dddd&", listMap3.get(0).get("value"));

        String kvString4 = "name==&age=20&&\nname1==&age1=20&&";
        List<Map<String, String>> map4 = StringUtils.splitKv(kvString4, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", map4.get(0).get("name"));
        assertEquals("20&&", map4.get(0).get("age"));
        assertEquals("=", map4.get(1).get("name1"));
        assertEquals("20&&", map4.get(1).get("age1"));

        String kvString5 = "name==&age=20&&\nname1==&age1=20&&&value=aaa&dddd&";
        List<Map<String, String>> map5 = StringUtils.splitKv(kvString5, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", map5.get(0).get("name"));
        assertEquals("20&&", map5.get(0).get("age"));
        assertEquals("=", map5.get(1).get("name1"));
        assertEquals("20&&", map5.get(1).get("age1"));
        assertEquals("aaa&dddd&", map5.get(1).get("value"));

        String kvString6 = "name==&age=20&&\\";
        List<Map<String, String>> map6 = StringUtils.splitKv(kvString6, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", map6.get(0).get("name"));
        assertEquals("20&&", map6.get(0).get("age"));

        String kvString7 = "name==&age=20&&'";
        List<Map<String, String>> map7 = StringUtils.splitKv(kvString7, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", map7.get(0).get("name"));
        assertEquals("20&&", map7.get(0).get("age"));

        String kvString8 = "name=\\=&age=20&a&'";
        List<Map<String, String>> map8 = StringUtils.splitKv(kvString8, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", map8.get(0).get("name"));
        assertEquals("20&a&", map8.get(0).get("age"));

        String kvString9 = "name=\\=&age=20&a\\&'";
        List<Map<String, String>> map9 = StringUtils.splitKv(kvString9, '&',
                '=', '\\', '\'', '\n', true);
        assertEquals("=", map8.get(0).get("name"));
        assertEquals("20&a&", map8.get(0).get("age"));
    }

    @Test
    public void testSplitCsvString() {
        String csvString1 = "name|age=20\\||&'";
        String[][] csv1Array1 = StringUtils.splitCsv(csvString1, '|',
                '\\', '\'', '\n');

        assertEquals("age=20|", csv1Array1[0][1]);
        assertEquals("&", csv1Array1[0][2]);

        String csvString2 = "name|age=20\\||&'\n\name|age=20\\||&'\n\n|home|\\home\\";
        String[][] csv1Array2 = StringUtils.splitCsv(csvString2, '|',
                '\\', '\'', '\n');

        assertEquals("name", csv1Array2[0][0]);
        assertEquals("age=20|", csv1Array2[0][1]);
        assertEquals("&\n\name|age=20\\||&", csv1Array2[0][2]);
        assertEquals("", csv1Array2[2][0]);
        assertEquals("home", csv1Array2[2][1]);
        assertEquals("home", csv1Array2[2][2]);
    }

    @Test
    public void testSplitCsvStringWhiteEscape() {
        String csvString1 = "name|age=20\\||&'";
        String[][] csv1Array1 = StringUtils.splitCsv(csvString1, '|',
                '\\', '\'', '\n', false, false);

        assertEquals("age=20\\|", csv1Array1[0][1]);
        assertEquals("&", csv1Array1[0][2]);

        String csvString2 = "name|age=20\\||&'\n\name|age=20\\||&'\n\n|home|\\home\\";
        String[][] csv1Array2 = StringUtils.splitCsv(csvString2, '|',
                '\\', '\'', '\n', false, false);

        assertEquals("name", csv1Array2[0][0]);
        assertEquals("age=20\\|", csv1Array2[0][1]);
        assertEquals("&\n\name|age=20\\||&", csv1Array2[0][2]);
        assertEquals("", csv1Array2[2][0]);
        assertEquals("home", csv1Array2[2][1]);
        assertEquals("\\home\\", csv1Array2[2][2]);
    }

    @Test
    public void testSplitCsvStringWithMaxFields() {

        String csvString = "name|age=20\\||&'\n\name|age=20\\||&'\n\n|home|\\home\\";
        String[][] csv1Array0 = StringUtils.splitCsv(csvString, '|',
                '\\', '\'', '\n', false, true,
                0);
        assertEquals(0, csv1Array0.length);

        String[][] csv1Array1 = StringUtils.splitCsv(csvString, '|',
                '\\', '\'', '\n', false, true,
                1);
        assertEquals("name|age=20\\||&'\n\name|age=20\\||&'", csv1Array1[0][0]);
        assertEquals("", csv1Array1[1][0]);
        assertEquals("|home|\\home\\", csv1Array1[2][0]);

        String[][] csv1Array2 = StringUtils.splitCsv(csvString, '|',
                '\\', '\'', '\n', false, true,
                2);
        assertEquals("name", csv1Array2[0][0]);
        assertEquals("age=20\\||&'\n\name|age=20\\||&'", csv1Array2[0][1]);
        assertEquals("", csv1Array2[1][0]);
        assertEquals("", csv1Array2[2][0]);
        assertEquals("home|\\home\\", csv1Array2[2][1]);

        String[][] csv1Array3 = StringUtils.splitCsv(csvString, '|',
                '\\', '\'', '\n', false, true,
                3);
        assertEquals("name", csv1Array3[0][0]);
        assertEquals("age=20|", csv1Array3[0][1]);
        assertEquals("&\n\name|age=20\\||&", csv1Array3[0][2]);
        assertEquals("", csv1Array3[2][0]);
        assertEquals("home", csv1Array3[2][1]);
        assertEquals("home", csv1Array3[2][2]);

        String[][] csv1Array4 = StringUtils.splitCsv(csvString, '|',
                '\\', '\'', '\n', false, true,
                4);
        assertEquals("name", csv1Array4[0][0]);
        assertEquals("age=20|", csv1Array4[0][1]);
        assertEquals("&\n\name|age=20\\||&", csv1Array4[0][2]);
        assertEquals("", csv1Array4[2][0]);
        assertEquals("home", csv1Array4[2][1]);
        assertEquals("home", csv1Array4[2][2]);
    }

    @Test
    public void testKvScapeCharSplit() {
        String text = "k1=v1&\nk\\2=v2\\&&k3=v3";
        List<Map<String, String>> kvMapList = splitKv(text, '&', '=', '\\',
                null, '\n', false);
        Assert.assertTrue(kvMapList != null && kvMapList.size() == 2);
        Assert.assertTrue(kvMapList.get(0).get("k3") == null);
        Assert.assertTrue(kvMapList.get(1).get("\nk2") == null);
        Assert.assertTrue(kvMapList.get(1).get("k\\2") != null);
    }
}
