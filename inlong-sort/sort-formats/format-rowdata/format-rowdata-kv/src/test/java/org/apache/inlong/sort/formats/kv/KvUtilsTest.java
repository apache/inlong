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

package org.apache.inlong.sort.formats.kv;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.util.StringUtils.concatKv;
import static org.apache.inlong.sort.formats.util.StringUtils.splitKv;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for kv splitting and concating.
 */
public class KvUtilsTest {

    @Test
    public void testSplitNormal() {
        List<Map<String, String>> list =
                splitKv("f1=a\nf1=b", '&', '=', '\\', '\"',
                        '\n', true);
        List<Map<String, String>> expectedList = new ArrayList<>();
        expectedList.add(new HashMap<String, String>() {

            {
                put("f1", "a");
            }
        });
        expectedList.add(new HashMap<String, String>() {

            {
                put("f1", "b");
            }
        });
        assertEquals(list, expectedList);

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "a");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("f1=a&f2=b&f3=c", '&', '=', null, null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("f1=&f2=b&f3=c", '&', '=', null, null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "a");
                        put("f2", "b");
                        put("f3", "");
                    }
                },
                splitKv("f1=a&f2=b&f3=", '&', '=', null, null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("=f1", "a");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("\\=f1=a&f2=b&f3=c", '&', '=', '\\', null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("&f1", "a");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("\\&f1=a&f2=b&f3=c", '&', '=', '\\', null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("&f1", "a");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("\"&f1\"=a&f2=b&f3=c", '&', '=', '\\', '\"'));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "a&");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("f1=a\\&&f2=b&f3=c", '&', '=', '\\', null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "a\\");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("f1=a\\\\&f2=b&f3=c", '&', '=', '\\', null));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "a&f2=b");
                        put("f3", "c");
                        put("f4", "d");
                    }
                },
                splitKv("f1=a\"&f2=\"b&f3=c&f4=d", '&', '=', '\\', '\"'));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "atest\\test");
                        put("f2", "b");
                        put("f3", "c");
                    }
                },
                splitKv("f1=a\"test\\test\"&f2=b&f3=c", '&', '=', '\\', '\"'));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "a");
                        put("f2", "\"b");
                        put("f3", "c\"");
                        put("f4", "d");
                    }
                },
                splitKv("f1=a&f2=\\\"b&f3=c\\\"&f4=d", '&', '=', '\\', '\"'));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("f1", "b");
                    }
                },
                splitKv("f1=a&f1=b", '&', '=', '\\', '\"'));

        assertEquals(
                new HashMap<String, String>() {

                    {
                        put("", "a");
                        put("f", "");
                    }
                },
                splitKv("=a&f=", '&', '=', '\\', '\"'));
    }

    @Test
    public void testSplitNormalWithoutEscape() {
        List<Map<String, String>> list =
                splitKv("f1=a\nf1=b", '&', '=', '\\', '\"',
                        '\n', false);
        List<Map<String, String>> expectedList = new ArrayList<>();
        expectedList.add(new HashMap<String, String>() {

            {
                put("f1", "a");
            }
        });
        expectedList.add(new HashMap<String, String>() {

            {
                put("f1", "b");
            }
        });
        assertEquals(list, expectedList);
        ArrayList expectedListMap = new ArrayList<HashMap<String, String>>();
        HashMap expectedMap = new HashMap<String, String>();
        expectedMap.put("\\=f1", "a");
        expectedMap.put("f2", "b");
        expectedMap.put("f3", "c");
        expectedListMap.add(expectedMap);
        assertEquals(expectedListMap,
                splitKv("\\=f1=a&f2=b&f3=c", '&', '=', '\\',
                        null, null, false));

        expectedMap.clear();
        expectedMap.put("\\&f1", "a");
        expectedMap.put("f2", "b");
        expectedMap.put("f3", "c");
        assertEquals(expectedListMap,
                splitKv("\\&f1=a&f2=b&f3=c", '&', '=', '\\',
                        null, null, false));

        expectedMap.clear();
        expectedMap.put("&f1", "a");
        expectedMap.put("f2", "b");
        expectedMap.put("f3", "c");
        assertEquals(expectedListMap,
                splitKv("\"&f1\"=a&f2=b&f3=c", '&', '=', '\\',
                        '\"', null, false));

        expectedMap.clear();
        expectedMap.put("f1", "a\\&");
        expectedMap.put("f2", "b");
        expectedMap.put("f3", "c");
        assertEquals(expectedListMap,
                splitKv("f1=a\\&&f2=b&f3=c", '&', '=', '\\',
                        null, null, false));

        expectedMap.clear();
        expectedMap.put("f1", "a\\\\");
        expectedMap.put("f2", "b");
        expectedMap.put("f3", "c");
        assertEquals(expectedListMap,
                splitKv("f1=a\\\\&f2=b&f3=c", '&', '=',
                        '\\', null, null, false));

        expectedMap.clear();
        expectedMap.put("f1", "a&f2=b");
        expectedMap.put("f3", "c");
        expectedMap.put("f4", "d");
        assertEquals(expectedListMap,
                splitKv("f1=a\"&f2=\"b&f3=c&f4=d", '&', '=',
                        '\\', '\"', null, false));

        expectedMap.clear();
        expectedMap.put("f1", "atest\\test");
        expectedMap.put("f2", "b");
        expectedMap.put("f3", "c");
        assertEquals(expectedListMap,
                splitKv("f1=a\"test\\test\"&f2=b&f3=c", '&', '=', '\\',
                        '\"', null, false));

        expectedMap.clear();
        expectedMap.put("f1", "a");
        expectedMap.put("f2", "\\\"b");
        expectedMap.put("f3", "c\\\"");
        expectedMap.put("f4", "d");
        assertEquals(expectedListMap,
                splitKv("f1=a&f2=\\\"b&f3=c\\\"&f4=d", '&', '=', '\\',
                        '\"', null, false));

        expectedMap.clear();
        expectedMap.put("", "a");
        expectedMap.put("f", "");
        HashMap expectedMap2 = new HashMap<String, String>();
        expectedMap2.put("c", "dd");
        expectedListMap.add(expectedMap2);
        assertEquals(expectedListMap,
                splitKv("=a&f=\nc=d\\d", '&', '=', '\\',
                        '\"', '\n', true));
    }

    @Test
    public void testSplitNestedValue() {
        Map<String, String> kvMap = splitKv("f1=a=a&f2=b&f3=c", '&', '=',
                '\\', '\"');
        Assert.assertEquals("a=a", kvMap.get("f1"));
    }

    @Test
    public void testSplitUnclosedEscaping() {
        Map<String, String> kvMap = splitKv("f1=a&f2=b\\", '&', '=',
                '\\', '\"');
        Assert.assertEquals("b", kvMap.get("f2"));
    }

    @Test
    public void testSplitUnclosedQuoting() {
        Map<String, String> kvMap = splitKv("f1=a&f2=b\"", '&', '=',
                '\\', '\"');
        Assert.assertEquals("b", kvMap.get("f2"));
    }

    @Test
    public void testSplitDanglingKey1() {
        Map<String, String> kvMap = splitKv("f1", '&', '=',
                null, null);
        Assert.assertEquals(null, kvMap.get("f1"));
    }

    @Test
    public void testSplitDanglingKey2() {
        Map<String, String> kvMap = splitKv("f1&f2=3", '&', '=',
                null, null);
    }

    @Test
    public void testConcatNormal() {
        assertEquals(
                "f1=a&f2=b&f3=c&f4=d",
                concatKv(
                        new String[]{"f1", "f2", "f3", "f4"},
                        new String[]{"a", "b", "c", "d"},
                        '&', '=', null, null));

        assertEquals(
                "f1\\&=a&f2=\\&b&f3=c&f4=d",
                concatKv(
                        new String[]{"f1&", "f2", "f3", "f4"},
                        new String[]{"a", "&b", "c", "d"},
                        '&', '=', '\\', '\"'));

        assertEquals(
                "f1=a&f2=\\\\b&f3=c&f4=d",
                concatKv(
                        new String[]{"f1", "f2", "f3", "f4"},
                        new String[]{"a", "\\b", "c", "d"},
                        '&', '=', '\\', '\"'));

        assertEquals(
                "f1=a&f2=\\\"b&f3=c&f4=d",
                concatKv(
                        new String[]{"f1", "f2", "f3", "f4"},
                        new String[]{"a", "\"b", "c", "d"},
                        '&', '=', '\\', '\"'));

        assertEquals(
                "f1\"&\"=a&f2=\"&\"b&f3=c&f4=d",
                concatKv(
                        new String[]{"f1&", "f2", "f3", "f4"},
                        new String[]{"a", "&b", "c", "d"},
                        '&', '=', null, '\"'));
    }

    @Test(expected = RuntimeException.class)
    public void testConcatNoEscapingAndQuoting() {
        concatKv(
                new String[]{"f1", "f2", "f3", "f4"},
                new String[]{"&a", "&b", "&c", "&d"},
                '&', '=', null, null);
    }

    @Test(expected = RuntimeException.class)
    public void testConcatNoEscaping() {
        concatKv(
                new String[]{"f1", "f2", "f3", "f4"},
                new String[]{"a", "\"b", "c", "d"},
                '&', '=', null, '\"');
    }
}
