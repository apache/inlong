package org.apache.inlong.sort.formats.common;

import org.apache.inlong.sort.formats.util.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StringUtilsTest {

    @Test
    public void testSplitKvString() {

        String kvString1 = "name=n&age=10";
        Map<String, String> map1 = StringUtils.splitKv(kvString1, '&',
                '=', '\\', '\'');
        assertEquals("n", map1.get("name"));
        assertEquals("10", map1.get("age"));

        String kvString2 = "name=&age=20&";
        Map<String, String> map2 = StringUtils.splitKv(kvString2, '&',
                '=', '\\', '\'');
        assertEquals("", map2.get("name"));
        assertEquals("20&", map2.get("age"));

        String kvString3 = "name==&age=20&&&value=aaa&dddd&";
        Map<String, String> map3 = StringUtils.splitKv(kvString3, '&',
                '=', '\\', '\'');
        assertEquals("=", map3.get("name"));
        assertEquals("20&&", map3.get("age"));
        assertEquals("aaa&dddd&", map3.get("value"));

        String kvString4 = "name==&age=20&&\nname1==&age1=20&&";
        List<Map<String, String>> map4 = StringUtils.splitKv(kvString4, '&',
                '=', '\\', '\'', '\n');
        assertEquals("=", map4.get(0).get("name"));
        assertEquals("20&&", map4.get(0).get("age"));
        assertEquals("=", map4.get(0).get("name1"));
        assertEquals("20&&", map4.get(0).get("age1"));

        String kvString5 = "name==&age=20&&\nname1==&age1=20&&&value=aaa&dddd&";
        List<Map<String, String>> map5 = StringUtils.splitKv(kvString5, '&',
                '=', '\\', '\'', '\n');
        assertEquals("=", map5.get(0).get("name"));
        assertEquals("20&&", map5.get(0).get("age"));
        assertEquals("=", map5.get(0).get("name1"));
        assertEquals("20&&", map5.get(0).get("age1"));
        assertEquals("aaa&dddd&", map5.get(0).get("value"));

        String kvString6 = "name==&age=20&&\\";
        List<Map<String, String>> map6 = StringUtils.splitKv(kvString6, '&',
                '=', '\\', '\'', '\n');
        assertEquals("=", map6.get(0).get("name"));
        assertEquals("20&&", map6.get(0).get("age"));

        String kvString7 = "name==&age=20&&'";
        List<Map<String, String>> map7 = StringUtils.splitKv(kvString7, '&',
                '=', '\\', '\'', '\n');
        assertEquals("=", map7.get(0).get("name"));
        assertEquals("20&&", map7.get(0).get("age"));

        String kvString8 = "name=\\=&age=20&a&'";
        List<Map<String, String>> map8 = StringUtils.splitKv(kvString8, '&',
                '=', '\\', '\'', '\n');
        assertEquals("=", map8.get(0).get("name"));
        assertEquals("20&a&", map8.get(0).get("age"));

        String kvString9 = "name=\\=&age=20&a\\&'";
        List<Map<String, String>> map9 = StringUtils.splitKv(kvString9, '&',
                '=', '\\', '\'', '\n');
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
}
