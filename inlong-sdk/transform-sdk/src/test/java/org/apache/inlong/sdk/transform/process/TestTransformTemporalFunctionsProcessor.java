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

package org.apache.inlong.sdk.transform.process;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

/**
 * TestTransformTemporalFunctionsProcessor
 * description: test the temporal functions in transform processor
 */
public class TestTransformTemporalFunctionsProcessor {

    private static final List<FieldInfo> srcFields = new ArrayList<>();
    private static final List<FieldInfo> dstFields = new ArrayList<>();
    private static final CsvSourceInfo csvSource;
    private static final KvSinkInfo kvSink;

    static {
        for (int i = 1; i < 4; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("string" + i);
            srcFields.add(field);
        }
        for (int i = 1; i < 4; i++) {
            FieldInfo field = new FieldInfo();
            field.setName("numeric" + i);
            srcFields.add(field);
        }
        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", '|', '\\', srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }

    @Before
    public void setUp() {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    @Test
    public void testToDateFunction() throws Exception {
        String transformSql1 = "select to_date(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: to_date('2024-08-15')
        List<String> output1 = processor1.transform("2024-08-15|apple|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=2024-08-15");
        String transformSql2 = "select to_date(string1, string2) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: to_date('20240815', 'yyyyMMdd')
        List<String> output2 = processor2.transform("20240815|yyyyMMdd|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=2024-08-15");
        // case3: to_date('08152024', 'MMddyyyy')
        List<String> output3 = processor2.transform("08152024|MMddyyyy|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=2024-08-15");
        // case4: to_date('2024/08/15', 'yyyy/MM/dd')
        List<String> output4 = processor2.transform("2024/08/15|yyyy/MM/dd|cloud|2|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=2024-08-15");
    }

    @Test
    public void testDateFormatFunction() throws Exception {
        String transformSql1 = "select date_format(string1, 'yyyy-MM-dd HH:mm:ss') from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: date_format('2024-08-01 22:56:56', 'yyyy-MM-dd HH:mm:ss')
        List<String> output1 = processor1.transform("2024-08-01 22:56:56", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=2024-08-01 22:56:56");

        String transformSql2 = "select date_format(string1, 'yyyy-MM-dd') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: date_format('2024-08-01 22:56:56', 'yyyy-MM-dd')
        List<String> output2 = processor2.transform("2024-08-01 22:56:56", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=2024-08-01");

        String transformSql3 = "select date_format(string1, 'yyyyMMddHHmmss') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: date_format('2024-08-01 22:56:56', 'yyyyMMddHHmmss')
        List<String> output3 = processor3.transform("2024-08-01 22:56:56", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=20240801225656");

        String transformSql4 = "select date_format(string1, 'yyyy/MM/dd HH:mm:ss') from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: date_format('2024-08-01 22:56:56', 'yyyy/MM/dd HH:mm:ss')
        List<String> output4 = processor4.transform("2024-08-01 22:56:56", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=2024/08/01 22:56:56");
    }

    @Test
    public void testDateExtractFunction() throws Exception {
        String transformSql1 = "select year(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: year(2024-08-08)
        List<String> output1 = processor1.transform("2024-08-08", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=2024");

        String transformSql2 = "select quarter(string1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: quarter(2024-08-08)
        List<String> output2 = processor2.transform("2024-08-08", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=3");

        String transformSql3 = "select month(string1) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: month(2024-08-08)
        List<String> output3 = processor3.transform("2024-08-08", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=8");

        String transformSql4 = "select week(string1) from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: week(2024-02-29)
        List<String> output4 = processor4.transform("2024-02-29", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=9");

        String transformSql5 = "select day_of_year(string1) from source";
        TransformConfig config5 = new TransformConfig(transformSql5);
        TransformProcessor<String, String> processor5 = TransformProcessor
                .create(config5, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case5: dayofyear(2024-02-29)
        List<String> output5 = processor5.transform("2024-02-29", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals(output5.get(0), "result=60");

        String transformSql6 = "select day_of_month(string1) from source";
        TransformConfig config6 = new TransformConfig(transformSql6);
        TransformProcessor<String, String> processor6 = TransformProcessor
                .create(config6, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case6: dayofmonth(2024-02-29)
        List<String> output6 = processor6.transform("2024-02-29", new HashMap<>());
        Assert.assertEquals(1, output6.size());
        Assert.assertEquals(output6.get(0), "result=29");

        String transformSql7 = "select day_of_week(string1) from source";
        TransformConfig config7 = new TransformConfig(transformSql7);
        TransformProcessor<String, String> processor7 = TransformProcessor
                .create(config7, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case7: dayofweek(2024-02-29) (Thursday)
        List<String> output7 = processor7.transform("2024-02-29", new HashMap<>());
        Assert.assertEquals(1, output7.size());
        Assert.assertEquals(output7.get(0), "result=5");

        String transformSql8 = "select dayname(string1) from source";
        TransformConfig config8 = new TransformConfig(transformSql8);
        TransformProcessor<String, String> processor8 = TransformProcessor
                .create(config8, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case8: dayname(2024-02-29) (Thursday)
        List<String> output8 = processor8.transform("2024-02-29", new HashMap<>());
        Assert.assertEquals(1, output8.size());
        Assert.assertEquals(output8.get(0), "result=THURSDAY");
    }

    @Test
    public void testTimestampExtractFunction() throws Exception {
        String transformSql1 = "select hour(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: hour(2024-08-12 12:23:34)
        List<String> output1 = processor1.transform("2024-08-12 12:23:34", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=12");

        String transformSql2 = "select minute(string1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: minute(2024-08-12 12:23:34)
        List<String> output2 = processor2.transform("2024-08-12 12:23:34", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=23");

        String transformSql3 = "select second(string1) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: second(2024-08-12 12:23:34)
        List<String> output3 = processor3.transform("2024-08-12 12:23:34", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=34");
    }

    @Test
    public void testFromUnixTimeFunction() throws Exception {
        String transformSql1 = "select from_unix_time(numeric1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: from_unix_time(44)
        List<String> output1 = processor1.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=1970-01-01 08:00:44");

        String transformSql2 = "select from_unix_time(numeric1, 'yyyy/MM/dd HH:mm:ss') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: from_unix_time(44, 'yyyy/MM/dd HH:mm:ss')
        List<String> output2 = processor2.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=1970/01/01 08:00:44");

        String transformSql3 = "select from_unix_time(numeric1, 'MMdd-yyyy') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: from_unix_time(44, 'MMdd-yyyy')
        List<String> output3 = processor3.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=0101-1970");

        String transformSql4 = "select from_unix_time(numeric1, 'yyyyMMddHHss') from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: from_unix_time(44, 'yyyyMMddHHss')
        List<String> output4 = processor4.transform("can|apple|cloud|44|1|3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=197001010844");
    }

    @Test
    public void testUnixTimestampFunction() throws Exception {
        String transformSql1 = "select unix_timestamp() from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: unix_timestamp()
        List<String> output1 = processor1.transform("", new HashMap<>());
        Assert.assertEquals(1, output1.size());

        String transformSql2 = "select unix_timestamp(string1, 'yyyy/MM/dd HH:mm:ss') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: unix_timestamp('1970/01/01 08:00:44', 'yyyy/MM/dd HH:mm:ss')
        List<String> output2 = processor2.transform("1970/01/01 08:00:44", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=44");

        String transformSql3 = "select unix_timestamp(string1) from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: unix_timestamp('1970/01/01 08:00:44')
        List<String> output3 = processor3.transform("1970-01-01 08:00:44", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=44");

        String transformSql4 = "select unix_timestamp(string1, 'yyyyMMddHHmmss') from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: unix_timestamp('19700101080044', 'yyyyMMddHHss')
        List<String> output4 = processor4.transform("19700101080044", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=44");
    }

    @Test
    public void testToTimestampFunction() throws Exception {
        String transformSql1 = "select to_timestamp(string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: to_timestamp('1970-01-01 00:00:44')
        List<String> output1 = processor1.transform("1970-01-01 00:00:44", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "result=1970-01-01 00:00:44.0");

        String transformSql2 = "select to_timestamp(string1, 'yyyy/MM/dd HH:mm:ss') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case2: to_timestamp('1970/01/01 00:00:44', 'yyyy/MM/dd HH:mm:ss')
        List<String> output2 = processor2.transform("1970/01/01 00:00:44", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(output2.get(0), "result=1970-01-01 00:00:44.0");

        String transformSql3 = "select to_timestamp(string1, 'yyyyMMddHHmmss') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case3: to_timestamp('19700101000044', 'yyyyMMddHHmmss')
        List<String> output3 = processor3.transform("19700101000044", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(output3.get(0), "result=1970-01-01 00:00:44.0");

        String transformSql4 = "select to_timestamp(string1, 'yyyy-MM-dd') from source";
        TransformConfig config4 = new TransformConfig(transformSql4);
        TransformProcessor<String, String> processor4 = TransformProcessor
                .create(config4, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case4: to_timestamp('1970-01-01', 'yyyy-MM-dd')
        List<String> output4 = processor4.transform("1970-01-01", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals(output4.get(0), "result=1970-01-01 00:00:00.0");
    }

    @Test
    public void testDateDiffFunction() throws Exception {
        String transformSql = null;
        TransformConfig config = null;
        TransformProcessor<String, String> processor = null;
        List<String> output = null;

        transformSql = "select datediff(string1,string2) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: datediff('1970-01-01','1970-01-02')
        output = processor.transform("1970-01-01|1970-01-02", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=-1", output.get(0));

        // case2: datediff('1970-01-02','1970-01-01')
        output = processor.transform("1970-01-02|1970-01-01", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1", output.get(0));

        // case3: datediff('2018-12-10 12:30:00', '2018-12-09 13:30:00')
        output = processor.transform("2018-12-10 12:30:00|2018-12-09 13:30:00", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=1", output.get(0));

        // case4: datediff('2018-12-10 12:30:00', '')
        output = processor.transform("2018-12-10 12:30:00|", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case5: datediff('2018-12', '2018-12-12')
        output = processor.transform("2018-12|2018-12-12", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));

        // case6: datediff('1970-01-01',null)
        transformSql = "select datediff(string1,xxd) from source";
        config = new TransformConfig(transformSql);
        processor = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        output = processor.transform("1970-01-01|1970-01-02", new HashMap<>());
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("result=null", output.get(0));
    }

    @Test
    public void testLocalTimeFunction() throws Exception {
        String transformSql1 = "select localtime() from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        DateTimeFormatter fomatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        // case1: localTime()
        List<String> output1 = processor1.transform("", new HashMap<>());
        LocalTime expectedTime1 = LocalTime.now().withNano(0);
        LocalTime actualTime1 = LocalTime.parse(output1.get(0).split("=")[1], fomatter);
        Duration duration1 = Duration.between(expectedTime1, actualTime1);
        Assert.assertEquals(1, output1.size());
        Assert.assertTrue(duration1.getSeconds() < 1);

        // case2: currentTime("UTC")
        String transformSql2 = "select current_time('UTC') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("", new HashMap<>());
        LocalTime expectedTime2 = LocalTime.now(ZoneId.of("UTC")).withNano(0);
        LocalTime actualTime2 = LocalTime.parse(output2.get(0).split("=")[1], fomatter);
        Duration duration2 = Duration.between(expectedTime2, actualTime2);
        Assert.assertEquals(1, output2.size());
        Assert.assertTrue(duration2.getSeconds() < 1);

        // case 3: localTime("America/New_York")
        String transformSql3 = "select localtime('America/New_York') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor3.transform("", new HashMap<>());
        LocalTime expectedTime3 = LocalTime.now(ZoneId.of("America/New_York")).withNano(0);
        LocalTime actualTime3 = LocalTime.parse(output3.get(0).split("=")[1], fomatter);
        Duration duration3 = Duration.between(expectedTime3, actualTime3);
        Assert.assertEquals(1, output3.size());
        Assert.assertTrue(duration3.getSeconds() < 1);
    }

    @Test
    public void testLocalDateFunction() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // case1: localDate() - default system time zone
        String transformSql1 = "select localdate() from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output1 = processor1.transform("", new HashMap<>());
        LocalDate expectedDate1 = LocalDate.now(ZoneId.systemDefault());
        LocalDate actualDate1 = LocalDate.parse(output1.get(0).split("=")[1], formatter);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(expectedDate1, actualDate1);

        // case2: localDate("UTC")
        String transformSql2 = "select localdate('UTC') from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("", new HashMap<>());
        LocalDate expectedDate2 = LocalDate.now(ZoneId.of("UTC"));
        LocalDate actualDate2 = LocalDate.parse(output2.get(0).split("=")[1], formatter);
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals(expectedDate2, actualDate2);

        // case3: localDate("UTC-12")
        String transformSql3 = "select localdate('UTC-12') from source";
        TransformConfig config3 = new TransformConfig(transformSql3);
        TransformProcessor<String, String> processor3 = TransformProcessor
                .create(config3, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output3 = processor3.transform("", new HashMap<>());
        LocalDate expectedDate3 = LocalDate.now(ZoneId.of("UTC-12"));
        LocalDate actualDate3 = LocalDate.parse(output3.get(0).split("=")[1], formatter);
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals(expectedDate3, actualDate3);
    }

    @Test
    public void testTimestampAdd() throws Exception {
        String transformSql1 = "select timestamp_add('day',string2,string1) from source";
        TransformConfig config1 = new TransformConfig(transformSql1);
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config1, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        // case1: timestamp_add('day',3,'1970-01-01')
        List<String> output1 = processor1.transform("1970-01-01|3", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals("result=1970-01-04", output1.get(0));

        // case2: timestamp_add('day',-3,'1970-01-01 00:00:44')
        List<String> output2 = processor1.transform("1970-01-01 00:00:44|-3", new HashMap<>());
        Assert.assertEquals(1, output2.size());
        Assert.assertEquals("result=1969-12-29 00:00:44", output2.get(0));

        String transformSql2 = "select timestamp_add('MINUTE',string2,string1) from source";
        TransformConfig config2 = new TransformConfig(transformSql2);
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config2, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        // case3: timestamp_add('MINUTE',3,'1970-01-01 00:00:44')
        List<String> output3 = processor2.transform("1970-01-01 00:00:44|3", new HashMap<>());
        Assert.assertEquals(1, output3.size());
        Assert.assertEquals("result=1970-01-01 00:03:44", output3.get(0));

        // case4: timestamp_add('MINUTE',-3,'1970-01-01 00:00:44')
        List<String> output4 = processor2.transform("1970-01-01 00:00:44|-3", new HashMap<>());
        Assert.assertEquals(1, output4.size());
        Assert.assertEquals("result=1969-12-31 23:57:44", output4.get(0));

        // case5: timestamp_add('MINUTE',-3,'1970-01-01')
        List<String> output5 = processor2.transform("1970-01-01|-3", new HashMap<>());
        Assert.assertEquals(1, output5.size());
        Assert.assertEquals("result=1969-12-31 23:57:00", output5.get(0));
    }
}
