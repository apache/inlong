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

package org.apache.inlong.sdk.transform.process.processor;

import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.encode.SinkEncoderFactory;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.TransformProcessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCsv2KvProcessor extends AbstractProcessorTestBase {

    @Test
    public void testCsv2Kv() throws Exception {
        List<FieldInfo> fields = this.getTestFieldList("ftime", "extinfo", "ds");
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', fields);
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", fields);
        String transformSql = "select ftime,extinfo,$ctx.partition from source where extinfo='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        Map<String, Object> extParams = new HashMap<>();
        extParams.put("partition", "2024042801");
        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", extParams);
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok&ds=2024042801");
        // case2
        config.setTransformSql("select ftime,extinfo from source where extinfo!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));

        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testCsv2KvNoField() throws Exception {
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', null);
        KvSinkInfo kvSink = new KvSinkInfo("UTF-8", null);
        String transformSql = "select $1 ftime,$2 extinfo from source where $2='ok'";
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output1 = processor1.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(1, output1.size());
        Assert.assertEquals(output1.get(0), "ftime=2024-04-28 00:00:00&extinfo=ok");
        // case2
        config.setTransformSql("select $1 ftime,$2 extinfo from source where $2!='ok'");
        TransformProcessor<String, String> processor2 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createKvEncoder(kvSink));
        List<String> output2 = processor2.transform("2024-04-28 00:00:00|ok", new HashMap<>());
        Assert.assertEquals(0, output2.size());
    }

    @Test
    public void testCsv2CsvSplit() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList("ftime", "extinfo", "country", "province", "operator",
                "apn", "gw", "src_ip_head", "info_str", "product_id", "app_version", "sdk_id", "sdk_version",
                "hardware_os", "qua", "upload_ip", "client_ip", "upload_apn", "event_code", "event_result",
                "package_size", "consume_time", "event_value", "event_time", "upload_time");
        List<FieldInfo> sinkFields = this.getTestFieldList("imp_hour", "ftime", "event_code", "event_time", "log_id",
                "qimei36", "platform", "hardware_os", "os_version", "brand", "model", "country", "province", "city",
                "network_type", "dt_qq", "app_version", "boundle_id", "dt_usid", "dt_pgid", "dt_ref_pgid", "dt_eid",
                "dt_element_lvtm", "dt_lvtm", "product_id", "biz_pub_params", "udf_kv", "sdk_type", "app_version_num");
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', sourceFields);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', sinkFields);
        String transformSql = "select replace(substr(ftime,1,10),'-','') as imp_hour,"
                + "ftime as ftime,event_code as event_code,"
                + "event_time as event_time,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A100') as log_id,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A153') as qimei36,"
                + "case when lower(url_decode(hardware_os,'GBK')) like '%android%' then 'android' when lower(url_decode(hardware_os,'GBK')) like '%ipad%' then 'ipad' when lower(url_decode(hardware_os,'GBK')) like '%iphone%' then 'iphone' when lower(url_decode(hardware_os,'GBK')) like '%harmony%' then 'harmony' when lower(url_decode(hardware_os,'GBK')) like '%windows%' then 'windows' when lower(url_decode(hardware_os,'GBK')) like '%mac%' then 'mac' when lower(url_decode(hardware_os,'GBK')) like '%linux%' then 'linux' else 'unknown' end as platform,"
                + "url_decode(hardware_os,'GBK') as hardware_os,"
                + "trim(case when hardware_os LIKE '%Android%' then regexp_extract(url_decode(hardware_os,'GBK'), 'Android(.+),level', 1) when hardware_os LIKE '%iPhone%' then regexp_extract(url_decode(hardware_os,'GBK'), 'OS(.+)\\\\(', 1) when hardware_os LIKE '%Harmony%' then regexp_extract(url_decode(hardware_os,'GBK'), 'Harmony\\\\s+[^\\\\s]+\\\\s+([^\\\\s]+)\\\\(', 1) else 'unknown' end) as os_version,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A9') as brand,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A10') as model,"
                + "country as country,"
                + "province as province,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A160') as city,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A19') as network_type,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_qq') as dt_qq,"
                + "url_decode(app_version,'GBK') as app_version,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','A67') as boundle_id,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_usid') as dt_usid,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_pgid') as dt_pgid,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_ref_pgid') as dt_ref_pgid,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_eid') as dt_eid,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_element_lvtm') as dt_element_lvtm,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','dt_lvtm') as dt_lvtm,"
                + "product_id as product_id,"
                + "json_remove(str_to_json(url_decode(event_value,'GBK'),'&','='),'udf_kv') as biz_pub_params,"
                + "parse_url(url_decode(event_value,'GBK'),'QUERY','udf_kv') as udf_kv,"
                + "case when sdk_id='js' then 1 when sdk_id='weapp' then 2 else 0 end as sdk_type,"
                + "split_index(app_version,'\\.',0)*1000+split_index(app_version,'\\.',1)*100+split_index(split_index(app_version,'\\.',2),'\\(',0) as app_version_num "
                + "from source where parse_url(url_decode(event_value,'GBK'),'QUERY','dt_pgid') like 'pg_sgrp_%'";
        System.out.println(transformSql);
        TransformConfig config = new TransformConfig(transformSql);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        String sourceData =
                "2025-01-01 01:01:01.001|extinfo=127.0.0.1|china|guangdong|unite|unknown|unknown|127.0.0.1 2025-01-01 01:01:01.001|INFO|MNJT|1.2.0.12345|js|1.2.3.4-qqvideo6|PJV110%3BAndroid+15%2Clevel+35||127.0.0.1|127.0.0.1|wifi|dt_imp|true|0|0|A9%3DOPPO%26A89%3D12345678%26A76%3D1.2.3.4%26A58%3DN%26A52%3D480%26A17%3D1080*2244%26A12%3Dzh%26A10%3DPJV110%26A158%3D12345678%26A67%3Dmobileapp%26A159%3DN%26A31%3D%2C%2C%26A160%3Dshenzhen%26ui_vrsn%3DPJV%28CN01%29%26udf_kv%3D%7B%22eid%22%3A%22search%22%2C%22cur_pg%22%3A%7B%7D%7D%26tianshu_id%3D%26red_pot%3D0%26param_patch_version%3D0%26message_box%3D%7B+%09%22message_unread%22%3A+0%2C+%09%22other_unread%22%3A+0%2C+%09%22validation_message_unread%22%3A+0%2C+%09%221%22%3A+0%7D%26dt_wxunionid%3D%26dt_wxopenid%3D%26param_is_gray_version%3Dfalse%26dt_usstmp%3D12345678%26dt_ussn%3D12345678%26dt_tid%3D%26dt_simtype%3D3%26os_vrsn%3DAndroid+15%26dt_seqid%3D1480%26dt_sdkversion%3D2445%26dt_qqopenid%3D%26dt_qq%3D12345678%26dt_usid%3D12345678%26dt_protoversion%3D1%26A99%3DN%26callfrom_type%3D0%26dt_ele_reuse_id%3D%26dt_omgbzid%3D%26dt_ele_scroll_flag%3D0%26dt_element_params%3D%5B%7B%22eid%22%3A%22search%22%7D%5D%26app_bld%3D12345678%26dt_ele_is_first_scroll_imp%3D0%26A88%3D12345678%26A48%3D%26A95%3D1.2.0.12345%26A19%3Dwifi%26A3%3D12345678%26dt_seqtime%3D12345678%26dt_pgid%3Dpg_sgrp_test%26dt_adcode%3D%26dt_oaid%3D%26qq_appid%3D12345678%26dt_starttype%3D1%26A100%3D12345678%26dt_wbopenid%3D%26A23%3D12345678%26A156%3DN%26A72%3D1.2.3.4%26A157%3D1.2.0.12345%26dt_mainlogin%3D%26A34%3D12345678%26A153%3D123456%26dt_coldstart%3D0%26app_vr%3D1.2.3%26A8%3D12345678%26client_page_name%3Dpage%26dt123456%3D0%26dt_mchlid%3D%26client_process_name%3Dcom.tencent.mobileqq%26os%3D1%26dt_accountid%3D12345678%26dt_callfrom%3D0%26dt_eid%3Dsearch%26dt_guid%3D12345678%26A1%3D12345678%26dt_callschema%3D1%26dt_fchlid%3D|2025-01-01 01:01:01.001|2025-08-07 16:39:26";
        List<String> output1 = processor1.transform(sourceData, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        System.out.println(output1.get(0));
        Assert.assertEquals(output1.get(0),
                "20250101|2025-01-01 01:01:01.001|dt_imp|2025-01-01 01:01:01.001|12345678|123456|android|PJV110;Android 15,level 35|15|OPPO|PJV110|china|guangdong|shenzhen|wifi|12345678|1.2.0.12345|mobileapp|12345678|pg_sgrp_test||search|||MNJT|{\"A88\":\"12345678\",\"A89\":\"12345678\",\"A48\":\"\",\"dt_wxopenid\":\"\",\"dt_seqtime\":\"12345678\",\"app_bld\":\"12345678\",\"A100\":\"12345678\",\"dt_fchlid\":\"\",\"A1\":\"12345678\",\"os_vrsn\":\"Android 15\",\"A3\":\"12345678\",\"dt_mchlid\":\"\",\"dt_usstmp\":\"12345678\",\"client_process_name\":\"com.tencent.mobileqq\",\"dt_guid\":\"12345678\",\"A8\":\"12345678\",\"dt_callfrom\":\"0\",\"A9\":\"OPPO\",\"dt_qq\":\"12345678\",\"tianshu_id\":\"\",\"dt_element_params\":\"[{\\\\\"eid\\\\\":\\\\\"search\\\\\"}]\",\"dt_ussn\":\"12345678\",\"dt_ele_reuse_id\":\"\",\"A95\":\"1.2.0.12345\",\"A52\":\"480\",\"dt_qqopenid\":\"\",\"A10\":\"PJV110\",\"A99\":\"N\",\"A12\":\"zh\",\"dt_ele_scroll_flag\":\"0\",\"dt_eid\":\"search\",\"dt_seqid\":\"1480\",\"A58\":\"N\",\"param_is_gray_version\":\"false\",\"A17\":\"1080*2244\",\"A19\":\"wifi\",\"A159\":\"N\",\"red_pot\":\"0\",\"dt_ele_is_first_scroll_imp\":\"0\",\"dt_wbopenid\":\"\",\"A157\":\"1.2.0.12345\",\"A158\":\"12345678\",\"A156\":\"N\",\"A153\":\"123456\",\"qq_appid\":\"12345678\",\"dt_wxunionid\":\"\",\"ui_vrsn\":\"PJV(CN01)\",\"dt_sdkversion\":\"2445\",\"dt_coldstart\":\"0\",\"A67\":\"mobileapp\",\"A23\":\"12345678\",\"client_page_name\":\"page\",\"dt_starttype\":\"1\",\"dt123456\":\"0\",\"callfrom_type\":\"0\",\"A160\":\"shenzhen\",\"dt_tid\":\"\",\"dt_usid\":\"12345678\",\"A72\":\"1.2.3.4\",\"param_patch_version\":\"0\",\"A31\":\",,\",\"A76\":\"1.2.3.4\",\"dt_omgbzid\":\"\",\"A34\":\"12345678\",\"dt_mainlogin\":\"\",\"os\":\"1\",\"message_box\":\"{ \\\\t\\\\\"message_unread\\\\\": 0, \\\\t\\\\\"other_unread\\\\\": 0, \\\\t\\\\\"validation_message_unread\\\\\": 0, \\\\t\\\\\"1\\\\\": 0}\",\"dt_protoversion\":\"1\",\"dt_callschema\":\"1\",\"dt_simtype\":\"3\",\"dt_pgid\":\"pg_sgrp_test\",\"dt_oaid\":\"\",\"dt_adcode\":\"\",\"dt_accountid\":\"12345678\",\"app_vr\":\"1.2.3\"}|{\"eid\":\"search\",\"cur_pg\":{}}|1|1200");
    }

    @Test
    public void testCsv2CsvRuntimesMap() throws Exception {
        List<FieldInfo> sourceFields = this.getTestFieldList("ftime", "extinfo", "country", "province", "operator",
                "apn", "gw", "src_ip_head", "info_str", "product_id", "app_version", "sdk_id", "sdk_version",
                "hardware_os", "qua", "upload_ip", "client_ip", "upload_apn", "event_code", "event_result",
                "package_size", "consume_time", "event_value", "event_time", "upload_time");
        List<FieldInfo> sinkFields = this.getTestFieldList("imp_hour", "ftime", "event_code", "event_time", "log_id",
                "qimei36", "platform", "hardware_os", "os_version", "brand", "model", "country", "province", "city",
                "network_type", "dt_qq", "app_version", "boundle_id", "dt_usid", "dt_pgid", "dt_ref_pgid", "dt_eid",
                "dt_element_lvtm", "dt_lvtm", "product_id", "biz_pub_params", "udf_kv", "sdk_type", "app_version_num");
        CsvSourceInfo csvSource = new CsvSourceInfo("UTF-8", '|', '\\', sourceFields);
        CsvSinkInfo csvSink = new CsvSinkInfo("UTF-8", '|', '\\', sinkFields);
        String transformSql = "select replace(substr(ftime,1,10),'-','') as imp_hour,"
                + "url_decode(event_value,'GBK') as decode_event_value,"
                + "url_decode(hardware_os,'GBK') as decode_hardvalue_os,"
                + "lower($ctx.decode_hardvalue_os) as lower_hardvalue_os,"
                + "ftime as ftime,event_code as event_code,"
                + "event_time as event_time,"
                + "parse_url($ctx.decode_event_value,'QUERY','A100') as log_id,"
                + "parse_url($ctx.decode_event_value,'QUERY','A153') as qimei36,"
                + "case when $ctx.lower_hardvalue_os like '%android%' then 'android' when $ctx.lower_hardvalue_os like '%ipad%' then 'ipad' when $ctx.lower_hardvalue_os like '%iphone%' then 'iphone' when $ctx.lower_hardvalue_os like '%harmony%' then 'harmony' when $ctx.lower_hardvalue_os like '%windows%' then 'windows' when $ctx.lower_hardvalue_os like '%mac%' then 'mac' when $ctx.lower_hardvalue_os like '%linux%' then 'linux' else 'unknown' end as platform,"
                + "$ctx.decode_hardvalue_os as hardware_os,"
                + "trim(case when hardware_os LIKE '%Android%' then regexp_extract($ctx.decode_hardvalue_os, 'Android(.+),level', 1) when hardware_os LIKE '%iPhone%' then regexp_extract($ctx.decode_hardvalue_os, 'OS(.+)\\\\(', 1) when hardware_os LIKE '%Harmony%' then regexp_extract($ctx.decode_hardvalue_os, 'Harmony\\\\s+[^\\\\s]+\\\\s+([^\\\\s]+)\\\\(', 1) else 'unknown' end) as os_version,"
                + "parse_url($ctx.decode_event_value,'QUERY','A9') as brand,"
                + "parse_url($ctx.decode_event_value,'QUERY','A10') as model,"
                + "country as country,"
                + "province as province,"
                + "parse_url($ctx.decode_event_value,'QUERY','A160') as city,"
                + "parse_url($ctx.decode_event_value,'QUERY','A19') as network_type,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_qq') as dt_qq,"
                + "url_decode(app_version,'GBK') as app_version,"
                + "parse_url($ctx.decode_event_value,'QUERY','A67') as boundle_id,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_usid') as dt_usid,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_pgid') as dt_pgid,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_ref_pgid') as dt_ref_pgid,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_eid') as dt_eid,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_element_lvtm') as dt_element_lvtm,"
                + "parse_url($ctx.decode_event_value,'QUERY','dt_lvtm') as dt_lvtm,"
                + "product_id as product_id,"
                + "json_remove(str_to_json($ctx.decode_event_value,'&','='),'udf_kv') as biz_pub_params,"
                + "parse_url($ctx.decode_event_value,'QUERY','udf_kv') as udf_kv,"
                + "case when sdk_id='js' then 1 when sdk_id='weapp' then 2 else 0 end as sdk_type,"
                + "split_index(app_version,'\\.',0)*1000+split_index(app_version,'\\.',1)*100+split_index(split_index(app_version,'\\.',2),'\\(',0) as app_version_num "
                + "from source where parse_url(url_decode(event_value,'GBK'),'QUERY','dt_pgid') like 'pg_sgrp_%'";
        System.out.println(transformSql);
        TransformConfig config = new TransformConfig(transformSql, new HashMap<>(), false, true);
        // case1
        TransformProcessor<String, String> processor1 = TransformProcessor
                .create(config, SourceDecoderFactory.createCsvDecoder(csvSource),
                        SinkEncoderFactory.createCsvEncoder(csvSink));
        String sourceData =
                "2025-01-01 01:01:01.001|extinfo=127.0.0.1|china|guangdong|unite|unknown|unknown|127.0.0.1 2025-01-01 01:01:01.001|INFO|MNJT|1.2.0.12345|js|1.2.3.4-qqvideo6|PJV110%3BAndroid+15%2Clevel+35||127.0.0.1|127.0.0.1|wifi|dt_imp|true|0|0|A9%3DOPPO%26A89%3D12345678%26A76%3D1.2.3.4%26A58%3DN%26A52%3D480%26A17%3D1080*2244%26A12%3Dzh%26A10%3DPJV110%26A158%3D12345678%26A67%3Dmobileapp%26A159%3DN%26A31%3D%2C%2C%26A160%3Dshenzhen%26ui_vrsn%3DPJV%28CN01%29%26udf_kv%3D%7B%22eid%22%3A%22search%22%2C%22cur_pg%22%3A%7B%7D%7D%26tianshu_id%3D%26red_pot%3D0%26param_patch_version%3D0%26message_box%3D%7B+%09%22message_unread%22%3A+0%2C+%09%22other_unread%22%3A+0%2C+%09%22validation_message_unread%22%3A+0%2C+%09%221%22%3A+0%7D%26dt_wxunionid%3D%26dt_wxopenid%3D%26param_is_gray_version%3Dfalse%26dt_usstmp%3D12345678%26dt_ussn%3D12345678%26dt_tid%3D%26dt_simtype%3D3%26os_vrsn%3DAndroid+15%26dt_seqid%3D1480%26dt_sdkversion%3D2445%26dt_qqopenid%3D%26dt_qq%3D12345678%26dt_usid%3D12345678%26dt_protoversion%3D1%26A99%3DN%26callfrom_type%3D0%26dt_ele_reuse_id%3D%26dt_omgbzid%3D%26dt_ele_scroll_flag%3D0%26dt_element_params%3D%5B%7B%22eid%22%3A%22search%22%7D%5D%26app_bld%3D12345678%26dt_ele_is_first_scroll_imp%3D0%26A88%3D12345678%26A48%3D%26A95%3D1.2.0.12345%26A19%3Dwifi%26A3%3D12345678%26dt_seqtime%3D12345678%26dt_pgid%3Dpg_sgrp_test%26dt_adcode%3D%26dt_oaid%3D%26qq_appid%3D12345678%26dt_starttype%3D1%26A100%3D12345678%26dt_wbopenid%3D%26A23%3D12345678%26A156%3DN%26A72%3D1.2.3.4%26A157%3D1.2.0.12345%26dt_mainlogin%3D%26A34%3D12345678%26A153%3D123456%26dt_coldstart%3D0%26app_vr%3D1.2.3%26A8%3D12345678%26client_page_name%3Dpage%26dt123456%3D0%26dt_mchlid%3D%26client_process_name%3Dcom.tencent.mobileqq%26os%3D1%26dt_accountid%3D12345678%26dt_callfrom%3D0%26dt_eid%3Dsearch%26dt_guid%3D12345678%26A1%3D12345678%26dt_callschema%3D1%26dt_fchlid%3D|2025-01-01 01:01:01.001|2025-08-07 16:39:26";
        List<String> output1 = processor1.transform(sourceData, new HashMap<>());
        Assert.assertEquals(1, output1.size());
        System.out.println(output1.get(0));
        Assert.assertEquals(output1.get(0),
                "20250101|2025-01-01 01:01:01.001|dt_imp|2025-01-01 01:01:01.001|12345678|123456|android|PJV110;Android 15,level 35|15|OPPO|PJV110|china|guangdong|shenzhen|wifi|12345678|1.2.0.12345|mobileapp|12345678|pg_sgrp_test||search|||MNJT|{\"A88\":\"12345678\",\"A89\":\"12345678\",\"A48\":\"\",\"dt_wxopenid\":\"\",\"dt_seqtime\":\"12345678\",\"app_bld\":\"12345678\",\"A100\":\"12345678\",\"dt_fchlid\":\"\",\"A1\":\"12345678\",\"os_vrsn\":\"Android 15\",\"A3\":\"12345678\",\"dt_mchlid\":\"\",\"dt_usstmp\":\"12345678\",\"client_process_name\":\"com.tencent.mobileqq\",\"dt_guid\":\"12345678\",\"A8\":\"12345678\",\"dt_callfrom\":\"0\",\"A9\":\"OPPO\",\"dt_qq\":\"12345678\",\"tianshu_id\":\"\",\"dt_element_params\":\"[{\\\\\"eid\\\\\":\\\\\"search\\\\\"}]\",\"dt_ussn\":\"12345678\",\"dt_ele_reuse_id\":\"\",\"A95\":\"1.2.0.12345\",\"A52\":\"480\",\"dt_qqopenid\":\"\",\"A10\":\"PJV110\",\"A99\":\"N\",\"A12\":\"zh\",\"dt_ele_scroll_flag\":\"0\",\"dt_eid\":\"search\",\"dt_seqid\":\"1480\",\"A58\":\"N\",\"param_is_gray_version\":\"false\",\"A17\":\"1080*2244\",\"A19\":\"wifi\",\"A159\":\"N\",\"red_pot\":\"0\",\"dt_ele_is_first_scroll_imp\":\"0\",\"dt_wbopenid\":\"\",\"A157\":\"1.2.0.12345\",\"A158\":\"12345678\",\"A156\":\"N\",\"A153\":\"123456\",\"qq_appid\":\"12345678\",\"dt_wxunionid\":\"\",\"ui_vrsn\":\"PJV(CN01)\",\"dt_sdkversion\":\"2445\",\"dt_coldstart\":\"0\",\"A67\":\"mobileapp\",\"A23\":\"12345678\",\"client_page_name\":\"page\",\"dt_starttype\":\"1\",\"dt123456\":\"0\",\"callfrom_type\":\"0\",\"A160\":\"shenzhen\",\"dt_tid\":\"\",\"dt_usid\":\"12345678\",\"A72\":\"1.2.3.4\",\"param_patch_version\":\"0\",\"A31\":\",,\",\"A76\":\"1.2.3.4\",\"dt_omgbzid\":\"\",\"A34\":\"12345678\",\"dt_mainlogin\":\"\",\"os\":\"1\",\"message_box\":\"{ \\\\t\\\\\"message_unread\\\\\": 0, \\\\t\\\\\"other_unread\\\\\": 0, \\\\t\\\\\"validation_message_unread\\\\\": 0, \\\\t\\\\\"1\\\\\": 0}\",\"dt_protoversion\":\"1\",\"dt_callschema\":\"1\",\"dt_simtype\":\"3\",\"dt_pgid\":\"pg_sgrp_test\",\"dt_oaid\":\"\",\"dt_adcode\":\"\",\"dt_accountid\":\"12345678\",\"app_vr\":\"1.2.3\"}|{\"eid\":\"search\",\"cur_pg\":{}}|1|1200");
    }
}
