package org.apache.inlong.sort.protocol.deserialization;

import org.apache.inlong.sort.protocol.ProtocolBaseTest;

public class DebeziumDeserializationInfoTest extends ProtocolBaseTest {

    @Override
    public void init() {
        expectedObject = new DebeziumDeserializationInfo(true, "SQL", false);
        expectedJson = "{\"type\":\"debezium_json\", \"ignore_parse_errors\":\"true\",\"timestamp_format_standard\":\"SQL\",\"update_before_include\":\"false\"}";
        equalObj1 = expectedObject;
        equalObj2 = new DebeziumDeserializationInfo(true, "SQL");
        unequalObj = "";


    }
}
