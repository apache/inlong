package org.apache.inlong.sort.singletenant.flink.kafka;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.serialization.DebeziumSerializationInfo;
import org.apache.inlong.sort.singletenant.flink.serialization.RowDataSerializationSchemaFactory;
import org.apache.kafka.common.utils.Bytes;

public class RowToDebeziumJsonKafkaSinkTest extends KafkaSinkTestBaseForRowData {

    @Override
    protected void prepareData() {
        topic = "test_kafka_row_to_debezium";
        fieldInfos = new FieldInfo[]{
                new FieldInfo("f1", new StringFormatInfo()),
                new FieldInfo("f2", new IntFormatInfo())
        };

        serializationSchema = RowDataSerializationSchemaFactory.build(
                fieldInfos, new DebeziumSerializationInfo("sql", "literal", "null", true)
        );

        prepareTestData();
    }

    private void prepareTestData() {
        testRows = new ArrayList<>();
        Row row1 = Row.of("Anna", 100);
        row1.setKind(RowKind.INSERT);
        testRows.add(row1);

        Row row2 = Row.of("Lisa", 90);
        row2.setKind(RowKind.DELETE);
        testRows.add(row2);

        Row row3 = Row.of("Bob", 80);
        row3.setKind(RowKind.UPDATE_BEFORE);
        testRows.add(row3);

        Row row4 = Row.of("Tom", 70);
        row4.setKind(RowKind.UPDATE_AFTER);
        testRows.add(row4);
    }

    @Override
    protected void verifyData(List<Bytes> results) {
        List<String> actualData = new ArrayList<>();
        results.forEach(value -> actualData.add(new String(value.get())));
        actualData.sort(String::compareTo);

        List<String> expectedData = new ArrayList<>();
        expectedData.add("{\"data\":[{\"f1\":\"Bob\",\"f2\":80}],\"type\":\"DELETE\"}");
        expectedData.add("{\"data\":[{\"f1\":\"Tom\",\"f2\":70}],\"type\":\"INSERT\"}");
        expectedData.add("{\"data\":[{\"f1\":\"Lisa\",\"f2\":90}],\"type\":\"DELETE\"}");
        expectedData.add("{\"data\":[{\"f1\":\"Anna\",\"f2\":100}],\"type\":\"INSERT\"}");
        expectedData.sort(String::compareTo);

        assertEquals(expectedData, actualData);
    }


}
