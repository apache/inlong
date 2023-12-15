package org.apache.inlong.sort.function;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link RoundTimestampFunction}
 */
public class RoundTimestampFunctionTest extends AbstractTestBase {

    public static final long TEST_TIMESTAMP = 1702610371L;

    /**
     * Test for round timestamp function
     *
     * @throws Exception The exception may throw when test round timestamp function
     */
    @Test
    public void testRoundTimestampFunction() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // step 1. Register custom function of ROUND_TIMESTAMP
        tableEnv.createTemporaryFunction("ROUND_TIMESTAMP", RoundTimestampFunction.class);

        // step 2. Generate test data and convert to DataStream

        List<Row> data = new ArrayList<>();
        data.add(Row.of(TEST_TIMESTAMP));
        TypeInformation<?>[] types = {BasicTypeInfo.LONG_TYPE_INFO};

        String[] names = {"f1"};
        RowTypeInfo typeInfo = new RowTypeInfo(types, names);
        DataStream<Row> dataStream = env.fromCollection(data).returns(typeInfo);

        String formattedTimestamp = "2023121510";

        // step 3. Convert from DataStream to Table and execute the ROUND_TIMESTAMP function
        Table tempView = tableEnv.fromDataStream(dataStream).as("f1");
        tableEnv.createTemporaryView("temp_view", tempView);
        Table outputTable = tableEnv.sqlQuery(
            "SELECT ROUND_TIMESTAMP(f1, 600, 'yyyyMMddmm') " +
                "from temp_view");

        // step 4. Get function execution result and parse it
        DataStream<Row> resultSet = tableEnv.toAppendStream(outputTable, Row.class);
        List<String> result = new ArrayList<>();
        for (CloseableIterator<Row> it = resultSet.executeAndCollect(); it.hasNext();) {
            Row row = it.next();
            if (row != null) {
                result.add(row.getField(0).toString());
            }
        }

        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), formattedTimestamp);

    }

}