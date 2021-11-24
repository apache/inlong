package org.apache.inlong.sort.flink.doris;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.doris.output.DorisOutputFormat;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;

/**
 * @program: incubator-inlong
 * @author: huzekang
 * @create: 2021-11-24 16:01
 **/
public class TestDorisSink {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);



		final DataSource<Tuple2<Boolean, Row>> data = env.fromElements(
				new Tuple2<Boolean, Row>(true, Row.of(1, "pengpeng", 13)),
				new Tuple2<Boolean, Row>(true, Row.of(2, "xingxing", 2))
		);


		DorisSinkOptions dorisSinkOptions = new DorisSinkOptionsBuilder()
				.setFeHostPorts("10.93.6.6:8030")
				.setUsername("admin")
				.setPassword("Admin123!")
				.setTableIdentifier("example_db.test_stream_load")
				.build();


		String[] fiels = {"id", "name", "age"};
		FormatInfo[] formatInfos = new FormatInfo[]{
				IntFormatInfo.INSTANCE,
				StringFormatInfo.INSTANCE,
				StringFormatInfo.INSTANCE};
		DorisOutputFormat outputFormat = new DorisOutputFormat(dorisSinkOptions,fiels,formatInfos);

		try {
			outputFormat.open(0, 1);
			data.output(outputFormat);
			outputFormat.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		env.execute("doris batch sink example");
	}
}
