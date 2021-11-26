/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.inlong.sort.flink.doris;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.flink.doris.output.DorisOutputFormat;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;


public class TestDorisSink {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);



		final DataSource<Tuple2<Boolean, Row>> data = env.fromElements(
				new Tuple2<Boolean, Row>(true, Row.of(1, "pengpeng", 13)),
				new Tuple2<Boolean, Row>(true, Row.of(2, "xingxing", 2))
		);


		DorisSinkOptions dorisSinkOptions = new DorisSinkOptionsBuilder()
				.setFeNodes("10.93.6.6:8030")
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
