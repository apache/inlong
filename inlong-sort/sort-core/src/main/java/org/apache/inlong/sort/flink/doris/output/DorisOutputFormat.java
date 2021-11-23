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
package org.apache.inlong.sort.flink.doris.output;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.IOException;


public class DorisOutputFormat   extends RichOutputFormat<Tuple2<Boolean, Row>> {
	@Override
	public void configure(Configuration configuration) {

	}

	@Override
	public void open(int i, int i1) throws IOException {

	}

	@Override
	public void writeRecord(Tuple2<Boolean, Row> booleanRowTuple2) throws IOException {

	}

	@Override
	public void close() throws IOException {

	}
}
