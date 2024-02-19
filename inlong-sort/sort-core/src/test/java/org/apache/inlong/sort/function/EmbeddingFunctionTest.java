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

package org.apache.inlong.sort.function;

import org.apache.inlong.sort.function.embedding.EmbeddingInput;
import org.apache.inlong.sort.function.embedding.LanguageModel;

import com.sun.net.httpserver.HttpServer;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.inlong.sort.function.EmbeddingFunction.DEFAULT_EMBEDDING_FUNCTION_NAME;

public class EmbeddingFunctionTest extends AbstractTestBase {

    @Test
    public void testMapper() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        EmbeddingInput embeddingInput = new EmbeddingInput("Input-Test", "Model-Test");
        String encodedContents = mapper.writeValueAsString(embeddingInput);
        String expect = "{\"input\":\"Input-Test\",\"model\":\"Model-Test\"}";
        Assert.assertEquals(encodedContents, expect);
    }

    @Test
    public void testLanguageModel() {
        String supportedLMs = LanguageModel.getAllSupportedLanguageModels();
        Assert.assertNotNull(supportedLMs);
        String[] supportLMArray = supportedLMs.split(",");
        Assert.assertEquals(supportLMArray.length, LanguageModel.values().length);

        Assert.assertTrue(LanguageModel.isLanguageModelSupported("BAAI/bge-large-zh-v1.5"));
        Assert.assertTrue(LanguageModel.isLanguageModelSupported("BAAI/bge-large-en"));
        Assert.assertTrue(LanguageModel.isLanguageModelSupported("intfloat/multilingual-e5-large"));
        Assert.assertFalse(LanguageModel.isLanguageModelSupported("fake/fake-language"));
    }

    /**
     * Test for embedding function
     *
     * @throws Exception The exception may throw when test Embedding function
     */
    @Test
    public void testEmbeddingFunction() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // step 1. Register custom function of Embedding
        tableEnv.createTemporaryFunction(DEFAULT_EMBEDDING_FUNCTION_NAME, EmbeddingFunction.class);

        List<String> udfNames = Arrays.asList(tableEnv.listUserDefinedFunctions());
        Assert.assertTrue(udfNames.contains(DEFAULT_EMBEDDING_FUNCTION_NAME.toLowerCase(Locale.ROOT)));

        // step 2. Generate test data and convert to DataStream
        int numOfMessages = 100;
        List<String> sourceDateList = new ArrayList<>();
        String msgPrefix = "Data for embedding-";
        for (int i = 0; i < numOfMessages; i++) {
            sourceDateList.add(msgPrefix + i);
        }

        List<Row> data = new ArrayList<>();
        sourceDateList.forEach(s -> data.add(Row.of(s)));
        TypeInformation<?>[] types = {BasicTypeInfo.STRING_TYPE_INFO};
        String[] names = {"f1"};
        RowTypeInfo typeInfo = new RowTypeInfo(types, names);
        DataStream<Row> dataStream = env.fromCollection(data).returns(typeInfo);

        // step 3. start a web server to mock embedding service
        String embeddingResult = "{\"result\": \"Result data for embedding\"}";
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(8899), 0); // or use InetSocketAddress(0) for
                                                                                   // ephemeral port
        httpServer.createContext("/get_embedding", exchange -> {
            byte[] response = embeddingResult.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        httpServer.start();

        // step 4. Convert from DataStream to Table and execute the Embedding function
        Table tempView = tableEnv.fromDataStream(dataStream).as("f1");
        tableEnv.createTemporaryView("temp_view", tempView);
        Table outputTable = tableEnv.sqlQuery(
                "SELECT " +
                        "f1," +
                        "EMBEDDING('http://localhost:8899/get_embedding', f1, 'BAAI/bge-large-en') " +
                        "from temp_view");

        // step 5. Get function execution result and parse it
        DataStream<Row> resultSet = tableEnv.toAppendStream(outputTable, Row.class);
        List<String> resultF0 = new ArrayList<>();
        List<String> resultF1 = new ArrayList<>();
        for (CloseableIterator<Row> it = resultSet.executeAndCollect(); it.hasNext();) {
            Row row = it.next();
            if (row != null) {
                resultF0.add(row.getField(0).toString());
                resultF1.add(row.getField(1).toString());
            }
        }
        Assert.assertEquals(resultF0.size(), numOfMessages);
        Assert.assertEquals(resultF1.size(), numOfMessages);
        Assert.assertEquals(resultF0, sourceDateList);
        for (String res : resultF1) {
            Assert.assertEquals(res, embeddingResult);
        }

        httpServer.stop(0);
    }
}
