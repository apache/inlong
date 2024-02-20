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

import com.google.common.base.Strings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedding function for LLM applications.
 * */
public class EmbeddingFunction extends ScalarFunction {

    public static final Logger logger = LoggerFactory.getLogger(EmbeddingFunction.class);
    public static final String DEFAULT_EMBEDDING_FUNCTION_NAME = "EMBEDDING";

    private final ObjectMapper mapper = new ObjectMapper();
    public static final int DEFAULT_CONNECT_TIMEOUT = 30000;
    public static final int DEFAULT_SOCKET_TIMEOUT = 30000;
    public static final String DEFAULT_MODEL = LanguageModel.BBAI_ZH.getModel();
    private transient HttpClient httpClient;

    /**
     * Embedding a LLM document(a String object for now) via http protocol
     * @param url the service url for embedding service
     * @param input the source data for embedding
     * @param model the language model supported in the embedding service
     * */
    public String eval(String url, String input, String model) {
        // url and input is not null
        if (Strings.isNullOrEmpty(url) || Strings.isNullOrEmpty(input)) {
            logger.error("Failed to embedding, both url and input can't be empty or null, url: {}, input: {}",
                    url, input);
            return null;
        }

        if (Strings.isNullOrEmpty(model)) {
            model = DEFAULT_MODEL;
            logger.info("model is null, use default model: {}", model);
        }

        if (!LanguageModel.isLanguageModelSupported(model)) {
            logger.error("Failed to embedding, language model {} not supported(only {} are supported right now)",
                    model, LanguageModel.getAllSupportedLanguageModels());
            return null;
        }

        // initialize httpClient
        if (httpClient == null) {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
                    .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT)
                    .build();
            httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        }

        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            EmbeddingInput embeddingInput = new EmbeddingInput(input, model);
            String encodedContents = mapper.writeValueAsString(embeddingInput);
            httpPost.setEntity(new StringEntity(encodedContents));
            HttpResponse response = httpClient.execute(httpPost);

            String returnStr = EntityUtils.toString(response.getEntity());
            int returnCode = response.getStatusLine().getStatusCode();
            if (Strings.isNullOrEmpty(returnStr) || HttpStatus.SC_OK != returnCode) {
                throw new Exception("Failed to embedding, result: " + returnStr + ", code: " + returnCode);
            }
            return returnStr;
        } catch (Exception e) {
            logger.error("Failed to embedding, url: {}, input: {}", url, input, e);
            return null;
        }
    }
}