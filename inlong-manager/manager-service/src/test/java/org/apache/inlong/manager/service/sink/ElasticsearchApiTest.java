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

package org.apache.inlong.manager.service.sink;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchFieldInfo;
import org.apache.inlong.manager.service.resource.sink.es.ElasticsearchApi;
import org.apache.inlong.manager.service.resource.sink.es.ElasticsearchConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import sun.misc.BASE64Encoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test class for {@link org.apache.inlong.manager.service.resource.sink.es.ElasticsearchApi}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ElasticsearchApiTest {

    @InjectMocks
    private ElasticsearchConfig elasticsearchConfig;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ResponseEntity<String> exchange;

    private ElasticsearchApi elasticsearchApi;

    public static final String DEFAULT_INDEXNAME = "test_index";

    private static final Gson GSON = new GsonBuilder().create();

    private static final String CONTENT_TYPE_KEY = "Content-Type";

    private static final String CONTENT_TYPE_VALUE = "application/json;charset=UTF-8";

    @BeforeEach
    public void before() {
        elasticsearchConfig.setHosts("http://127.0.0.1:9200");
        elasticsearchConfig.setAuthEnable(true);
        elasticsearchConfig.setUsername("admin");
        elasticsearchConfig.setPassword("inlong");
        elasticsearchApi = new ElasticsearchApi();
        elasticsearchApi.setEsConfig(elasticsearchConfig);
    }

    /**
     * Test cases for {@link ElasticsearchApi#search(String, JsonObject)}.
     *
     * @throws Exception
     */
    @Test
    public void testSearch() throws Exception {
        final String url = elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH + DEFAULT_INDEXNAME + "/_search";
        final String resultJson = "{\n"
                + "  \"took\" : 2,\n"
                + "  \"timed_out\" : false,\n"
                + "  \"_shards\" : {\n"
                + "    \"total\" : 1,\n"
                + "    \"successful\" : 1,\n"
                + "    \"skipped\" : 0,\n"
                + "    \"failed\" : 0\n"
                + "  },\n"
                + "  \"hits\" : {\n"
                + "    \"total\" : {\n"
                + "      \"value\" : 3,\n"
                + "      \"relation\" : \"eq\"\n"
                + "    },"
                + "  \"max_score\" : 1.0,\n"
                + "    \"hits\" : []"
                + "  }\n"
                + "}\n";
        final String searchJson = "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}";
        final JsonObject expected = GSON.fromJson(resultJson, JsonObject.class);
        final JsonObject search = GSON.fromJson(searchJson, JsonObject.class);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getBody()).thenReturn(resultJson);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        JsonObject result = elasticsearchApi.search(DEFAULT_INDEXNAME, search);
        assertEquals(expected, result);
    }

    /**
     * Test cases for {@link ElasticsearchApi#indexExists(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testIndexExists() throws Exception {
        final String url = elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH + DEFAULT_INDEXNAME;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.HEAD), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        boolean result = elasticsearchApi.indexExists(DEFAULT_INDEXNAME);
        assertEquals(true, result);
    }

    /**
     * Test cases for {@link ElasticsearchApi#ping()}.
     *
     * @throws Exception
     */
    @Test
    public void testPing() throws Exception {
        final String url = elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH;
        when(restTemplate.exchange(eq(url), eq(HttpMethod.HEAD), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);

        boolean result = elasticsearchApi.ping();
        assertEquals(true, result);
    }

    /**
     * Test cases for {@link ElasticsearchApi#createIndex(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateIndex() throws Exception {
        final String url = elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH + DEFAULT_INDEXNAME;
        final String createJson = "{\n"
                + "  \"acknowledged\" : true,\n"
                + "  \"shards_acknowledged\" : true,\n"
                + "  \"index\" : \"test_index\"\n"
                + "}";
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(exchange.getBody()).thenReturn(createJson);
        elasticsearchApi.createIndex(DEFAULT_INDEXNAME);

        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(nullValue()));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(getHeaders())));
    }

    /**
     * Test cases for {@link ElasticsearchApi#createIndexAndMapping(String, List)}.
     *
     * @throws Exception
     */
    @Test
    public void testCreateIndexAndMapping() throws Exception {
        final String url = elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH + DEFAULT_INDEXNAME;
        final String createJson = "{\n"
                + "  \"acknowledged\" : true,\n"
                + "  \"shards_acknowledged\" : true,\n"
                + "  \"index\" : \"test_index\"\n"
                + "}";
        final List<ElasticsearchFieldInfo> fieldInfos = new ArrayList<>();
        final ElasticsearchFieldInfo log_ts = new ElasticsearchFieldInfo();
        log_ts.setFieldType("keyword");
        log_ts.setFieldName("log_ts");
        fieldInfos.add(log_ts);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(exchange.getBody()).thenReturn(createJson);
        elasticsearchApi.createIndexAndMapping(DEFAULT_INDEXNAME, fieldInfos);

        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(notNullValue()));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(getHeaders())));
    }

    /**
     * Test cases for {@link ElasticsearchApi#getMappingMap(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetMappingInfo() throws Exception {
        final String url =
                elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH + DEFAULT_INDEXNAME + "/_mapping";
        final String mappingJson = "{\n"
                + "  \"test_index\" : {\n"
                + "    \"mappings\" : {\n"
                + "      \"properties\" : {\n"
                + "        \"count\" : {\n"
                + "          \"type\" : \"double\"\n"
                + "        },\n"
                + "        \"date\" : {\n"
                + "          \"type\" : \"date\",\n"
                + "          \"format\" : \"yyyy-MM-dd HH:mm:ss||yyy-MM-dd||epoch_millis\"\n"
                + "        },\n"
                + "        \"delay\" : {\n"
                + "          \"type\" : \"double\"\n"
                + "        },\n"
                + "        \"inlong_group_id\" : {\n"
                + "          \"type\" : \"text\"\n"
                + "        },\n"
                + "        \"inlong_stream_id\" : {\n"
                + "          \"type\" : \"text\"\n"
                + "        },\n"
                + "        \"log_ts\" : {\n"
                + "          \"type\" : \"keyword\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";

        when(restTemplate.exchange(eq(url), eq(HttpMethod.GET), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(exchange.getBody()).thenReturn(mappingJson);
        Map<String, Map<String, String>> result = elasticsearchApi.getMappingMap(DEFAULT_INDEXNAME);
        assertEquals("double", result.get("count").get("type"));
        assertEquals("double", result.get("delay").get("type"));
        assertEquals("date", result.get("date").get("type"));
        assertEquals("yyyy-MM-dd HH:mm:ss||yyy-MM-dd||epoch_millis", result.get("date").get("format"));
        assertEquals("text", result.get("inlong_stream_id").get("type"));
        assertEquals("text", result.get("inlong_group_id").get("type"));
        assertEquals("keyword", result.get("log_ts").get("type"));
    }

    /**
     * Test cases for {@link ElasticsearchApi#addFields(String, List)}.
     *
     * @throws Exception
     */
    @Test
    public void testAddFields() throws Exception {
        final String url =
                elasticsearchConfig.getOneHttpUrl() + InlongConstants.SLASH + DEFAULT_INDEXNAME + "/_mapping";
        final String mappingJson = "{\n"
                + "  \"test_index\" : {\n"
                + "    \"mappings\" : {\n"
                + "      \"properties\" : {\n"
                + "        \"count\" : {\n"
                + "          \"type\" : \"double\"\n"
                + "        },\n"
                + "        \"date\" : {\n"
                + "          \"type\" : \"date\",\n"
                + "          \"format\" : \"yyyy-MM-dd HH:mm:ss||yyy-MM-dd||epoch_millis\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        final List<ElasticsearchFieldInfo> fieldInfos = new ArrayList<>();
        final ElasticsearchFieldInfo log_ts = new ElasticsearchFieldInfo();
        log_ts.setFieldType("keyword");
        log_ts.setFieldName("log_ts");
        fieldInfos.add(log_ts);
        when(restTemplate.exchange(eq(url), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class))).thenReturn(
                exchange);
        when(exchange.getStatusCode()).thenReturn(HttpStatus.OK);
        when(exchange.getBody()).thenReturn(mappingJson);

        elasticsearchApi.addFields(DEFAULT_INDEXNAME, fieldInfos);
        ArgumentCaptor<HttpEntity> requestEntity = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).exchange(eq(url), eq(HttpMethod.PUT), requestEntity.capture(), eq(String.class));
        assertThat(requestEntity.getValue().getBody(), allOf(notNullValue()));
        assertThat(requestEntity.getValue().getHeaders(), allOf(notNullValue(), is(getHeaders())));

    }

    private HttpHeaders getHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(CONTENT_TYPE_KEY, CONTENT_TYPE_VALUE);
        if (elasticsearchConfig.getAuthEnable()) {
            if (StringUtils.isNotEmpty(elasticsearchConfig.getUsername()) && StringUtils.isNotEmpty(
                    elasticsearchConfig.getPassword())) {
                String tokenStr = elasticsearchConfig.getUsername() + ":" + elasticsearchConfig.getPassword();
                String token = String.valueOf(new BASE64Encoder().encode(tokenStr.getBytes(StandardCharsets.UTF_8)));
                headers.add("Authorization", "Basic " + token);
            }
        }
        return headers;
    }

    /**
     * The case only supports local testing.
     *
     * @throws Exception
     */
    @Ignore
    public void testLocal() throws Exception {
        ElasticsearchConfig config = new ElasticsearchConfig();
        config.setHosts("http://127.0.0.1:9200");
        ElasticsearchApi api = new ElasticsearchApi();
        api.setEsConfig(config);
        api.ping();
        api.createIndex(DEFAULT_INDEXNAME);
        api.getMappingMap(DEFAULT_INDEXNAME);
    }
}
