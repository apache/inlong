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

package org.apache.inlong.sort.flink.doris.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.flink.doris.DorisSinkOptions;
import org.apache.inlong.sort.flink.doris.load.DorisConnectParam;
import org.apache.inlong.sort.flink.doris.load.DorisHttpUtils;
import org.apache.inlong.sort.flink.doris.load.DorisRowConverter;
import org.apache.inlong.sort.flink.doris.load.DorisStreamLoad;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DorisOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisOutputFormat.class);

    private DorisSinkOptions dorisSinkOptions;
    private DorisStreamLoad dorisStreamLoad;
    private final List<String> batch = new ArrayList<>();
    private boolean closed = false;

    private final String[] fieldNames;

    private final FormatInfo[] formatInfos;

    public DorisOutputFormat(DorisSinkOptions dorisSinkOptions,
                             String[] fieldNames,
                             FormatInfo[] formatInfos) {
        this.dorisSinkOptions = dorisSinkOptions;
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.formatInfos = Preconditions.checkNotNull(formatInfos);
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {

        // init doris stream load
        String streamLoadUrl = getNewStreamLoadUrl();
        final DorisConnectParam dorisConnectParam = new DorisConnectParam(streamLoadUrl,
                dorisSinkOptions.getUsername(), dorisSinkOptions.getPassword(), dorisSinkOptions.getDatabase(),
                dorisSinkOptions.getTable(), dorisSinkOptions.getStreamLoadProp()
        );
        dorisStreamLoad = new DorisStreamLoad(dorisConnectParam);
        LOG.info("Get doris stream load url :" + streamLoadUrl);
        // todo init scheduler flush
    }

    private String getNewBeHostPort() {
        return DorisHttpUtils.getRandomBeNode(dorisSinkOptions.getFeHostPorts(),
                dorisSinkOptions.getMaxRetries(),
                dorisSinkOptions.getUsername(), dorisSinkOptions.getPassword());
    }

    private String getNewStreamLoadUrl() {
        // convert hostPort to loadUrlStr
        String loadUrlPattern = "http://<hostPort>/api/<database>/<tableName>/_stream_load?";
        final ST st = new ST(loadUrlPattern);
        st.add("hostPort", getNewBeHostPort());
        st.add("database", dorisSinkOptions.getDatabase());
        st.add("tableName", dorisSinkOptions.getTable());
        return st.render();
    }

    @Override
    public void writeRecord(Tuple2<Boolean, Row> booleanRowTuple2) throws IOException {

        // add json to batch
        addBatch(booleanRowTuple2);
        // judge if need to flush batch
        if (batch.size() >= dorisSinkOptions.getBatchSize()) {
            flush();
        }
    }

    private void addBatch(Tuple2<Boolean, Row> booleanRowTuple2) {
        final Row row = booleanRowTuple2.f1;
        Map<String, Object> rowDataMap = new HashMap<>();
        DorisRowConverter.setRow(formatInfos, fieldNames, row, rowDataMap);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        final String rowDataJsonString = gson.toJson(rowDataMap);
        batch.add(rowDataJsonString);
    }

    /**
     * every batch commit to doris with a few retry
     *
     * @throws IOException
     */
    private synchronized void flush() throws IOException {
        if (batch.isEmpty()) {
            return;
        }
        // covert batch to json string
        String result = batch.stream().collect(Collectors.joining(",", "[", "]"));

        boolean abnormal = false;
        String chooseNewStreamLoadUrl = null;
        for (int i = 0; i <= dorisSinkOptions.getMaxRetries(); i++) {
            try {
                if (!abnormal) {
                    dorisStreamLoad.load(result);
                } else {
                    dorisStreamLoad.load(result, chooseNewStreamLoadUrl);
                }
                batch.clear();
                break;
            } catch (Exception e) {
                LOG.error("doris sink error, retry times = {}", i, e);
                if (i >= dorisSinkOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    abnormal = true;
                    chooseNewStreamLoadUrl = getNewStreamLoadUrl();
                    LOG.warn("Stream load error,switch new  stream load url : {}", chooseNewStreamLoadUrl, e);
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (batch.size() > 0) {
                flush();
            }
        }

    }

}
