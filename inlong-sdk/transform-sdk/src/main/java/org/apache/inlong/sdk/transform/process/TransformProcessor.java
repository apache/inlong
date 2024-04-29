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

package org.apache.inlong.sdk.transform.process;

import org.apache.inlong.sdk.transform.decode.CsvSourceDecoder;
import org.apache.inlong.sdk.transform.decode.KvSourceDecoder;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.decode.SourceDecoder;
import org.apache.inlong.sdk.transform.encode.CsvSinkEncoder;
import org.apache.inlong.sdk.transform.encode.DefaultSinkData;
import org.apache.inlong.sdk.transform.encode.KvSinkEncoder;
import org.apache.inlong.sdk.transform.encode.SinkData;
import org.apache.inlong.sdk.transform.encode.SinkEncoder;
import org.apache.inlong.sdk.transform.pojo.CsvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.pojo.KvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.SinkInfo;
import org.apache.inlong.sdk.transform.pojo.SourceInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.operator.ExpressionOperator;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.StringUtils;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * TransformProcessor
 * 
 */
public class TransformProcessor {

    private TransformConfig config;
    private SourceDecoder decoder;
    private SinkEncoder encoder;
    private Charset srcCharset = Charset.defaultCharset();
    protected Charset sinkCharset = Charset.defaultCharset();

    private PlainSelect transformSelect;
    private ExpressionOperator where;
    private Map<String, ValueParser> selectItemMap;

    private ObjectMapper objectMapper = new ObjectMapper();

    public TransformProcessor(String configString)
            throws JsonMappingException, JsonProcessingException, JSQLParserException {
        TransformConfig config = this.objectMapper.readValue(configString, TransformConfig.class);
        this.init(config);
    }

    public TransformProcessor(TransformConfig config) throws JSQLParserException {
        this.init(config);
    }

    private void init(TransformConfig config) throws JSQLParserException {
        this.config = config;
        if (!StringUtils.isBlank(config.getSourceInfo().getCharset())) {
            this.srcCharset = Charset.forName(config.getSourceInfo().getCharset());
        }
        if (!StringUtils.isBlank(config.getSinkInfo().getCharset())) {
            this.sinkCharset = Charset.forName(config.getSinkInfo().getCharset());
        }
        this.initDecoder(config);
        this.initEncoder(config);
        this.initTransformSql();
    }

    private void initDecoder(TransformConfig config) {
        SourceInfo sourceInfo = config.getSourceInfo();
        if (sourceInfo instanceof CsvSourceInfo) {
            this.decoder = new CsvSourceDecoder((CsvSourceInfo) sourceInfo);
        } else if (sourceInfo instanceof KvSourceInfo) {
            this.decoder = new KvSourceDecoder((KvSourceInfo) sourceInfo);
        }
    }

    private void initEncoder(TransformConfig config) {
        SinkInfo sinkInfo = config.getSinkInfo();
        if (sinkInfo instanceof CsvSinkInfo) {
            this.encoder = new CsvSinkEncoder((CsvSinkInfo) sinkInfo);
        } else if (sinkInfo instanceof KvSinkInfo) {
            this.encoder = new KvSinkEncoder((KvSinkInfo) sinkInfo);
        }
    }

    private void initTransformSql() throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Select select = (Select) parserManager.parse(new StringReader(config.getTransformSql()));
        this.transformSelect = (PlainSelect) select.getSelectBody();
        this.where = OperatorTools.buildOperator(this.transformSelect.getWhere());
        List<SelectItem> items = this.transformSelect.getSelectItems();
        this.selectItemMap = new HashMap<>(items.size());
        for (SelectItem item : items) {
            if (item instanceof SelectExpressionItem) {
                SelectExpressionItem exprItem = (SelectExpressionItem) item;
                if (exprItem.getAlias() == null) {
                    this.selectItemMap.put(exprItem.toString(),
                            OperatorTools.buildParser(exprItem.getExpression()));
                } else {
                    this.selectItemMap.put(exprItem.getAlias().getName(),
                            OperatorTools.buildParser(exprItem.getExpression()));
                }
            }
        }
    }

    public List<String> transform(byte[] srcBytes, Map<String, Object> extParams) {
        SourceData sourceData = this.decoder.decode(srcBytes, extParams);
        List<String> sinkDatas = new ArrayList<>(sourceData.getRowCount());
        for (int i = 1; i <= sourceData.getRowCount(); i++) {
            if (!this.where.check(sourceData, i)) {
                continue;
            }
            SinkData sinkData = new DefaultSinkData();
            for (Entry<String, ValueParser> entry : this.selectItemMap.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue().parse(sourceData, i);
                sinkData.putField(fieldName, String.valueOf(fieldValue));
            }
            sinkDatas.add(this.encoder.encode(sinkData));
        }
        return sinkDatas;
    }

    public List<String> transform(String srcString, Map<String, Object> extParams) {
        return this.transform(srcString.getBytes(this.srcCharset), extParams);
    }
}
