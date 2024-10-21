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

import net.sf.jsqlparser.schema.Column;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.decode.SourceDecoder;
import org.apache.inlong.sdk.transform.encode.DefaultSinkData;
import org.apache.inlong.sdk.transform.encode.SinkEncoder;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.TransformConfig;
import org.apache.inlong.sdk.transform.process.operator.ExpressionOperator;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.google.common.collect.ImmutableMap;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.StringUtils;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TransformProcessor
 *
 */
public class TransformProcessor<I, O> {

    private static final Map<String, Object> EMPTY_EXT_PARAMS = ImmutableMap.of();
    private static final String DUMMY_SELECT = "select *";

    private final TransformConfig config;
    private final SourceDecoder<I> decoder;
    private final SinkEncoder<O> encoder;

    private PlainSelect transformSelect;
    private ExpressionOperator where;
    private List<ValueParserNode> selectItems;

    private List<String> sinkFieldList;

    public static <I, O> TransformProcessor<I, O> create(
            TransformConfig config,
            SourceDecoder<I> decoder,
            SinkEncoder<O> encoder) throws JSQLParserException {
        return new TransformProcessor<>(config, decoder, encoder);
    }

    private TransformProcessor(
            TransformConfig config,
            SourceDecoder<I> decoder,
            SinkEncoder<O> encoder)
            throws JSQLParserException {
        this.config = config;
        this.decoder = decoder;
        this.encoder = encoder;
        this.init();
    }

    private void init() throws JSQLParserException {
        if (!config.isStrictOrder() && encoder != null && encoder.getFields() != null) {
            List<FieldInfo> fields = encoder.getFields();
            this.sinkFieldList = new ArrayList<>(fields.size());
            fields.forEach(v -> this.sinkFieldList.add(v.getName()));
        }

        if (StringUtils.isNotEmpty(config.getTransformSql())) {
            this.initTransformSql(config.getTransformSql());
        } else {
            this.initTransformSql(DUMMY_SELECT);
        }
    }

    private void initTransformSql(String sql) throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Select select = (Select) parserManager.parse(new StringReader(sql));
        this.transformSelect = (PlainSelect) select.getSelectBody();
        this.where = OperatorTools.buildOperator(this.transformSelect.getWhere());
        List<SelectItem> items = this.transformSelect.getSelectItems();
        this.selectItems = new ArrayList<>(items.size());
        List<FieldInfo> fields = this.encoder.getFields();
        for (int i = 0; i < items.size(); i++) {
            SelectItem item = items.get(i);
            String fieldName = null;
            if (config.isStrictOrder() && i < fields.size()) {
                fieldName = fields.get(i).getName();
            }
            if (item instanceof SelectExpressionItem) {
                SelectExpressionItem exprItem = (SelectExpressionItem) item;
                if (fieldName == null) {
                    if (exprItem.getAlias() == null) {
                        fieldName = exprItem.toString();
                    } else {
                        fieldName = exprItem.getAlias().getName();
                    }
                    if (!this.checkSelectField(fieldName)) {
                        throw new JSQLParserException(
                                String.format("Field name:%s can not be found in sink field list.", fieldName));
                    }
                }
                this.selectItems
                        .add(new ValueParserNode(fieldName, OperatorTools.buildParser(exprItem.getExpression())));
            } else if (item instanceof AllColumns) {
                for (FieldInfo fieldInfo : decoder.getFields()) {
                    String name = fieldInfo.getName();
                    this.selectItems.add(new ValueParserNode(name, new ColumnParser(new Column(name))));
                }
            }
        }
    }

    public boolean checkSelectField(String fieldName) {
        if (config.isIgnoreConfigError()) {
            return true;
        }
        return this.sinkFieldList != null && this.sinkFieldList.contains(fieldName);
    }

    public List<O> transform(I input) {
        return this.transform(input, EMPTY_EXT_PARAMS);
    }

    public List<O> transform(I input, Map<String, Object> extParams) {
        Context context = new Context(config.getConfiguration(), extParams);

        // decode
        SourceData sourceData = this.decoder.decode(input, context);
        if (sourceData == null) {
            return null;
        }

        List<O> sinkDatas = new ArrayList<>(sourceData.getRowCount());
        for (int i = 0; i < sourceData.getRowCount(); i++) {

            // where check
            if (this.where != null && !this.where.check(sourceData, i, context)) {
                continue;
            }

            // parse value
            DefaultSinkData sinkData = new DefaultSinkData();
            for (ValueParserNode node : this.selectItems) {
                String fieldName = node.getFieldName();
                ValueParser parser = node.getParser();
                if (parser == null || StringUtils.equals(fieldName, SinkEncoder.ALL_SOURCE_FIELD_SIGN)) {
                    if (input instanceof String) {
                        sinkData.addField(fieldName, (String) input);
                    } else {
                        sinkData.addField(fieldName, "");
                    }
                    continue;
                }
                try {
                    Object fieldValue = parser.parse(sourceData, i, context);
                    if (fieldValue == null) {
                        sinkData.addField(fieldName, "");
                    } else {
                        sinkData.addField(fieldName, fieldValue.toString());
                    }
                } catch (Throwable t) {
                    sinkData.addField(fieldName, "");
                }
            }

            if (this.sinkFieldList != null) {
                sinkData.setKeyList(this.sinkFieldList);
            }
            // encode
            sinkDatas.add(this.encoder.encode(sinkData, context));
        }
        return sinkDatas;
    }

}
