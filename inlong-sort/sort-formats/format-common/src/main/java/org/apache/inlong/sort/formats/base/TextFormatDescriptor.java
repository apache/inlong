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

package org.apache.inlong.sort.formats.base;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatUtils;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;

import org.apache.flink.table.descriptors.DescriptorProperties;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.inlong.sort.formats.base.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_SCHEMA;

/**
 * The base descriptor for text formats.
 *
 * @param <T> The type of the format descriptor.
 */
public abstract class TextFormatDescriptor<T extends TextFormatDescriptor>
        extends
            FormatDescriptor {

    protected final DescriptorProperties internalProperties =
            new DescriptorProperties(true);

    protected final List<String> fieldNames = new ArrayList<>();

    protected final List<FormatInfo> fieldFormatInfos = new ArrayList<>();

    protected TextFormatDescriptor(String type, int version) {
        super(type, version);
    }

    /**
     * Defines the format for the next field.
     *
     * @param fieldName The name of the field.
     * @param fieldFormatInfo The format of the field.
     */
    @SuppressWarnings("unchecked")
    public T field(String fieldName, FormatInfo fieldFormatInfo) {
        checkNotNull(fieldName);
        checkNotNull(fieldFormatInfo);
        checkNotDuplicated(fieldName);

        fieldNames.add(fieldName);
        fieldFormatInfos.add(fieldFormatInfo);

        return (T) this;
    }

    /**
     * Sets the format schema. Required if schema is not derived.
     *
     * <p>Note: This schema defined by this method will be removed if
     * {@link #field(String, FormatInfo)} is called.</p>
     *
     * @param format The format.
     */
    @SuppressWarnings("unchecked")
    public T schema(RowFormatInfo format) {
        checkNotNull(format);
        internalProperties.putString(FORMAT_SCHEMA, FormatUtils.marshall(format));
        return (T) this;
    }

    /**
     * Sets the format schema. Required if schema is not derived.
     *
     * <p>Note: This schema defined by this method will be removed if
     * {@link #field(String, FormatInfo)} is called.</p>
     *
     * @param schema The format schema.
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public T schema(String schema) {
        checkNotNull(schema);
        internalProperties.putString(FORMAT_SCHEMA, schema);
        return (T) this;
    }

    /**
     * Derives the format schema from the table's schema. Required if no format
     * schema is defined.
     *
     * <p>This allows for defining schema information only once.
     *
     * <p>The names, types, and fields' order of the format are determined by
     * the table's schema. Time attributes are ignored if their origin is not a
     * field. A "from" definition is interpreted as a field renaming in the
     * format.
     */
    @SuppressWarnings("unchecked")
    public T deriveSchema() {
        internalProperties.putBoolean(FORMAT_DERIVE_SCHEMA, true);
        return (T) this;
    }

    /**
     * Sets the charset of the text.
     *
     * @param charset The charset of the text.
     */
    @SuppressWarnings("unchecked")
    public T charset(Charset charset) {
        checkNotNull(charset);
        internalProperties.putString(FORMAT_CHARSET, charset.name());
        return (T) this;
    }

    /**
     * Sets the escape character (disabled by default).
     *
     * @param escapeCharacter escaping character (e.g. backslash).
     */
    @SuppressWarnings("unchecked")
    public T escapeCharacter(char escapeCharacter) {
        internalProperties.putCharacter(FORMAT_ESCAPE_CHARACTER, escapeCharacter);
        return (T) this;
    }

    /**
     * Sets the quote character (disabled by default).
     *
     * @param quoteCharacter quoting character (e.g. quotation).
     */
    @SuppressWarnings("unchecked")
    public T quoteCharacter(char quoteCharacter) {
        internalProperties.putCharacter(FORMAT_QUOTE_CHARACTER, quoteCharacter);
        return (T) this;
    }

    /**
     * Sets the null literal string that is interpreted as a null value
     * (disabled by default).
     *
     * @param nullLiteral null literal (e.g. "null" or "n/a")
     */
    @SuppressWarnings("unchecked")
    public T nullLiteral(String nullLiteral) {
        checkNotNull(nullLiteral);
        internalProperties.putString(FORMAT_NULL_LITERAL, nullLiteral);
        return (T) this;
    }

    /**
     * Ignores the errors in the serialization and deserialization.
     */
    @SuppressWarnings("unchecked")
    public T ignoreErrors() {
        internalProperties.putBoolean(FORMAT_IGNORE_ERRORS, true);
        return (T) this;
    }

    @Override
    public Map<String, String> toFormatProperties() {
        if (!fieldNames.isEmpty()) {
            RowFormatInfo formatInfo =
                    new RowFormatInfo(
                            fieldNames.toArray(new String[0]),
                            fieldFormatInfos.toArray(new FormatInfo[0]));

            String schema = FormatUtils.marshall(formatInfo);
            internalProperties.putString(FORMAT_SCHEMA, schema);
        }

        return internalProperties.asMap();
    }

    private void checkNotDuplicated(String newFieldName) {
        if (fieldNames.contains(newFieldName)) {
            throw new IllegalArgumentException("Field " + newFieldName + " already exists.");
        }
    }
}
