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

package org.apache.inlong.sort.base.dirty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.format.DynamicSchemaFormatFactory;
import org.apache.inlong.sort.base.format.JsonDynamicSchemaFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Dirty sink helper, it helps dirty data sink for {@link DirtySink}
 * @param <T>
 */
public class DirtySinkHelper<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DirtySinkHelper.class);
    static final Pattern REGEX_PATTERN = Pattern.compile("\\$\\{\\s*([\\w.-]+)\\s*}", Pattern.CASE_INSENSITIVE);

    private DirtyOptions dirtyOptions;
    private final @Nullable DirtySink<T> dirtySink;

    public DirtySinkHelper(DirtyOptions dirtyOptions, @Nullable DirtySink<T> dirtySink) {
        this.dirtyOptions = Preconditions.checkNotNull(dirtyOptions, "dirtyOptions is null");
        this.dirtySink = dirtySink;
    }

    /**
     * Open for dirty sink
     *
     * @param configuration The configuration that is used for dirty sink
     */
    public void open(Configuration configuration) {
        if (dirtySink != null) {
            try {
                dirtySink.open(configuration);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Dirty data sink
     * @param dirtyData The dirty data
     * @param dirtyType The dirty type {@link DirtyType}
     * @param e The cause of dirty data
     */
    public void invoke(T dirtyData, DirtyType dirtyType, Throwable e) {
        if (!dirtyOptions.ignoreDirty()) {
            RuntimeException ex;
            if (e instanceof RuntimeException) {
                ex = (RuntimeException) e;
            } else {
                ex = new RuntimeException(e);
            }
            throw ex;
        }
        if (dirtySink != null) {
            DirtyData.Builder<T> builder = DirtyData.builder();
            try {
                builder.setData(dirtyData)
                        .setDirtyType(dirtyType)
                        .setLabels(dirtyOptions.getLabels())
                        .setLogTag(dirtyOptions.getLogTag())
                        .setDirtyMessage(e.getMessage())
                        .setIdentifier(dirtyOptions.getIdentifier());
                dirtySink.invoke(builder.build());
            } catch (Exception ex) {
                if (!dirtyOptions.ignoreSideOutputErrors()) {
                    throw new RuntimeException(ex);
                }
                LOGGER.warn("Dirty sink failed", ex);
            }
        }
    }

    public void invokeMultiple(String tableIdentifier, T dirtyData, DirtyType dirtyType, Throwable e,
            String sinkMultipleFormat) {
        if (!dirtyOptions.ignoreDirty()) {
            RuntimeException ex;
            if (e instanceof RuntimeException) {
                ex = (RuntimeException) e;
            } else {
                ex = new RuntimeException(e);
            }
            throw ex;
        }

        JsonDynamicSchemaFormat jsonDynamicSchemaFormat =
                (JsonDynamicSchemaFormat) DynamicSchemaFormatFactory.getFormat(sinkMultipleFormat);
        JsonNode rootNode = null;
        String[] actualIdentifier = tableIdentifier.split("\\.");;

        try {
            // for rowdata where identifier is not the first element, append identifier and SEPARATOR before it.
            if (dirtyData instanceof RowData) {
                rootNode = jsonDynamicSchemaFormat.deserialize(((RowData) dirtyData).getBinary(0));
                handleDirty(dirtyType, e, null, rootNode, jsonDynamicSchemaFormat, dirtyData);
            } else if (dirtyData instanceof JsonNode) {
                rootNode = (JsonNode) dirtyData;
                handleDirty(dirtyType, e, null, rootNode, jsonDynamicSchemaFormat, dirtyData);
            } else if (dirtyData instanceof String) {
                handleDirty(dirtyType, e, actualIdentifier, null, jsonDynamicSchemaFormat, dirtyData);
            } else {
                throw new Exception("unidentified dirty data " + dirtyData);
            }
        } catch (Exception ex) {
            LOGGER.warn("parse dirty data {} of class {} failed", dirtyData, dirtyData.getClass());
            invoke(dirtyData, DirtyType.DESERIALIZE_ERROR, e);
        }
    }

    private void handleDirty(DirtyType dirtyType, Throwable e,
            String[] actualIdentifier, JsonNode rootNode, JsonDynamicSchemaFormat jsonDynamicSchemaFormat,
            T dirtyData) {
        if (dirtySink != null) {
            DirtyData.Builder<T> builder = DirtyData.builder();
            try {
                if (rootNode != null) {
                    String labels = regexReplace(dirtyOptions.getLabels(), dirtyType, e.getMessage(), null);
                    String logTag = regexReplace(dirtyOptions.getLogTag(), dirtyType, e.getMessage(), null);
                    String identifier = regexReplace(dirtyOptions.getIdentifier(), dirtyType, e.getMessage(), null);
                    builder.setData(dirtyData)
                            .setDirtyType(dirtyType)
                            .setLabels(jsonDynamicSchemaFormat.parse(rootNode, labels))
                            .setLogTag(jsonDynamicSchemaFormat.parse(rootNode, logTag))
                            .setDirtyMessage(e.getMessage())
                            .setIdentifier(jsonDynamicSchemaFormat.parse(rootNode, identifier));
                } else {
                    // for dirty data without proper rootnode, parse completely into string literals
                    String labels = regexReplace(dirtyOptions.getLabels(), dirtyType, e.getMessage(), actualIdentifier);
                    String logTag = regexReplace(dirtyOptions.getLogTag(), dirtyType, e.getMessage(), actualIdentifier);
                    String identifier =
                            regexReplace(dirtyOptions.getIdentifier(), dirtyType, e.getMessage(), actualIdentifier);
                    builder.setData(dirtyData)
                            .setDirtyType(dirtyType)
                            .setLabels(labels)
                            .setLogTag(logTag)
                            .setDirtyMessage(e.getMessage())
                            .setIdentifier(identifier);
                }
                dirtySink.invoke(builder.build());
            } catch (Exception ex) {
                if (!dirtyOptions.ignoreSideOutputErrors()) {
                    throw new RuntimeException(ex);
                }
                LOGGER.warn("Dirty sink failed", ex);
            }
        }
    }

    public static String regexReplace(String pattern, DirtyType dirtyType,
            String dirtyMessage, String[] actualIdentifier) throws IOException {

        if (pattern == null) {
            return null;
        }

        final String DIRTY_TYPE_KEY = "DIRTY_TYPE";
        final String DIRTY_MESSAGE_KEY = "DIRTY_MESSAGE";
        final String SYSTEM_TIME_KEY = "SYSTEM_TIME";
        final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        Map<String, String> paramMap = new HashMap<>();
        paramMap.put(SYSTEM_TIME_KEY, DATE_TIME_FORMAT.format(LocalDateTime.now()));
        paramMap.put(DIRTY_TYPE_KEY, dirtyType.format());
        paramMap.put(DIRTY_MESSAGE_KEY, dirtyMessage);

        Matcher matcher = REGEX_PATTERN.matcher(pattern);

        // if RootNode is not available, generate a complete paramMap with {database} {table},etc.
        if (actualIdentifier != null) {
            int i = 0;
            while (matcher.find()) {
                try {
                    String keyText = matcher.group(1);
                    int finalI = i;
                    paramMap.computeIfAbsent(keyText, k -> actualIdentifier[finalI]);
                } catch (Exception e) {
                    throw new IOException("param map replacement failed", e);
                }
                i++;
            }
        }

        matcher = REGEX_PATTERN.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String keyText = matcher.group(1);
            String replacement = paramMap.get(keyText);
            if (replacement == null) {
                continue;
            }
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public void setDirtyOptions(DirtyOptions dirtyOptions) {
        this.dirtyOptions = dirtyOptions;
    }

    public DirtyOptions getDirtyOptions() {
        return dirtyOptions;
    }

    @Nullable
    public DirtySink<T> getDirtySink() {
        return dirtySink;
    }
}
