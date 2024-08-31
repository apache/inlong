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

package org.apache.inlong.agent.plugin.utils;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

public class SQLServerTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLServerTimeConverter.class);

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneOffset defalutZoneOffset = ZoneOffset.systemDefault().getRules().getOffset(Instant.now());

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> defalutZoneOffset = ZoneOffset.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            LOGGER.error("The {} setting is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        if ("DATE".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("org.apache.inlong.agent.date.string");
            converter = this::convertDate;
        }
        if ("TIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("org.apache.inlong.agent.time.string");
            converter = this::convertTime;
        }
        if ("DATETIME".equals(sqlType) ||
                "DATETIME2".equals(sqlType) ||
                "SMALLDATETIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("org.apache.inlong.agent.datetime.string");
            converter = this::convertDateTime;
        }
        if ("DATETIMEOFFSET".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("org.apache.inlong.agent.datetimeoffset.string");
            converter = this::convertDateTimeOffset;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
            LOGGER.info("register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
        }
    }

    private String convertDate(Object input) {
        if (input instanceof java.sql.Date) {
            return dateFormatter.format(((java.sql.Date) input).toLocalDate());
        }
        return input == null ? null : input.toString();
    }

    private String convertTime(Object input) {
        if (input instanceof java.sql.Time) {
            return timeFormatter.format(((java.sql.Time) input).toLocalTime());
        } else if (input instanceof java.sql.Timestamp) {
            return timeFormatter.format(((java.sql.Timestamp) input).toLocalDateTime().toLocalTime());
        }
        return input == null ? null : input.toString();
    }

    private String convertDateTime(Object input) {
        if (input instanceof java.sql.Timestamp) {
            return datetimeFormatter.format(((java.sql.Timestamp) input).toLocalDateTime());
        }
        return input == null ? null : input.toString();
    }

    private String convertDateTimeOffset(Object input) {
        if (input instanceof microsoft.sql.DateTimeOffset) {
            microsoft.sql.DateTimeOffset dateTimeOffset = (microsoft.sql.DateTimeOffset) input;
            return datetimeFormatter.format(
                    dateTimeOffset.getOffsetDateTime().withOffsetSameInstant(defalutZoneOffset).toLocalDateTime());
        }
        return input == null ? null : input.toString();
    }
}
