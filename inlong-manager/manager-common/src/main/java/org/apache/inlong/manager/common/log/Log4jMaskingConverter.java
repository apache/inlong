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

package org.apache.inlong.manager.common.log;

import java.util.Arrays;
import org.apache.inlong.manager.common.log.converter.MaskingConverter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

@Plugin(name = "logmasker", category = PatternConverter.CATEGORY)
@ConverterKeys({"msk", "mask"})
public class Log4jMaskingConverter extends LogEventPatternConverter {
    private MaskingConverter maskingConverter;

    private Log4jMaskingConverter(MaskingConverter maskingConverter) {
        super("Log4jMaskingConverter", null);
        this.maskingConverter = maskingConverter;
    }

    public static Log4jMaskingConverter newInstance(String[] options) {
        MaskingConverter maskingConverter = new MaskingConverter();
        if (options == null) {
            maskingConverter.init(null);
        } else {
            maskingConverter.init(Arrays.asList(options));
        }
        return new Log4jMaskingConverter(maskingConverter);
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        StringBuilder logMessage = new StringBuilder(event.getMessage().getFormattedMessage());
        maskingConverter.mask(logMessage);
        toAppendTo.append(logMessage);
    }

}
