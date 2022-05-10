package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

public class InLongMsgOptions {
    private InLongMsgOptions() {
    }

    public static final ConfigOption<String> INNER_FORMAT =
            ConfigOptions.key("inner.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Defines the format identifier for encoding attr data. \n"
                            + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                        + "fields are set to null in case of errors");

    public static void validateDecodingFormatOptions(ReadableConfig config) {
        String innerFormat = config.get(INNER_FORMAT);
        if (innerFormat == null) {
            throw new ValidationException(
                    INNER_FORMAT.key()  + " shouldn't be null.");
        }
    }
}
