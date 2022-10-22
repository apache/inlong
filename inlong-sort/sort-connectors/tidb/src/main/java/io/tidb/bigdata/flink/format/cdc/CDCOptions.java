/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.format.cdc;

import io.tidb.bigdata.cdc.Key;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

public class CDCOptions {

  public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
      ConfigOptions.key("ignore-parse-errors").booleanType().defaultValue(false)
          .withDescription(
              "Optional flag to skip change events with parse errors instead of failing;\n"
                  + "fields are set to null in case of errors, false by default.");

  public static final ConfigOption<String> SCHEMA_INCLUDE =
      ConfigOptions.key("schema.include").stringType().noDefaultValue()
          .withDescription("Only read events belong to the specific schema");

  public static final ConfigOption<String> TABLE_INCLUDE =
      ConfigOptions.key("table.include").stringType().noDefaultValue()
          .withDescription("Only read events belong to the specific table");

  public static final ConfigOption<String> TYPE_INCLUDE =
      ConfigOptions.key("type.include").stringType().noDefaultValue()
          .withDescription("Only read events of some specific types");

  public static final ConfigOption<Long> EARLIEST_VERSION =
      ConfigOptions.key("earliest.version").longType().noDefaultValue()
          .withDescription("Only read events older than given version");

  public static final ConfigOption<Long> EARLIEST_TIMESTAMP =
      ConfigOptions.key("earliest.timestamp").longType().defaultValue(0L)
          .withDescription("Only read events older than given timestamp in milliseconds");

  /**
   * Validator for craft decoding format.
   */
  public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
  }

  private static boolean isNotEmpty(String str) {
    return str != null && !str.isEmpty();
  }

  private static Optional<Stream<String>> getOptionalStream(final ReadableConfig config,
      final ConfigOption<String> key) {
    return config.getOptional(key)
        .map(l -> Arrays.stream(l.split("[ ]*[,;|][ ]*"))
            .filter(CDCOptions::isNotEmpty));
  }

  static Set<String> getOptionalSet(final ReadableConfig config,
      final ConfigOption<String> key) {
    return getOptionalStream(config, key)
        .map(l -> l.collect(Collectors.toSet())).orElse(null);
  }

  static <R> Set<R> getOptionalSet(final ReadableConfig config,
      final ConfigOption<String> key,
      final Function<String, R> mapper) {
    return getOptionalStream(config, key)
        .map(s -> s.map(mapper).collect(Collectors.toSet())).orElse(null);
  }

  static long getEarliestTs(final ReadableConfig config) {
    return config.getOptional(EARLIEST_VERSION)
        .orElseGet(() -> Key.fromTimestamp(config.get(EARLIEST_TIMESTAMP)));
  }
}