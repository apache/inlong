/*
 * Copyright 2020 TiDB Project Authors.
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

package io.tidb.bigdata.jdbc;

/**
 * jdbc:tidb://host:port/database
 */
public class TiDBDriver extends LoadBalancingDriver {

  public static final String TIDB_URL_PREFIX = "jdbc:tidb://";

  static {
    ExceptionHelper.uncheckedRun(() -> java.sql.DriverManager.registerDriver(new TiDBDriver()));
  }

  public TiDBDriver() {
    super(TIDB_URL_PREFIX);
  }

  public static String driverForUrl(final String url) {
    return url.startsWith(TIDB_URL_PREFIX) ? TiDBDriver.class.getName() : getMySqlDriverName();
  }
}