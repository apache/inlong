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

package io.tidb.bigdata.tidb;

import java.net.URI;
import java.net.URISyntaxException;
import org.tikv.common.HostMapping;

/**
 * Host mapping implementation that add extra dns search suffix
 */
public class DnsSearchHostMapping implements HostMapping {
  private final String dnsSearch;

  public DnsSearchHostMapping(String dnsSearch) {
    if (dnsSearch != null && !dnsSearch.isEmpty()) {
      this.dnsSearch = "." + dnsSearch;
    } else {
      this.dnsSearch = "";
    }
  }

  @Override
  public URI getMappedURI(URI uri) {
    if (!uri.getHost().endsWith(dnsSearch)) {
      try {
        return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost() + dnsSearch,
            uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
      } catch (URISyntaxException ex) {
        throw new IllegalArgumentException(ex);
      }
    } else {
      return uri;
    }
  }
}
