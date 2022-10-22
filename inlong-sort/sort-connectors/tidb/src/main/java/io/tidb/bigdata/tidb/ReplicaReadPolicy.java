/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.tidb;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.replica.Region;
import org.tikv.common.replica.ReplicaSelector;
import org.tikv.common.replica.Store;
import org.tikv.common.replica.Store.Label;

public class ReplicaReadPolicy implements ReplicaSelector {

  static final Logger LOG = LoggerFactory.getLogger(ReplicaReadPolicy.class);

  public static final ReplicaReadPolicy DEFAULT = ReplicaReadPolicy.create(ImmutableMap.of());

  private final Map<String, String> labels;
  private final Set<String> whitelist;
  private final Set<String> blacklist;
  private final List<Role> roles;

  private ReplicaReadPolicy(final Map<String, String> labels, final Set<String> whitelist,
      final Set<String> blacklist, final List<Role> roles) {
    this.labels = labels;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
    this.roles = roles;
  }

  @Override
  public List<Store> select(Region region) {
    Store leader = region.getLeader();
    Store[] stores = region.getStores();
    List<Store> followers = Arrays.stream(stores)
        .filter(store -> store.isFollower() && accept(store))
        .collect(Collectors.toList());
    List<Store> learners = Arrays.stream(stores)
        .filter(store -> store.isLearner() && accept(store))
        .collect(Collectors.toList());
    Collections.shuffle(followers);
    Collections.shuffle(learners);
    List<Store> candidates = new ArrayList<>(stores.length);
    for (Role role : roles) {
      switch (role) {
        case LEADER:
          candidates.add(leader);
          continue;
        case FOLLOWER:
          candidates.addAll(followers);
          continue;
        case LEARNER:
          candidates.addAll(learners);
          continue;
        default:
          continue;
      }
    }
    if (candidates.size() == 0) {
      throw new IllegalStateException("Can not get enough candidates");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Current candidates are: {}", candidates);
    }
    return candidates;
  }

  private static Map<String, String> extractLabels(final Map<String, String> properties) {
    String[] labels = properties.getOrDefault(ClientConfig.TIDB_REPLICA_READ_LABEL,
        ClientConfig.TIDB_REPLICA_READ_LABEL_DEFAULT).split(",");
    Map<String, String> map = new HashMap<>(labels.length);
    String key;
    String value;
    for (String pair : labels) {
      if (pair.isEmpty()) {
        continue;
      }
      String[] pairs = pair.trim().split("=");
      if (pairs.length != 2 || (key = pairs[0].trim()).isEmpty()
          || (value = pairs[1].trim()).isEmpty()) {
        throw new IllegalArgumentException("Invalid replica read labels: "
            + Arrays.toString(labels));
      }
      map.put(key, value);
    }
    return map;
  }

  private static Set<String> extractList(final Map<String, String> properties,
      final String key, final String defaultValue) {
    return Arrays.stream(properties.getOrDefault(key, defaultValue).split(","))
        .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
  }

  public static ReplicaReadPolicy create(final Map<String, String> properties) {
    Map<String, String> labels = extractLabels(properties);
    Set<String> whitelist = extractList(properties,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_WHITELIST,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_DEFAULT);
    Set<String> blacklist = extractList(properties,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_BLACKLIST,
        ClientConfig.TIDB_REPLICA_READ_ADDRESS_DEFAULT);
    List<Role> roles = Arrays.stream(properties.getOrDefault(ClientConfig.TIDB_REPLICA_READ,
            ClientConfig.TIDB_REPLICA_READ_DEFAULT).split(","))
        .map(Role::fromString).collect(Collectors.toList());
    return new ReplicaReadPolicy(labels, whitelist, blacklist, roles);
  }

  private boolean inWhitelist(Store store) {
    if (whitelist.isEmpty()) {
      return false;
    }
    return whitelist.stream().anyMatch(a -> a.equals(store.getAddress()));
  }

  private boolean notInBlacklist(Store store) {
    if (blacklist.isEmpty()) {
      return true;
    }
    return blacklist.stream().noneMatch(a -> a.equals(store.getAddress()));
  }

  private boolean matchLabels(Store store) {
    if (labels.isEmpty()) {
      return true;
    }
    int matched = 0;
    for (Label label : store.getLabels()) {
      if (label.getValue().equals(labels.get(label.getKey()))) {
        matched++;
      }
    }
    return matched == labels.size();
  }

  protected boolean accept(Store store) {
    return (matchLabels(store) || inWhitelist(store)) && notInBlacklist(store);
  }

  enum Role {
    LEADER, FOLLOWER, LEARNER;

    public static Role fromString(String role) {
      for (Role value : values()) {
        if (value.name().equalsIgnoreCase(role)) {
          return value;
        }
      }
      throw new IllegalArgumentException("Available roles are: " + Arrays.toString(values()));
    }
  }
}
