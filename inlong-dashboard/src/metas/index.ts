/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useState, useEffect, useCallback } from 'react';
import type { MetaExportWithBackend, MetaExportWithBackendList } from '@/metas/types';
import { consumes, defaultValue as defaultConsume } from './consumes';
import { groups, defaultValue as defaultGroup } from './groups';
import { clusters, defaultValue as defaultCluster } from './clusters';
import { nodes, defaultValue as defaultNode } from './nodes';
import { streams, defaultValue as defaultStream } from './streams';
import { sources, defaultValue as defaultSource } from './sources';
import { sinks, defaultValue as defaultSink } from './sinks';

export interface UseLoadMetaResult {
  loading: boolean;
  Entity: MetaExportWithBackend;
}

export type MetaTypeKeys = 'consume' | 'group' | 'cluster' | 'node' | 'stream' | 'source' | 'sink';

const metasMap: Record<MetaTypeKeys, [MetaExportWithBackendList, string?]> = {
  consume: [consumes, defaultConsume],
  group: [groups, defaultGroup],
  cluster: [clusters, defaultCluster],
  node: [nodes, defaultNode],
  stream: [streams, defaultStream],
  source: [sources, defaultSource],
  sink: [sinks, defaultSink],
};

export const useDefaultMeta = (metaType: MetaTypeKeys) => {
  const [options = [], defaultValue] = metasMap[metaType];
  return {
    defaultValue: defaultValue || options[0].value,
    options: options.map(item => ({ label: item.label, value: item.value })),
  };
};

export const useLoadMeta = (metaType: MetaTypeKeys, subType: string): UseLoadMetaResult => {
  const [loading, setLoading] = useState<boolean>(false);
  const [Entity, setEntity] = useState<{ default: MetaExportWithBackend }>();

  const load = useCallback(
    async subType => {
      const subList = metasMap[metaType]?.[0];
      const LoadEntity = subList?.find(item => item.value === subType)?.LoadEntity;
      if (LoadEntity) {
        setLoading(true);
        try {
          const result = await LoadEntity();
          setEntity(result);
        } finally {
          setLoading(false);
        }
      }
    },
    [metaType],
  );

  useEffect(() => {
    load(subType);
  }, [subType, load]);

  return {
    loading,
    Entity: Entity?.default,
  };
};
