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

import type { MetaExportWithBackendList } from '@/plugins/types';
import type { SourceMetaType } from '../types';

export const allDefaultSources: MetaExportWithBackendList<SourceMetaType> = [
  {
    label: 'ALL',
    value: '',
    useSync: false,
    LoadEntity: () => import('../common/SourceInfo').then(r => ({ default: r.SourceInfo })),
  },
  {
    label: 'Auto-Push',
    value: 'AUTO_PUSH',
    useSync: false,
    LoadEntity: () => import('./AutoPush'),
  },
  {
    label: 'File',
    value: 'FILE',
    useSync: false,
    LoadEntity: () => import('./File'),
  },
  {
    label: 'Kafka',
    value: 'KAFKA',
    useSync: false,
    LoadEntity: () => import('./Kafka'),
  },
  {
    label: 'MySQL',
    value: 'MYSQL_BINLOG',
    LoadEntity: () => import('./MySQLBinlog'),
  },
  {
    label: 'OceanBase',
    value: 'OCEANBASE',
    LoadEntity: () => import('./OceanBaseBinlog'),
  },
  {
    label: 'MongoDB',
    value: 'MONGODB',
    LoadEntity: () => import('./Mongodb'),
  },
  {
    label: 'MQTT',
    value: 'MQTT',
    LoadEntity: () => import('./MQTT'),
  },
  {
    label: 'Oracle',
    value: 'ORACLE',
    LoadEntity: () => import('./Oracle'),
  },
  {
    label: 'PostgreSQL',
    value: 'POSTGRESQL',
    LoadEntity: () => import('./PostgreSQL'),
  },
  {
    label: 'Pulsar',
    value: 'PULSAR',
    LoadEntity: () => import('./Pulsar'),
  },
  {
    label: 'Redis',
    value: 'REDIS',
    LoadEntity: () => import('./Redis'),
  },
  {
    label: 'SQLServer',
    value: 'SQLSERVER',
    LoadEntity: () => import('./SQLServer'),
  },
  {
    label: 'Iceberg',
    value: 'ICEBERG',
    LoadEntity: () => import('./Iceberg'),
  },
  {
    label: 'COS',
    value: 'COS',
    LoadEntity: () => import('./COS'),
  },
  {
    label: 'SQL',
    value: 'SQL',
    LoadEntity: () => import('./SQL'),
  },
];
