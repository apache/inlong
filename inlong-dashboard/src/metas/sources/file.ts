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

import i18n from '@/i18n';
import rulesPattern from '@/utils/pattern';
import type { FieldItemType } from '@/metas/common';

export const file: FieldItemType[] = [
  {
    type: 'input',
    label: i18n.t('meta.Sources.File.DataSourceIP'),
    name: 'agentIp',
    rules: [
      {
        pattern: rulesPattern.ip,
        message: i18n.t('meta.Sources.File.IpRule'),
        required: true,
      },
    ],
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sources.File.FilePath'),
    name: 'pattern',
    tooltip: i18n.t('meta.Sources.File.FilePathHelp'),
    rules: [{ required: true }],
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sources.File.TimeOffset'),
    name: 'timeOffset',
    tooltip: i18n.t('meta.Sources.File.TimeOffsetHelp'),
  },
];
