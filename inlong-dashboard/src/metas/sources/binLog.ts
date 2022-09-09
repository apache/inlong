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
import type { FieldItemType } from '@/metas/common';

export const binLog: FieldItemType[] = [
  {
    name: 'hostname',
    type: 'input',
    label: i18n.t('meta.Sources.Db.Server'),
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
    _renderTable: true,
  },
  {
    name: 'port',
    type: 'inputnumber',
    label: i18n.t('meta.Sources.Db.Port'),
    initialValue: 3306,
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
      min: 0,
      max: 65535,
    }),
    _renderTable: true,
  },
  {
    name: 'user',
    type: 'input',
    label: i18n.t('meta.Sources.Db.User'),
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  },
  {
    name: 'password',
    type: 'password',
    label: i18n.t('meta.Sources.Db.Password'),
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  },
  {
    name: 'historyFilename',
    type: 'input',
    label: i18n.t('meta.Sources.Db.HistoryFilename'),
    rules: [{ required: true }],
    initialValue: '/data/inlong-agent/.history',
    props: values => ({
      disabled: values?.status === 101,
    }),
    _renderTable: true,
  },
  {
    name: 'serverTimezone',
    type: 'input',
    label: i18n.t('meta.Sources.Db.ServerTimezone'),
    tooltip: 'UTC, UTC+8, Asia/Shanghai, ...',
    initialValue: 'UTC',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  },
  {
    name: 'intervalMs',
    type: 'inputnumber',
    label: i18n.t('meta.Sources.Db.IntervalMs'),
    initialValue: 1000,
    rules: [{ required: true }],
    suffix: 'ms',
    props: values => ({
      disabled: values?.status === 101,
      min: 1000,
      max: 3600000,
    }),
  },
  {
    name: 'allMigration',
    type: 'radio',
    label: i18n.t('meta.Sources.Db.AllMigration'),
    rules: [{ required: true }],
    initialValue: false,
    props: values => ({
      disabled: values?.status === 101,
      options: [
        {
          label: i18n.t('basic.Yes'),
          value: true,
        },
        {
          label: i18n.t('basic.No'),
          value: false,
        },
      ],
    }),
  },
  {
    name: 'tableWhiteList',
    type: 'input',
    label: i18n.t('meta.Sources.Db.TableWhiteList'),
    tooltip: i18n.t('meta.Sources.Db.TableWhiteListHelp'),
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
    visible: values => !values?.allMigration,
  },
];
