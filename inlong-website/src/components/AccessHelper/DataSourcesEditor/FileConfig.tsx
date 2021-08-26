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

import { ColumnsType } from 'antd/es/table';
import rulesPattern from '@/utils/pattern';
import i18n from '@/i18n';

export const getCreateFormContent = () => {
  const array = [
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.DataSourceIP'),
      name: 'ip',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.IpRule'),
        },
      ],
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.Port'),
      name: 'port',
      props: {
        min: 1,
        max: 65535,
      },
      rules: [{ required: true }],
    },
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.FilePath'),
      name: 'filePath',
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.FillInTheAbsolutePath'),
    },
  ];

  return array;
};

export const tableColumns: ColumnsType = [
  {
    title: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.DataSourceIP'),
    dataIndex: 'ip',
    width: 150,
  },
  {
    title: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.Port'),
    dataIndex: 'port',
    width: 120,
  },
  {
    title: i18n.t('components.AccessHelper.DataSourcesEditor.FileConfig.FilePath'),
    dataIndex: 'filePath',
  },
];
