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

import { getColsFromFields } from '@/utils/metaData';
import { ColumnsType } from 'antd/es/table';
import rulesPattern from '@/utils/pattern';
import i18n from '@/i18n';

export const getDataSourcesFileFields = (type: 'form' | 'col' = 'form') => {
  const fileds = [
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.File.DataSourceIP'),
      name: 'ip',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: i18n.t('components.AccessHelper.DataSourceMetaData.File.IpRule'),
        },
      ],
      _inTable: {
        width: 120,
      },
    },
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.File.FilePath'),
      name: 'filePath',
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.DataSourceMetaData.File.FillInTheAbsolutePath'),
      _inTable: true,
    },
  ];

  return type === 'col' ? getColsFromFields(fileds) : fileds;
};

export const dataSourcesFileColumns = getDataSourcesFileFields('col') as ColumnsType;
