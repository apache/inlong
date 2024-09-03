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
import { Button } from 'antd';
import React from 'react';
import { timestampFormat } from '@/core/utils';

export const useColumns = ({ onDelete, openModal }) => {
  const defaultColumns = [
    {
      title: i18n.t('pages.PackageAgent.Config.FileName'),
      dataIndex: 'fileName',
    },
    {
      title: i18n.t('pages.PackageAgent.Config.StoragePath'),
      dataIndex: 'storagePath',
    },
    {
      title: i18n.t('pages.PackageAgent.Config.DownloadUrl'),
      dataIndex: 'downloadUrl',
    },
    {
      title: i18n.t('basic.Creator'),
      dataIndex: 'creator',
      render: (text, record) => (
        <>
          <div>{text}</div>
          <div>{record.createTime && timestampFormat(record.createTime)}</div>
        </>
      ),
    },
    {
      title: i18n.t('basic.Modifier'),
      dataIndex: 'modifier',
      render: (text, record) => (
        <>
          <div>{text}</div>
          <div>{record.modifyTime && timestampFormat(record.modifyTime)}</div>
        </>
      ),
    },
  ];
  return defaultColumns.concat([
    {
      title: i18n.t('basic.Operating'),
      dataIndex: 'action',
      render: (text, record) => (
        <>
          <Button type="link" onClick={() => openModal(record)}>
            {i18n.t('basic.Detail')}
          </Button>
          <Button type="link" onClick={() => onDelete(record)}>
            {i18n.t('basic.Delete')}
          </Button>
        </>
      ),
    } as any,
  ]);
};

export const getFormContent = initialValues => [
  {
    type: 'inputsearch',
    name: 'keyword',
    initialValue: initialValues.keyword,
    props: {
      allowClear: true,
    },
  },
];
