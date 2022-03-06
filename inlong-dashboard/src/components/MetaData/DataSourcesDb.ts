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

import request from '@/utils/request';
import { getColsFromFields } from '@/utils/metaData';
import { ColumnsType } from 'antd/es/table';
import rulesPattern from '@/utils/pattern';
import i18n from '@/i18n';

export const getDataSourcesDbFields = (
  type: 'form' | 'col' = 'form',
  { currentValues } = {} as any,
) => {
  const fileds = [
    {
      name: 'accessType',
      type: 'radio',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.AccessType'),
      initialValue: 'DB_SYNC_AGENT',
      rules: [{ required: true }],
      props: {
        options: [
          // {
          //   label: 'SQL',
          //   value: 'SQL',
          // },
          {
            label: 'BinLog',
            value: 'DB_SYNC_AGENT',
          },
        ],
      },
    },
    {
      name: 'serverName',
      type: 'select',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.Server'),
      rules: [{ required: true }],
      extraNames: ['serverId'],
      props: {
        // asyncValueLabel: '',
        options: {
          requestService: async () => {
            const groupData = await request({
              url: '/commonserver/getByUser',
              params: {
                serverType: 'DB',
              },
            });
            return groupData;
          },
          requestParams: {
            formatResult: result =>
              result?.map(item => ({
                ...item,
                label: item.serverName,
                value: item.serverName,
                serverId: item.id,
              })),
          },
        },
        onChange: (value, option) => ({
          serverId: option.serverId,
          dbName: option.dbName,
          clusterName: undefined,
        }),
        disabled: currentValues?.status === 101,
      },
      _inTable: true,
    },
    {
      name: 'dbName',
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.Name'),
      rules: [{ required: true }],
      _inTable: true,
    },
    {
      name: 'dbAgentIp',
      type: 'input',
      label: 'DB Agent IP',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: i18n.t('components.AccessHelper.DataSourceMetaData.Db.IpRule'),
        },
      ],
    },
    {
      name: 'tableName',
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.TableName'),
      rules: [{ required: true }],
      props: {
        placeholder: i18n.t('components.AccessHelper.DataSourceMetaData.Db.TableNamePlaceholder'),
      },
      _inTable: true,
    },
    {
      name: 'charset',
      type: 'select',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.Charset'),
      rules: [{ required: true }],
      initialValue: 'UTF-8',
      props: {
        options: [
          {
            label: 'UTF-8',
            value: 'UTF-8',
          },
          {
            label: 'GBK',
            value: 'GBK',
          },
        ],
      },
      _inTable: true,
    },
    {
      name: 'skipDelete',
      type: 'radio',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.SkipDelete'),
      rules: [{ required: true }],
      initialValue: 1,
      props: {
        options: [
          {
            label: i18n.t('basic.Yes'),
            value: 1,
          },
          {
            label: i18n.t('basic.No'),
            value: 0,
          },
        ],
      },
    },
    {
      name: '_startDumpPosition',
      type: 'radio',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.StartDumpPosition'),
      initialValue: currentValues?._startDumpPosition || 0,
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('basic.Yes'),
            value: 1,
          },
          {
            label: i18n.t('basic.No'),
            value: 0,
          },
        ],
      },
    },
    {
      name: 'startDumpPosition.logIdentity.sourceIp',
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.startDumpPositionIp'),
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: i18n.t('components.AccessHelper.DataSourceMetaData.Db.IpRule'),
        },
      ],
      visible: values => values?._startDumpPosition,
    },
    {
      name: 'startDumpPosition.logIdentity.sourcePort',
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.startDumpPositionPort'),
      props: {
        min: 1,
        max: 65535,
      },
      rules: [{ required: true }],
      visible: values => values?._startDumpPosition,
    },
    {
      name: 'startDumpPosition.entryPosition.journalName',
      type: 'input',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.startDumpPositionFilename'),
      rules: [{ required: true }],
      visible: values => values?._startDumpPosition,
    },
    {
      name: 'startDumpPosition.entryPosition.position',
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.DataSourceMetaData.Db.startDumpPositionPosition'),
      rules: [{ required: true }],
      props: {
        min: 1,
        max: 1000000000,
        precision: 0,
      },
      visible: values => values?._startDumpPosition,
    },
  ];

  return type === 'col' ? getColsFromFields(fileds) : fileds;
};

export const toFormValues = data => {
  return {
    ...data,
    _startDumpPosition: data.startDumpPosition ? 1 : 0,
  };
};

export const toSubmitValues = data => {
  const output = { ...data };
  delete output._startDumpPosition;
  return {
    ...output,
    startDumpPosition: data._startDumpPosition ? output.startDumpPosition : null,
  };
};

export const dataSourcesDbColumns = getDataSourcesDbFields('col') as ColumnsType;
