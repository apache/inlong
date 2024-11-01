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

import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';
import { SourceInfo } from '../common/SourceInfo';
import i18n from '@/i18n';
import rulesPattern from '@/core/utils/pattern';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class PostgreSQLSource
  extends SourceInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
      showSearch: true,
      allowClear: true,
      filterOption: false,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/cluster/list',
          method: 'POST',
          data: {
            keyword,
            type: 'AGENT',
            clusterTag: values.clusterTag,
            pageNum: 1,
            pageSize: 10,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              ...item,
              label: item.displayName,
              value: item.name,
            })),
        },
      },
      onChange: (value, option) => {
        return {
          clusterId: option.id,
        };
      },
    }),
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.ClusterName')
  inlongClusterName: string;

  @FieldDecorator({
    type: 'text',
    hidden: true,
  })
  @I18n('clusterId')
  @IngestionField()
  clusterId: number;

  @FieldDecorator({
    type: 'select',
    rules: [
      {
        pattern: rulesPattern.ip,
        message: i18n.t('meta.Sources.File.IpRule'),
        required: true,
      },
    ],
    props: values => ({
      disabled: Boolean(values.id),
      showSearch: true,
      allowClear: true,
      filterOption: false,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/cluster/node/list',
          method: 'POST',
          data: {
            keyword,
            parentId: values.clusterId,
            pageNum: 1,
            pageSize: 10,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              ...item,
              label: item.ip,
              value: item.ip,
            })),
        },
      },
    }),
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.DataSourceIP')
  agentIp: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.Hostname')
  hostname: string;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    initialValue: 5432,
    props: values => ({
      min: 1,
      max: 65535,
      disabled: values?.status === 101,
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.Port')
  port: number;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.Database')
  database: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.SchemaName')
  schema: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @ColumnDecorator()
  @I18n('meta.Sources.PostgreSQL.Username')
  username: string;

  @FieldDecorator({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.Password')
  password: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    tooltip: i18n.t('meta.Sources.PostgreSQL.TableNameHelp'),
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.TableName')
  tableNameList: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.PrimaryKey')
  primaryKey: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'decoderbufs',
    props: values => ({
      disabled: values?.status === 101,
      options: [
        {
          label: 'decoderbufs',
          value: 'decoderbufs',
        },
        {
          label: 'wal2json',
          value: 'wal2json',
        },
        {
          label: 'wal2json_rds',
          value: 'wal2json_rds',
        },
        {
          label: 'wal2json_streaming',
          value: 'wal2json_streaming',
        },
        {
          label: 'wal2json_rds_streaming',
          value: 'wal2json_rds_streaming',
        },
        {
          label: 'pgoutput',
          value: 'pgoutput',
        },
      ],
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.PostgreSQL.decodingPluginName')
  decodingPluginName: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'GMT+8:00',
    props: values => ({
      disabled: Boolean(values.id),
      options: [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11,
        -12,
      ].map(item => ({
        label: Math.sign(item) === 1 || Math.sign(item) === 0 ? `GMT+${item}:00` : `GMT${item}:00`,
        value: Math.sign(item) === 1 || Math.sign(item) === 0 ? `GMT+${item}:00` : `GMT${item}:00`,
      })),
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.File.TimeZone')
  dataTimeZone: string;

  parse(data) {
    let obj = { ...data };
    obj.tableNameList = obj.tableNameList.join(',');
    return obj;
  }

  stringify(data) {
    let obj = { ...data };
    if (typeof obj.tableNameList === 'string') {
      obj.tableNameList = obj.tableNameList.split(',');
    }
    return obj;
  }
}
