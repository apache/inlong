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
import rulesPattern from '@/core/utils/pattern';
import i18n from '@/i18n';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class MongodbSource
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
      disabled: Boolean(values.id),
      placeholder: 'localhost:27017,localhost:27018',
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Mongodb.Hosts')
  hosts: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Mongodb.Username')
  username: string;

  @FieldDecorator({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Mongodb.Password')
  password: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Mongodb.Database')
  database: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Mongodb.Collection')
  collection: string;

  @FieldDecorator({
    type: 'radio',
    props: values => ({
      disabled: Boolean(values.id),
      initialValue: 'initial',
      options: [
        {
          label: i18n.t('meta.Sources.Mongodb.SnapshotMode.Initial'),
          value: 'initial',
        },
        {
          label: i18n.t('meta.Sources.Mongodb.SnapshotMode.Never'),
          value: 'never',
        },
      ],
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.Mongodb.SnapshotMode')
  snapshotMode: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Mongodb.PrimaryKey')
  primaryKey: string;
}
