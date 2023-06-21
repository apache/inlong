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
import i18n from '@/i18n';
import { SinkInfo } from '../common/SinkInfo';
import NodeSelect from '@/ui/components/NodeSelect';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class ClickHouseSink
  extends SinkInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.ClickHouse.DbName')
  dbName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.ClickHouse.TableName')
  tableName: string;

  @FieldDecorator({
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      nodeType: 'CLICKHOUSE',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  dataNodeName: string;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 1000,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.ClickHouse.FlushRecordUnit'),
  })
  @I18n('meta.Sinks.ClickHouse.FlushRecord')
  flushRecord: number;

  @FieldDecorator({
    type: 'radio',
    initialValue: 0,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.ClickHouse.Yes'),
          value: 1,
        },
        {
          label: i18n.t('meta.Sinks.ClickHouse.No'),
          value: 0,
        },
      ],
    }),
    rules: [{ required: true }],
  })
  @I18n('meta.Sinks.ClickHouse.IsDistributed')
  isDistributed: number;

  @FieldDecorator({
    type: 'select',
    initialValue: 'BALANCE',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'BALANCE',
          value: 'BALANCE',
        },
        {
          label: 'RANDOM',
          value: 'RANDOM',
        },
        {
          label: 'HASH',
          value: 'HASH',
        },
      ],
    }),
    visible: values => values.isDistributed,
  })
  @I18n('meta.Sinks.ClickHouse.PartitionStrategy')
  partitionStrategy: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    visible: values => values.isDistributed && values.partitionStrategy === 'HASH',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.PartitionFields')
  partitionFields: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'Log',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'Log',
          value: 'Log',
        },
        {
          label: 'MergeTree',
          value: 'MergeTree',
        },
        {
          label: 'ReplicatedMergeTree',
          value: 'ReplicatedMergeTree',
        },
      ],
    }),
  })
  @I18n('meta.Sinks.ClickHouse.Engine')
  engine: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.OrderBy')
  orderBy: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.PartitionBy')
  partitionBy: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.PrimaryKey')
  primaryKey: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.Cluster')
  cluster: string;

  @FieldDecorator({
    type: 'inputnumber',
    visible: values => values.engine === 'MergeTree' || values.engine === 'ReplicatedMergeTree',
    suffix: {
      type: 'select',
      name: 'ttlUnit',
      props: values => ({
        disabled: [110, 130].includes(values?.status),
        options: [
          {
            label: 'Second',
            value: 'second',
          },
          {
            label: 'Minute',
            value: 'minute',
          },
          {
            label: 'Hour',
            value: 'hour',
          },
          {
            label: 'Day',
            value: 'day',
          },
          {
            label: 'Week',
            value: 'week',
          },
          {
            label: 'Month',
            value: 'month',
          },
          {
            label: 'Quarter',
            value: 'quarter',
          },
          {
            label: 'Year',
            value: 'year',
          },
        ],
      }),
    },
    props: values => ({
      min: 1,
      precision: 0,
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('Time To Live')
  ttl: number;
}
