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

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class PulsarSource
  extends SourceInfo
  implements DataWithBackend, RenderRow, RenderList
{
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
  @I18n('meta.Sources.Pulsar.PulsarTenant')
  pulsarTenant: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Pulsar.Namespace')
  namespace: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('Admin url')
  adminUrl: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('Service url')
  serviceUrl: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('Pulsar topic')
  topic: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: 'CSV',
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: 'CSV',
          value: 'CSV',
        },
        {
          label: 'Key-Value',
          value: 'KV',
        },
        {
          label: 'Avro',
          value: 'AVRO',
        },
        {
          label: 'JSON',
          value: 'JSON',
        },
      ],
    }),
    rules: [{ required: true }],
  })
  @SyncField()
  @I18n('meta.Sources.Pulsar.SerializationType')
  serializationType: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Pulsar.PrimaryKey')
  primaryKey: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: 'UTF-8',
    props: values => ({
      disabled: values?.status === 101,
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
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Pulsar.DataEncoding')
  dataEncoding: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Pulsar.DataSeparator')
  dataSeparator: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Pulsar.DataEscapeChar')
  dataEscapeChar: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: 'INLONG_MSG_V0',
    props: values => ({
      disabled: values?.status === 101,
      options: [
        {
          label: 'InLongMsg V0',
          value: 'INLONG_MSG_V0',
        },
        {
          label: 'InLongMsg V1',
          value: 'INLONG_MSG_V1',
        },
        {
          label: 'Raw',
          value: 'RAW',
        },
      ],
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sources.Pulsar.WrapType')
  wrapType: string;
}
