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

export const kafka: FieldItemType[] = [
  {
    name: 'bootstrapServers',
    type: 'input',
    label: i18n.t('meta.Sinks.Kafka.Server'),
    rules: [{ required: true }],
    initialValue: '127.0.0.1:9092',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'topicName',
    type: 'input',
    label: 'Topic',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'serializationType',
    type: 'radio',
    label: i18n.t('meta.Sinks.Kafka.SerializationType'),
    initialValue: 'JSON',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'JSON',
          value: 'JSON',
        },
        {
          label: 'CANAL',
          value: 'CANAL',
        },
        {
          label: 'AVRO',
          value: 'AVRO',
        },
      ],
    }),
    _renderTable: true,
  },
  {
    name: 'partitionNum',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.Kafka.PartitionNum'),
    initialValue: 3,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
      max: 30,
    }),
    rules: [{ required: true }],
  },
  {
    name: 'autoOffsetReset',
    type: 'radio',
    label: i18n.t('meta.Sinks.Kafka.AutoOffsetReset'),
    initialValue: 'earliest',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'earliest',
          value: 'earliest',
        },
        {
          label: 'latest',
          value: 'latest',
        },
        {
          label: 'none',
          value: 'none',
        },
      ],
    }),
  },
];
