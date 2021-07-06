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

import React from 'react';
import StaffSelect from '../../StaffSelect';
import { FormItemProps } from '@/components/FormGenerator';
import { pickObjectArray } from '@/utils';

export default (names: string[], currentValues: Record<string, unknown> = {}): FormItemProps[] => {
  const fields: FormItemProps[] = [
    {
      type: 'text',
      label: '业务ID',
      name: 'businessIdentifier',
      initialValue: currentValues.businessIdentifier,
    },
    {
      type: 'text',
      label: 'TubeTopic',
      name: 'mqResourceObj',
      initialValue: currentValues.mqResourceObj,
      visible: values => values.middlewareType === 'TUBE',
    },
    {
      type: 'input',
      label: '业务英文名称',
      name: 'name',
      props: {
        maxLength: 32,
      },
      rules: [
        { required: true },
        { pattern: /^[a-z_\d]+$/, message: '仅限小写字⺟、数字和下划线' },
      ],
      initialValue: currentValues.name,
    },
    {
      type: 'input',
      label: '业务中文名称',
      name: 'cnName',
      props: {
        maxLength: 32,
      },
      initialValue: currentValues.cnName,
      rules: [{ required: true }],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: '业务责任人',
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      rules: [{ required: true, type: 'array', min: 2, message: '请填写至少2个责任人' }],
      extra: '至少2人，业务责任人可查看、修改业务信息，新增和修改所有接入配置项',
    },
    {
      type: 'textarea',
      label: '业务介绍',
      name: 'description',
      props: {
        showCount: true,
        maxLength: 100,
      },
      initialValue: currentValues.description,
      rules: [{ required: true }],
    },
    {
      type: 'radio',
      label: '消息中间件选型',
      name: 'middlewareType',
      initialValue: currentValues.middlewareType ?? 'TUBE',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: '⾼吞吐（TUBE）',
            value: 'TUBE',
          },
        ],
      },
    },
    {
      type: 'inputnumber',
      label: '按天接入条数',
      name: 'dailyRecords',
      initialValue: currentValues.dailyRecords,
      rules: [{ required: true }],
      suffix: '万条/天',
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: '按天接入大小',
      name: 'dailyStorage',
      initialValue: currentValues.dailyStorage,
      rules: [{ required: true }],
      suffix: 'GB/天',
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: '每秒接入峰值',
      name: 'peakRecords',
      initialValue: currentValues.peakRecords,
      rules: [{ required: true }],
      suffix: '条/秒',
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: '单条最大长度',
      name: 'maxLength',
      initialValue: currentValues.maxLength,
      rules: [{ required: true }],
      suffix: 'Byte',
      props: {
        min: 1,
        precision: 0,
      },
    },
  ] as FormItemProps[];

  return pickObjectArray(names, fields);
};
