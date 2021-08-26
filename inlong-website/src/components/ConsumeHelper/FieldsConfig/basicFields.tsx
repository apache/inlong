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
import { FormItemProps } from '@/components/FormGenerator';
import { pickObjectArray } from '@/utils';
import StaffSelect from '@/components/StaffSelect';
import i18n from '@/i18n';
import BussinessSelect from '../BussinessSelect';

export default (
  names: string[],
  bussinessDetail: Record<'middlewareType', string> = { middlewareType: '' },
  currentValues: Record<string, any> = {},
): FormItemProps[] => {
  const fields: FormItemProps[] = [
    {
      type: 'input',
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.ConsumerGroupName'),
      name: 'consumerGroupName',
      initialValue: currentValues.consumerGroupName,
      extra: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.ConsumerGroupNameRules'),
      rules: [
        { required: true },
        {
          pattern: /^[a-z_\d]+$/,
          message: i18n.t(
            'components.ConsumeHelper.FieldsConfig.basicFields.ConsumerGroupNameRules',
          ),
        },
      ],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.Consumption'),
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      extra: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.OwnersExtra'),
      rules: [
        {
          required: true,
          type: 'array',
          min: 2,
          message: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.OwnersRule'),
        },
      ],
    },
    {
      type: BussinessSelect,
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.ConsumerTargetBusinessID'),
      name: 'businessIdentifier',
      initialValue: currentValues.businessIdentifier,
      rules: [{ required: true }],
      props: {
        style: { width: 500 },
        onChange: () => ({
          topic: '',
        }),
      },
    },
    {
      type: 'select',
      label: 'Topic',
      name: 'topic',
      initialValue: currentValues.topic,
      rules: [{ required: true }],
      props: {
        options: {
          requestService: `/business/getTopic/${currentValues.businessIdentifier}`,
          requestParams: {
            formatResult: result => [
              {
                label: result.topicName,
                value: result.topicName,
              },
            ],
          },
        },
      },
      visible: values => !!values.businessIdentifier,
    },
    {
      type: 'radio',
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.filterEnabled'),
      name: 'filterEnabled',
      initialValue: currentValues.filterEnabled ?? 0,
      props: {
        options: [
          {
            label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.Yes'),
            value: 1,
          },
          {
            label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.No'),
            value: 0,
          },
        ],
      },
      rules: [{ required: true }],
      visible: !!bussinessDetail.middlewareType,
    },
    {
      type: 'input',
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.ConsumerDataStreamID'),
      name: 'dataStreamIdentifier',
      initialValue: currentValues.dataStreamIdentifier,
      extra: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.DataStreamIDsHelp'),
      rules: [{ required: true }],
      visible: values => bussinessDetail.middlewareType && values.filterEnabled,
    },
    {
      type: 'text',
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.MasterAddress'),
      name: 'masterUrl',
      initialValue: currentValues.masterUrl,
    },
  ] as FormItemProps[];

  return pickObjectArray(names, fields);
};
