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
import BusinessSelect from '../BusinessSelect';

export default (
  names: (string | FormItemProps)[],
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
        },
      ],
    },
    {
      type: BusinessSelect,
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.ConsumerTargetBusinessID'),
      name: 'inlongGroupId',
      extraNames: ['middlewareType'],
      initialValue: currentValues.inlongGroupId,
      rules: [{ required: true }],
      props: {
        style: { width: 500 },
        onChange: (inlongGroupId, record) => ({
          topic: undefined,
          middlewareType: record.middlewareType,
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
        mode: currentValues.middlewareType === 'PULSAR' ? 'multiple' : '',
        options: {
          requestService: `/business/getTopic/${currentValues.inlongGroupId}`,
          requestParams: {
            formatResult: result =>
              result.middlewareType === 'TUBE'
                ? [
                    {
                      label: result.mqResourceObj,
                      value: result.mqResourceObj,
                    },
                  ]
                : result.dsTopicList?.map(item => ({
                    ...item,
                    label: item.mqResourceObj,
                    value: item.mqResourceObj,
                  })) || [],
          },
        },
      },
      visible: values => !!values.inlongGroupId,
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
      visible: values => !!values.middlewareType && values.middlewareType !== 'PULSAR',
    },
    {
      type: 'input',
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.ConsumerDataStreamID'),
      name: 'inlongStreamId',
      initialValue: currentValues.inlongStreamId,
      extra: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.DataStreamIDsHelp'),
      rules: [{ required: true }],
      style:
        currentValues.middlewareType === 'PULSAR'
          ? {
              display: 'none',
            }
          : {},
      visible: values => values.middlewareType === 'PULSAR' || values.filterEnabled,
    },
    {
      type: 'text',
      label: i18n.t('components.ConsumeHelper.FieldsConfig.basicFields.MasterAddress'),
      name: 'masterUrl',
      initialValue: currentValues.masterUrl,
    },
    {
      type: 'radio',
      label: 'isDlq',
      name: 'mqExtInfo.isDlq',
      initialValue: currentValues.mqExtInfo?.isDlq ?? 0,
      rules: [{ required: true }],
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
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'input',
      label: 'deadLetterTopic',
      name: 'mqExtInfo.deadLetterTopic',
      initialValue: currentValues.mqExtInfo?.deadLetterTopic,
      rules: [{ required: true }],
      visible: values => values.mqExtInfo?.isDlq && values.middlewareType === 'PULSAR',
    },
    {
      type: 'radio',
      label: 'isRlq',
      name: 'mqExtInfo.isRlq',
      initialValue: currentValues.mqExtInfo?.isRlq ?? 0,
      rules: [{ required: true }],
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
      visible: values => values.mqExtInfo?.isDlq && values.middlewareType === 'PULSAR',
    },
    {
      type: 'input',
      label: 'retryLetterTopic',
      name: 'mqExtInfo.retryLetterTopic',
      initialValue: currentValues.mqExtInfo?.retryLetterTopic,
      rules: [{ required: true }],
      visible: values =>
        values.mqExtInfo?.isDlq && values.mqExtInfo?.isRlq && values.middlewareType === 'PULSAR',
    },
  ] as FormItemProps[];

  return pickObjectArray(names, fields);
};
