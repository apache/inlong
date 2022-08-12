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

export default (
  names: (string | FormItemProps)[],
  currentValues: Record<string, any> = {},
): FormItemProps[] => {
  const fields: FormItemProps[] = [
    {
      type: 'input',
      label: i18n.t('meta.Consumption.ConsumerGroupName'),
      name: 'consumerGroup',
      initialValue: currentValues.consumerGroup,
      extra: i18n.t('meta.Consumption.ConsumerGroupNameRules'),
      rules: [
        { required: true },
        {
          pattern: /^[0-9a-z_\d]+$/,
          message: i18n.t('meta.Consumption.ConsumerGroupNameRules'),
        },
      ],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: i18n.t('meta.Consumption.Owner'),
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      extra: i18n.t('meta.Consumption.OwnersExtra'),
      rules: [
        {
          required: true,
        },
      ],
    },
    {
      type: 'select',
      label: i18n.t('meta.Consumption.ConsumerTargetBusinessID'),
      name: 'inlongGroupId',
      extraNames: ['mqType'],
      initialValue: currentValues.inlongGroupId,
      rules: [{ required: true }],
      props: {
        showSearch: true,
        filterOption: false,
        options: {
          requestTrigger: ['onOpen', 'onSearch'],
          requestService: keyword => ({
            url: '/group/list',
            method: 'POST',
            data: {
              keyword,
              pageNum: 1,
              pageSize: 20,
              status: 130,
            },
          }),
          requestParams: {
            formatResult: result =>
              result?.list?.map(item => ({
                ...item,
                label: `${item.inlongGroupId} (${item.mqType})`,
                value: item.inlongGroupId,
              })),
          },
        },
        onChange: (value, option) => ({
          topic: undefined,
          mqType: option.mqType,
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
        mode: currentValues.mqType === 'PULSAR' ? 'multiple' : '',
        options: {
          requestService: `/group/getTopic/${currentValues.inlongGroupId}`,
          requestParams: {
            formatResult: result =>
              result.mqType === 'TUBEMQ'
                ? [
                    {
                      label: result.mqResource,
                      value: result.mqResource,
                    },
                  ]
                : result.streamTopics?.map(item => ({
                    ...item,
                    label: item.mqResource,
                    value: item.mqResource,
                  })) || [],
          },
        },
      },
      visible: values => !!values.inlongGroupId,
    },
    {
      type: 'radio',
      label: i18n.t('meta.Consumption.filterEnabled'),
      name: 'filterEnabled',
      initialValue: currentValues.filterEnabled ?? 0,
      props: {
        options: [
          {
            label: i18n.t('meta.Consumption.Yes'),
            value: 1,
          },
          {
            label: i18n.t('meta.Consumption.No'),
            value: 0,
          },
        ],
      },
      rules: [{ required: true }],
      visible: values => !!values.mqType && values.mqType !== 'PULSAR',
    },
    {
      type: 'input',
      label: i18n.t('meta.Consumption.ConsumerDataStreamID'),
      name: 'inlongStreamId',
      initialValue: currentValues.inlongStreamId,
      extra: i18n.t('meta.Consumption.DataStreamIDsHelp'),
      rules: [{ required: true }],
      style:
        currentValues.mqType === 'PULSAR'
          ? {
              display: 'none',
            }
          : {},
      visible: values => values.mqType === 'PULSAR' || values.filterEnabled,
    },
    {
      type: 'text',
      label: i18n.t('meta.Consumption.MasterAddress'),
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
            label: i18n.t('meta.Consumption.Yes'),
            value: 1,
          },
          {
            label: i18n.t('meta.Consumption.No'),
            value: 0,
          },
        ],
      },
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'input',
      label: 'deadLetterTopic',
      name: 'mqExtInfo.deadLetterTopic',
      initialValue: currentValues.mqExtInfo?.deadLetterTopic,
      rules: [{ required: true }],
      visible: values => values.mqExtInfo?.isDlq && values.mqType === 'PULSAR',
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
            label: i18n.t('meta.Consumption.Yes'),
            value: 1,
          },
          {
            label: i18n.t('meta.Consumption.No'),
            value: 0,
          },
        ],
      },
      visible: values => values.mqExtInfo?.isDlq && values.mqType === 'PULSAR',
    },
    {
      type: 'input',
      label: 'retryLetterTopic',
      name: 'mqExtInfo.retryLetterTopic',
      initialValue: currentValues.mqExtInfo?.retryLetterTopic,
      rules: [{ required: true }],
      visible: values =>
        values.mqExtInfo?.isDlq && values.mqExtInfo?.isRlq && values.mqType === 'PULSAR',
    },
  ] as FormItemProps[];

  return pickObjectArray(names, fields);
};
