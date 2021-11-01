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
import i18n from '@/i18n';

export default (names: string[], currentValues: Record<string, unknown> = {}): FormItemProps[] => {
  const fields: FormItemProps[] = [
    {
      type: 'text',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.BusinessID'),
      name: 'inlongGroupId',
      initialValue: currentValues.inlongGroupId,
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
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.BusinessEnglishName'),
      name: 'name',
      props: {
        maxLength: 32,
      },
      rules: [
        { required: true },
        {
          pattern: /^[a-z_\d]+$/,
          message: i18n.t(
            'components.AccessHelper.FieldsConfig.businessFields.BusinessEnglishNameRules',
          ),
        },
      ],
      initialValue: currentValues.name,
    },
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.BusinessLabelName'),
      name: 'cnName',
      props: {
        maxLength: 32,
      },
      initialValue: currentValues.cnName,
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.BusinessOwners'),
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      rules: [
        {
          required: true,
        },
      ],
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.BusinessOwnersExtra'),
    },
    {
      type: 'textarea',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.BusinessIntroduction'),
      name: 'description',
      props: {
        showCount: true,
        maxLength: 100,
      },
      initialValue: currentValues.description,
    },
    {
      type: 'radio',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.MessageMiddleware'),
      name: 'middlewareType',
      initialValue: currentValues.middlewareType ?? 'TUBE',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.Tube'),
            value: 'TUBE',
          },
        ],
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.NumberOfAccess'),
      name: 'dailyRecords',
      initialValue: currentValues.dailyRecords,
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.FieldsConfig.businessFields.thousand/day'),
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.AccessSize'),
      name: 'dailyStorage',
      initialValue: currentValues.dailyStorage,
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.FieldsConfig.businessFields.GB/Day'),
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.AccessPeakPerSecond'),
      name: 'peakRecords',
      initialValue: currentValues.peakRecords,
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.FieldsConfig.businessFields.Stripe/Second'),
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.SingleStripMaximumLength'),
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
