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
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.BusinessID'),
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
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.BusinessEnglishName'),
      name: 'name',
      props: {
        maxLength: 32,
      },
      rules: [
        { required: true },
        {
          pattern: /^[a-z_\d]+$/,
          message: i18n.t(
            'components.AccessHelper.FieldsConfig.bussinessFields.BusinessEnglishNameRules',
          ),
        },
      ],
      initialValue: currentValues.name,
    },
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.BusinessLabelName'),
      name: 'cnName',
      props: {
        maxLength: 32,
      },
      initialValue: currentValues.cnName,
      rules: [{ required: true }],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.BusinessOwners'),
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      rules: [
        {
          required: true,
        },
      ],
      extra: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.BusinessOwnersExtra'),
    },
    {
      type: 'textarea',
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.BusinessIntroduction'),
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
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.MessageMiddleware'),
      name: 'middlewareType',
      initialValue: currentValues.middlewareType ?? 'TUBE',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.Tube'),
            value: 'TUBE',
          },
        ],
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.NumberOfAccess'),
      name: 'dailyRecords',
      initialValue: currentValues.dailyRecords,
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.thousand/day'),
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.AccessSize'),
      name: 'dailyStorage',
      initialValue: currentValues.dailyStorage,
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.GB/Day'),
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.AccessPeakPerSecond'),
      name: 'peakRecords',
      initialValue: currentValues.peakRecords,
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.FieldsConfig.bussinessFields.Stripe/Second'),
      props: {
        min: 1,
        precision: 0,
      },
    },
    {
      type: 'inputnumber',
      label: i18n.t(
        'components.AccessHelper.FieldsConfig.bussinessFields.SingleStripMaximumLength',
      ),
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
