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

export default (
  names: (string | FormItemProps)[],
  currentValues: Record<string, any> = {},
): FormItemProps[] => {
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
            label: 'TUBE',
            value: 'TUBE',
          },
          {
            label: 'PULSAR',
            value: 'PULSAR',
          },
        ],
      },
    },
    {
      type: 'radio',
      label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.QueueModule'),
      name: 'queueModule',
      initialValue: currentValues.queueModule ?? 'serial',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.Parallel'),
            value: 'parallel',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.businessFields.Serial'),
            value: 'serial',
          },
        ],
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'Topic Partition Nums',
      name: 'topicPartitionNum',
      initialValue: currentValues.topicPartitionNum ?? 3,
      rules: [{ required: true }],
      props: {
        min: 1,
        max: 20,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR' && values.queueModule === 'parallel',
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
      visible: values => values.middlewareType === 'TUBE',
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
      visible: values => values.middlewareType === 'TUBE',
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
      visible: values => values.middlewareType === 'TUBE',
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
      visible: values => values.middlewareType === 'TUBE',
    },
    {
      type: 'inputnumber',
      label: 'ensemble',
      name: 'mqExtInfo.ensemble',
      initialValue: currentValues.mqExtInfo?.ensemble ?? 3,
      suffix: i18n.t('components.AccessHelper.FieldsConfig.businessFields.EnsembleSuffix'),
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.EnsembleExtra'),
      rules: [
        ({ getFieldValue }) => ({
          validator(_, val) {
            if (val) {
              const writeQuorum = getFieldValue(['mqExtInfo', 'writeQuorum']) || 0;
              const ackQuorum = getFieldValue(['mqExtInfo', 'ackQuorum']) || 0;
              const ensemble = val || 0;
              return ackQuorum <= writeQuorum && writeQuorum <= ensemble
                ? Promise.resolve()
                : Promise.reject(new Error('Max match: ensemble ≥ write quorum ≥ ack quorum'));
            }
            return Promise.resolve();
          },
        }),
      ],
      props: {
        min: 1,
        max: 10,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'write quorum',
      name: 'mqExtInfo.writeQuorum',
      initialValue: currentValues.mqExtInfo?.writeQuorum ?? 3,
      suffix: i18n.t('components.AccessHelper.FieldsConfig.businessFields.WriteQuorumSuffix'),
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.WriteQuorumExtra'),
      props: {
        min: 1,
        max: 10,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'ack quorum',
      name: 'mqExtInfo.ackQuorum',
      initialValue: currentValues.mqExtInfo?.ackQuorum ?? 2,
      suffix: i18n.t('components.AccessHelper.FieldsConfig.businessFields.AckQuorumSuffix'),
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.AckQuorumExtra'),
      props: {
        min: 1,
        max: 10,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'retention time',
      name: 'mqExtInfo.retentionTime',
      initialValue: currentValues.mqExtInfo?.retentionTime ?? 72,
      rules: [
        ({ getFieldValue }) => ({
          validator(_, val) {
            const retentionSize = getFieldValue(['mqExtInfo', 'retentionSize']);
            if ((val === 0 && retentionSize > 0) || (val > 0 && retentionSize === 0)) {
              return Promise.reject(
                new Error(
                  'Can not: retentionTime=0,retentionSize>0 | retentionTime>0,retentionSize=0',
                ),
              );
            }
            if (val) {
              const unit = getFieldValue(['mqExtInfo', 'retentionTimeUnit']);
              const value = unit === 'hours' ? Math.ceil(val / 24) : val;
              return value <= 14 ? Promise.resolve() : Promise.reject(new Error('Max: 14 Days'));
            }
            return Promise.resolve();
          },
        }),
      ],
      suffix: {
        type: 'select',
        name: 'mqExtInfo.retentionTimeUnit',
        initialValue: currentValues.rmqExtInfo?.etentionTimeUnit ?? 'hours',
        props: {
          options: [
            {
              label: 'D',
              value: 'days',
            },
            {
              label: 'H',
              value: 'hours',
            },
          ],
        },
      },
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.RetentionTimeExtra'),
      props: {
        min: -1,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'ttl',
      name: 'mqExtInfo.ttl',
      initialValue: currentValues.mqExtInfo?.ttl ?? 24,
      rules: [
        ({ getFieldValue }) => ({
          validator(_, val) {
            if (val) {
              const unit = getFieldValue(['mqExtInfo', 'ttlUnit']);
              const value = unit === 'hours' ? Math.ceil(val / 24) : val;
              return value <= 14 ? Promise.resolve() : Promise.reject(new Error('Max: 14 Days'));
            }
            return Promise.resolve();
          },
        }),
      ],
      suffix: {
        type: 'select',
        name: 'mqExtInfo.ttlUnit',
        initialValue: currentValues.mqExtInfo?.ttlUnit ?? 'hours',
        props: {
          options: [
            {
              label: 'D',
              value: 'days',
            },
            {
              label: 'H',
              value: 'hours',
            },
          ],
        },
      },
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.TtlExtra'),
      props: {
        min: 1,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'retention size',
      name: 'mqExtInfo.retentionSize',
      initialValue: currentValues.mqExtInfo?.retentionSize ?? -1,
      suffix: {
        type: 'select',
        name: 'mqExtInfo.retentionSizeUnit',
        initialValue: currentValues.mqExtInfo?.retentionSizeUnit ?? 'MB',
        props: {
          options: [
            {
              label: 'MB',
              value: 'MB',
            },
            {
              label: 'GB',
              value: 'GB',
            },
            {
              label: 'TB',
              value: 'TB',
            },
          ],
        },
      },
      extra: i18n.t('components.AccessHelper.FieldsConfig.businessFields.RetentionSizeExtra'),
      props: {
        min: -1,
        precision: 0,
      },
      visible: values => values.middlewareType === 'PULSAR',
    },
  ] as FormItemProps[];

  return pickObjectArray(names, fields);
};
