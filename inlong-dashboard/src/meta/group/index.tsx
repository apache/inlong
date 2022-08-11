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
import StaffSelect from '@/components/StaffSelect';
import { FormItemProps } from '@/components/FormGenerator';
import { pickObjectArray } from '@/utils';
import i18n from '@/i18n';

export default (
  names: (string | FormItemProps)[],
  currentValues: Record<string, any> = {},
): FormItemProps[] => {
  const fields: FormItemProps[] = [
    {
      type: 'input',
      label: i18n.t('meta.Group.InlongGroupId'),
      name: 'inlongGroupId',
      props: {
        maxLength: 32,
      },
      rules: [
        { required: true },
        {
          pattern: /^[a-z_\-\d]+$/,
          message: i18n.t('meta.Group.InlongGroupIdRules'),
        },
      ],
      initialValue: currentValues.inlongGroupId,
    },
    {
      type: 'text',
      label: currentValues.mqType === 'TUBEMQ' ? 'TubeMQ Topic' : 'Pulsar Namespace',
      name: 'mqResource',
      initialValue: currentValues.mqResource,
    },
    {
      type: 'input',
      label: i18n.t('meta.Group.InlongGroupName'),
      name: 'name',
      props: {
        maxLength: 32,
      },
      initialValue: currentValues.name,
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: i18n.t('meta.Group.BusinessOwners'),
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      rules: [
        {
          required: true,
        },
      ],
      extra: i18n.t('meta.Group.BusinessOwnersExtra'),
    },
    {
      type: 'textarea',
      label: i18n.t('meta.Group.BusinessIntroduction'),
      name: 'description',
      props: {
        showCount: true,
        maxLength: 100,
      },
      initialValue: currentValues.description,
    },
    {
      type: 'radio',
      label: i18n.t('meta.Group.MessageMiddleware'),
      name: 'mqType',
      initialValue: currentValues.mqType ?? 'TUBEMQ',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: 'TubeMQ',
            value: 'TUBEMQ',
          },
          {
            label: 'Pulsar',
            value: 'PULSAR',
          },
        ],
      },
    },
    {
      type: 'radio',
      label: i18n.t('meta.Group.QueueModule'),
      name: 'queueModule',
      initialValue: currentValues.queueModule ?? 'SERIAL',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('meta.Group.Parallel'),
            value: 'PARALLEL',
          },
          {
            label: i18n.t('meta.Group.Serial'),
            value: 'SERIAL',
          },
        ],
      },
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: i18n.t('meta.Group.PartitionNum'),
      name: 'partitionNum',
      initialValue: currentValues.partitionNum ?? 3,
      rules: [{ required: true }],
      props: {
        min: 1,
        max: 20,
        precision: 0,
      },
      visible: values => values.mqType === 'PULSAR' && values.queueModule === 'PARALLEL',
    },
    {
      type: 'inputnumber',
      label: i18n.t('meta.Group.NumberOfAccess'),
      name: 'dailyRecords',
      initialValue: currentValues.dailyRecords,
      rules: [{ required: true }],
      suffix: i18n.t('meta.Group.thousand/day'),
      props: {
        min: 1,
        precision: 0,
      },
      visible: values => values.mqType === 'TUBEMQ',
    },
    {
      type: 'inputnumber',
      label: i18n.t('meta.Group.AccessSize'),
      name: 'dailyStorage',
      initialValue: currentValues.dailyStorage,
      rules: [{ required: true }],
      suffix: i18n.t('meta.Group.GB/Day'),
      props: {
        min: 1,
        precision: 0,
      },
      visible: values => values.mqType === 'TUBEMQ',
    },
    {
      type: 'inputnumber',
      label: i18n.t('meta.Group.AccessPeakPerSecond'),
      name: 'peakRecords',
      initialValue: currentValues.peakRecords,
      rules: [{ required: true }],
      suffix: i18n.t('meta.Group.Stripe/Second'),
      props: {
        min: 1,
        precision: 0,
      },
      visible: values => values.mqType === 'TUBEMQ',
    },
    {
      type: 'inputnumber',
      label: i18n.t('meta.Group.SingleStripMaximumLength'),
      name: 'maxLength',
      initialValue: currentValues.maxLength,
      rules: [{ required: true }],
      suffix: 'Byte',
      props: {
        min: 1,
        precision: 0,
      },
      visible: values => values.mqType === 'TUBEMQ',
    },
    {
      type: 'inputnumber',
      label: 'ensemble',
      name: 'ensemble',
      initialValue: currentValues?.ensemble ?? 3,
      suffix: i18n.t('meta.Group.EnsembleSuffix'),
      extra: i18n.t('meta.Group.EnsembleExtra'),
      rules: [
        ({ getFieldValue }) => ({
          validator(_, val) {
            if (val) {
              const writeQuorum = getFieldValue(['writeQuorum']) || 0;
              const ackQuorum = getFieldValue(['ackQuorum']) || 0;
              return ackQuorum <= writeQuorum && writeQuorum <= val
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
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'Write Quorum',
      name: 'writeQuorum',
      initialValue: currentValues?.writeQuorum ?? 3,
      suffix: i18n.t('meta.Group.WriteQuorumSuffix'),
      extra: i18n.t('meta.Group.WriteQuorumExtra'),
      props: {
        min: 1,
        max: 10,
        precision: 0,
      },
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'ACK Quorum',
      name: 'ackQuorum',
      initialValue: currentValues?.ackQuorum ?? 2,
      suffix: i18n.t('meta.Group.AckQuorumSuffix'),
      extra: i18n.t('meta.Group.AckQuorumExtra'),
      props: {
        min: 1,
        max: 10,
        precision: 0,
      },
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'Time To Live',
      name: 'ttl',
      initialValue: currentValues?.ttl ?? 24,
      rules: [
        ({ getFieldValue }) => ({
          validator(_, val) {
            if (val) {
              const unit = getFieldValue(['ttlUnit']);
              const value = unit === 'hours' ? Math.ceil(val / 24) : val;
              return value <= 14 ? Promise.resolve() : Promise.reject(new Error('Max: 14 Days'));
            }
            return Promise.resolve();
          },
        }),
      ],
      suffix: {
        type: 'select',
        name: 'ttlUnit',
        initialValue: currentValues?.ttlUnit ?? 'hours',
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
      extra: i18n.t('meta.Group.TtlExtra'),
      props: {
        min: 1,
        precision: 0,
      },
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'Retention Time',
      name: 'retentionTime',
      initialValue: currentValues?.retentionTime ?? 72,
      rules: [
        ({ getFieldValue }) => ({
          validator(_, val) {
            const retentionSize = getFieldValue(['retentionSize']);
            if ((val === 0 && retentionSize > 0) || (val > 0 && retentionSize === 0)) {
              return Promise.reject(
                new Error(
                  'Can not: retentionTime=0, retentionSize>0 | retentionTime>0, retentionSize=0',
                ),
              );
            }
            if (val) {
              const unit = getFieldValue(['retentionTimeUnit']);
              const value = unit === 'hours' ? Math.ceil(val / 24) : val;
              return value <= 14 ? Promise.resolve() : Promise.reject(new Error('Max: 14 Days'));
            }
            return Promise.resolve();
          },
        }),
      ],
      suffix: {
        type: 'select',
        name: 'retentionTimeUnit',
        initialValue: currentValues?.retentionTimeUnit ?? 'hours',
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
      extra: i18n.t('meta.Group.RetentionTimeExtra'),
      props: {
        min: -1,
        precision: 0,
      },
      visible: values => values.mqType === 'PULSAR',
    },
    {
      type: 'inputnumber',
      label: 'Retention Size',
      name: 'retentionSize',
      initialValue: currentValues?.retentionSize ?? -1,
      suffix: {
        type: 'select',
        name: 'retentionSizeUnit',
        initialValue: currentValues?.retentionSizeUnit ?? 'MB',
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
      extra: i18n.t('meta.Group.RetentionSizeExtra'),
      props: {
        min: -1,
        precision: 0,
      },
      visible: values => values.mqType === 'PULSAR',
    },
  ] as FormItemProps[];

  return pickObjectArray(names, fields);
};
