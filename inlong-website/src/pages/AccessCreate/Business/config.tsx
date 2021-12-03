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
import { Divider } from 'antd';
import i18n from '@/i18n';
import { genBusinessFields } from '@/components/AccessHelper';

export const getFormContent = ({ changedValues, isUpdate }) =>
  genBusinessFields(
    [
      {
        type: (
          <Divider orientation="left">
            {i18n.t('pages.AccessCreate.Business.config.AccessRequirements')}
          </Divider>
        ),
      },
      'middlewareType',
      'queueModule',
      'topicPartitionNum',
      {
        type: (
          <Divider orientation="left">
            {i18n.t('pages.AccessCreate.Business.config.BusinessInformation')}
          </Divider>
        ),
      },
      'name',
      'cnName',
      'inCharges',
      'description',
      {
        type: (
          <Divider orientation="left">
            {i18n.t('pages.AccessCreate.Business.config.AccessScale')}
          </Divider>
        ),
        visible: values => values.middlewareType === 'TUBE',
      },
      'dailyRecords',
      'dailyStorage',
      'peakRecords',
      'maxLength',
      {
        type: (
          <Divider orientation="left">
            {i18n.t('components.AccessHelper.FieldsConfig.businessFields.DataCopyTitle')}
          </Divider>
        ),
        visible: values => values.middlewareType === 'PULSAR',
      },
      // 'mqExtInfo.ensemble',
      'mqExtInfo.writeQuorum',
      'mqExtInfo.ackQuorum',
      {
        type: (
          <Divider orientation="left">
            {i18n.t('components.AccessHelper.FieldsConfig.businessFields.DataStoragePeriodTitle')}
          </Divider>
        ),
        visible: values => values.middlewareType === 'PULSAR',
      },
      'mqExtInfo.retentionTime',
      'mqExtInfo.ttl',
      'mqExtInfo.retentionSize',
    ],
    changedValues,
  ).map(item => {
    const obj = { ...item };
    if (isUpdate && obj.name === 'name') {
      obj.props = {
        ...obj.props,
        disabled: true,
      };
    }

    return obj;
  });
