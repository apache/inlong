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
import { Link } from 'react-router-dom';
import i18n from '@/i18n';
import { statusList, genStatusTag } from './status';
import { timestampFormat } from '@/core/utils';
import StatusTag from '@/ui/components/StatusTag';

export const getFilterFormContent = defaultValues => [
  {
    type: 'inputnumber',
    name: 'processId',
    props: {
      style: { width: 150 },
      min: 1,
      max: 100000000,
      placeholder: i18n.t('pages.Approvals.ProcessID'),
    },
  },
  {
    type: 'inputsearch',
    label: i18n.t('pages.Approvals.Applicant'),
    name: 'applicant',
  },
  {
    type: 'select',
    label: i18n.t('basic.Status'),
    name: 'statusSet',
    initialValue: defaultValues.status,
    props: {
      mode: 'multiple',
      dropdownMatchSelectWidth: false,
      options: statusList,
      allowClear: true,
    },
  },
];

export const getColumns = activedName => [
  {
    title: i18n.t('pages.Approvals.ProcessID'),
    dataIndex: 'processId',
    render: (text, record) => (
      <Link
        to={`/process/${activedName}/${text}?taskId=${record.id}&inlongGroupMode=${record.showInList?.[0]?.inlongGroupMode}`}
      >
        {text}
      </Link>
    ),
  },
  {
    title: i18n.t('pages.Approvals.Applicant'),
    dataIndex: 'applicant',
  },
  {
    title: i18n.t('pages.Approvals.ApplicationType'),
    dataIndex: 'processDisplayName',
  },
  {
    title: i18n.t('pages.Approvals.GroupId'),
    dataIndex: 'inlongGroupId',
    render: (text, record) =>
      record.showInList
        ?.filter(item => item.inlongGroupId)
        ?.map(item => item.inlongGroupId)
        .join(';'),
  },
  {
    title: i18n.t('pages.Approvals.ConsumeName'),
    dataIndex: 'consumerGroup',
    width: 200,
    render: (text, record) =>
      record.showInList
        ?.filter(item => item.consumerGroup)
        ?.map(item => item.consumerGroup)
        .join(';'),
  },
  {
    title: i18n.t('pages.Approvals.GroupMode'),
    dataIndex: 'inlongGroupMode',
    render: (text, record) => {
      if (record.processName === 'APPLY_CONSUME_PROCESS') {
        return (
          <StatusTag
            type={'warning'}
            icon={<span />}
            title={i18n.t('pages.Approvals.GroupMode.Subscription')}
          />
        );
      }
      return record.showInList?.[0]?.inlongGroupMode === 1 ? (
        <StatusTag
          type={'success'}
          icon={<span />}
          title={i18n.t('pages.Approvals.GroupMode.DataSync')}
        />
      ) : (
        <StatusTag
          type={'primary'}
          icon={<span />}
          title={i18n.t('pages.Approvals.GroupMode.Ingestion')}
        />
      );
    },
  },
  {
    title: i18n.t('pages.Approvals.ApplicationTime'),
    dataIndex: 'startTime',
    render: text => timestampFormat(text),
  },
  {
    title: i18n.t('pages.Approvals.ProcessingTime'),
    dataIndex: 'endTime',
    render: (text, record) => {
      if (record.status === 'PENDING') {
        return '';
      } else {
        return timestampFormat(text);
      }
    },
  },
  {
    title: i18n.t('basic.Status'),
    dataIndex: 'status',
    render: text => genStatusTag(text),
  },
  {
    title: i18n.t('basic.Operating'),
    dataIndex: 'action',
    render: (text, record) => (
      <Link
        to={`/process/${activedName}/${record.processId}?taskId=${record.id}&inlongGroupMode=${record.showInList?.[0]?.inlongGroupMode}`}
      >
        {record.status === 'PENDING' ? i18n.t('pages.Approvals.Approval') : i18n.t('basic.Detail')}
      </Link>
    ),
  },
];
