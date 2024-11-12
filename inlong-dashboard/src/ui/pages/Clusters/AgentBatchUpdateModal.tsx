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

import React, { useMemo, useState } from 'react';
import { Button, message, Modal } from 'antd';
import { ModalProps } from 'antd/es/modal';
import i18n from '@/i18n';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import dayjs, { Dayjs } from 'dayjs';

export interface Props extends ModalProps {
  agentList?: [];
  agentTotal?: number;
  parentId?: number;
  openStatusModal?: () => void;
  getArgs?: (args) => void;
}

const Comp: React.FC<Props> = ({ agentList, agentTotal, parentId, ...modalProps }) => {
  const [form] = useForm();
  const content = () => [
    {
      type: 'inputnumber',
      label: i18n.t('pages.Clusters.Node.BatchNum'),
      name: 'batchNum',
      initialValue: 2,
      rules: [
        {
          required: true,
        },
      ],
    },
    {
      type: 'inputnumber',
      label: i18n.t('pages.Clusters.Node.Interval'),
      name: 'interval',
      initialValue: 5,
      rules: [
        {
          required: true,
        },
      ],
      suffix: i18n.t('pages.Clusters.Node.Minute'),
    },
    {
      type: 'radio',
      label: i18n.t('pages.Clusters.Node.OperationType'),
      name: 'operationType',
      initialValue: 0,
      rules: [{ required: true }],
      props: {
        style: {
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'start',
          alignItems: 'start',
        },
        options: [
          {
            label: i18n.t('pages.Clusters.Node.UpgradeAgentAndInstaller'),
            value: 0,
          },
          {
            label: i18n.t('pages.Clusters.Node.UpgradeAgent'),
            value: 1,
          },
          {
            label: i18n.t('pages.Clusters.Node.UpgradeInstaller'),
            value: 2,
          },
          {
            label: i18n.t('pages.Clusters.Node.RestartAgent'),
            value: 3,
          },
        ],
      },
    },
    {
      type: 'select',
      label: i18n.t('pages.Clusters.Node.Agent.Version'),
      name: 'moduleIdList',
      visible: values => values.operationType !== 2 && values.operationType !== 3,
      props: {
        options: {
          requestAuto: true,
          requestTrigger: ['onOpen'],
          requestService: keyword => ({
            url: '/module/list',
            method: 'POST',
            data: {
              keyword,
              pageNum: 1,
              pageSize: 9999,
            },
          }),
          requestParams: {
            formatResult: result =>
              result?.list
                ?.filter(item => item.type === 'AGENT')
                .map(item => ({
                  ...item,
                  label: `${item.name} ${item.version}`,
                  value: item.id,
                })),
          },
        },
      },
      rules: [
        {
          required: true,
        },
      ],
    },
    {
      type: 'select',
      label: i18n.t('pages.Clusters.Node.AgentInstaller'),
      name: 'installer',
      visible: values => values.operationType === 0 || values.operationType === 2,
      props: {
        options: {
          requestAuto: true,
          requestTrigger: ['onOpen'],
          requestService: keyword => ({
            url: '/module/list',
            method: 'POST',
            data: {
              keyword,
              pageNum: 1,
              pageSize: 9999,
            },
          }),
          requestParams: {
            formatResult: result =>
              result?.list
                ?.filter(item => item.type === 'INSTALLER')
                .map(item => ({
                  ...item,
                  label: `${item.name} ${item.version}`,
                  value: item.id,
                })),
          },
        },
      },
      rules: [
        {
          required: true,
        },
      ],
    },
  ];

  const valuesToSubmitList = (agentList, values, submitList) => {
    switch (values.operationType) {
      case 0:
        agentList.forEach(item => {
          delete item.protocolType;
          item.moduleIdList = [values.moduleIdList, values.installer];
          submitList.push({
            ...item,
            moduleIdList: [values.moduleIdList, values.installer],
            isInstall: true,
          });
        });
        break;
      case 1:
        agentList.forEach(item => {
          delete item.protocolType;
          item.moduleIdList = [values.moduleIdList, item.moduleIdList[1]];
          submitList.push({
            ...item,
          });
          delete item.isInstall;
        });
        break;

      case 2:
        agentList.forEach(item => {
          delete item.protocolType;
          item.isInstall = true;
          item.moduleIdList = [item.moduleIdList[0], values.installer];
          submitList.push({
            ...item,
          });
        });
        break;

      case 3:
        agentList.forEach(item => {
          delete item.protocolType;
          submitList.push({
            ...item,
          });
          delete item.isInstall;
        });
        break;
    }
  };
  const batchUpdate = async (agentList, onOk: (e: React.MouseEvent<HTMLButtonElement>) => void) => {
    const values = await form.validateFields();
    const submitList = [];

    valuesToSubmitList(agentList, values, submitList);
    const baseBatchSize =
      Math.floor(submitList.length / values.batchNum) === 0
        ? 1
        : Math.floor(submitList.length / values.batchNum);
    const remainder = submitList.length % values.batchNum;
    const map = new Map();
    const batchNum = agentList.length < values.batchNum ? agentList.length : values.batchNum;

    for (let i = 1; i <= batchNum; i++) {
      if (i === batchNum) {
        map.set(i, submitList.slice((i - 1) * baseBatchSize, submitList.length));
      } else {
        map.set(i, submitList.slice((i - 1) * baseBatchSize, i * baseBatchSize));
      }
    }
    const args = {
      map: map,
      interval: values.interval,
      operationType: values.operationType,
      ids: agentList.map(item => item.id),
      submitDataList: submitList,
    };
    modalProps.getArgs(args);
    modalProps?.onOk(values);
    modalProps?.openStatusModal();
  };

  return (
    <Modal
      {...modalProps}
      title={i18n.t('pages.Clusters.Node.BatchUpdate')}
      width={600}
      footer={[
        <Button key="cancel" onClick={e => modalProps.onCancel(e)}>
          {i18n.t('basic.Cancel')}
        </Button>,
        <Button
          key="update"
          type="primary"
          onClick={async e => {
            await batchUpdate(agentList, modalProps.onOk);
          }}
        >
          {i18n.t('basic.Confirm')}
        </Button>,
      ]}
    >
      <FormGenerator content={content()} form={form} useMaxWidth labelWrap />
    </Modal>
  );
};

export default Comp;
