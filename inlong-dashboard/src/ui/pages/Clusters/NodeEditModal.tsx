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

import React, { useEffect, useMemo, useState } from 'react';
import i18n from '@/i18n';
import { Modal, message, Button } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import request from '@/core/utils/request';
import rulesPattern from '@/core/utils/pattern';

export interface NodeEditModalProps extends ModalProps {
  id?: number;
  type: string;
  clusterId: number;
}

const NodeEditModal: React.FC<NodeEditModalProps> = ({ id, type, clusterId, ...modalProps }) => {
  const [form] = useForm();
  const [isInstall, setInstallType] = useState(false);

  const { data: savedData, run: getData } = useRequest(
    id => ({
      url: `/cluster/node/get/${id}`,
    }),
    {
      manual: true,
      onSuccess: result => {
        if (type === 'AGENT') {
          // Only keep the first element and give the rest to the 'installer'
          result.installer = result?.moduleIdList.slice(1);
          result.moduleIdList = result?.moduleIdList.slice(0, 1);
          if (result.username) {
            setInstallType(true);
            result.isInstall = true;
          }
        }
        form.setFieldsValue(result);
      },
    },
  );

  const onOk = async () => {
    const values = await form.validateFields();
    const isUpdate = id;
    const submitData = {
      ...values,
      type,
      parentId: savedData?.parentId || clusterId,
    };
    if (isUpdate) {
      submitData.id = id;
      submitData.version = savedData?.version;
      if (type === 'AGENT') {
        if (!submitData.isInstall) {
          submitData.username = '';
          submitData.password = '';
          submitData.sshPort = '';
          submitData.sshKey = '';
        }
      }
    }
    if (type === 'AGENT') {
      if (submitData.installer !== undefined) {
        if (Array.isArray(submitData.moduleIdList)) {
          submitData.moduleIdList = submitData.moduleIdList.concat(submitData.installer);
        } else {
          submitData.moduleIdList = [submitData.moduleIdList].concat(submitData.installer);
        }
      }
    }
    await request({
      url: `/cluster/node/${isUpdate ? 'update' : 'save'}`,
      method: 'POST',
      data: submitData,
    });
    await modalProps?.onOk(submitData);
    message.success(i18n.t('basic.OperatingSuccess'));
  };

  const [agentInstaller, setAgentInstaller] = useState([]);
  const getAgentInstaller = async () => {
    const result = await request({
      url: '/module/list',
      method: 'POST',
      data: {
        pageNum: 1,
        pageSize: 9999,
      },
    });
    setAgentInstaller(result.list?.sort((a, b) => b.modifyTime - a.modifyTime));
    return result;
  };

  const { data: sshKeys, run: getSSHKeys } = useRequest(
    () => ({
      url: '/cluster/node/getManagerSSHPublicKey',
      method: 'GET',
    }),
    {
      manual: true,
      onSuccess: result => {
        form.setFieldValue('sshKey', result);
      },
    },
  );

  const testSSHConnection = async () => {
    const values = await form.validateFields();
    const submitData = {
      ip: values.ip,
      sshPort: values.sshPort,
      username: values.username,
      password: values.password,
    };
    await request({
      url: '/cluster/node/testSSHConnection',
      method: 'POST',
      data: submitData,
    });
    message.success(i18n.t('basic.ConnectionSuccess'));
  };

  useUpdateEffect(() => {
    if (modalProps.open) {
      // open
      setInstallType(false);
      form.resetFields();
      getAgentInstaller();
      if (id) {
        getData(id);
      }
    }
  }, [modalProps.open]);

  useEffect(() => {
    form.setFieldValue('identifyType', 'password');
    if (modalProps.open && !id) {
      form.setFieldValue(
        'installer',
        agentInstaller?.filter(item => item.type === 'INSTALLER')?.[0]?.id,
      );
    }
  }, [agentInstaller]);

  const content = useMemo(() => {
    return Id => [
      {
        type: 'input',
        label: 'IP',
        name: 'ip',
        rules: [
          {
            pattern: rulesPattern.ip,
            required: type === 'AGENT',
            message: i18n.t('pages.Clusters.Node.IpRule'),
          },
        ],
        props: {
          disabled: !!Id,
        },
      },
      {
        type: 'inputnumber',
        label: i18n.t('pages.Clusters.Node.Port'),
        hidden: type === 'AGENT',
        name: 'port',
        rules: [
          {
            pattern: rulesPattern.port,
            message: i18n.t('pages.Clusters.Node.PortRule'),
          },
        ],
        props: {
          min: 0,
          max: 65535,
        },
      },
      {
        type: 'radio',
        label: i18n.t('pages.Clusters.Node.Online'),
        name: 'enabledOnline',
        initialValue: true,
        hidden: type !== 'DATAPROXY',
        rules: [{ required: true }],
        props: {
          options: [
            {
              label: i18n.t('basic.Yes'),
              value: true,
            },
            {
              label: i18n.t('basic.No'),
              value: false,
            },
          ],
        },
      },
      {
        type: 'select',
        label: i18n.t('pages.Clusters.Node.Agent.Version'),
        name: 'moduleIdList',
        hidden: type !== 'AGENT',
        props: {
          filterOption: (input, option) =>
            (option?.label ?? '').toLowerCase().includes(input.toLowerCase()),
          options: agentInstaller
            ?.filter(item => item.type === 'AGENT')
            .map(item => ({
              ...item,
              label: `${item.name} ${item.version}`,
              value: item.id,
            })),
        },
      },
      {
        type: 'textarea',
        label: i18n.t('pages.Clusters.Node.Description'),
        name: 'description',
        props: {
          maxLength: 256,
        },
      },
      {
        type: 'radio',
        label: i18n.t('pages.Clusters.Node.IsInstall'),
        name: 'isInstall',
        initialValue: false,
        hidden: type !== 'AGENT',
        visible: type === 'AGENT',
        rules: [{ required: true }],
        props: {
          onChange: ({ target: { value } }) => {
            setInstallType(value);
          },
          options: [
            {
              label: i18n.t('pages.Clusters.Node.ManualInstall'),
              value: false,
            },
            {
              label: i18n.t('pages.Clusters.Node.SSHInstall'),
              value: true,
            },
          ],
        },
      },
      {
        type: 'radio',
        label: i18n.t('pages.Clusters.Node.IdentifyType'),
        name: 'identifyType',
        initialValue: 'password',
        hidden: type !== 'AGENT',
        visible: values => isInstall,
        rules: [{ required: true }],
        props: {
          onChange: ({ target: { value } }) => {
            if (value === 'sshKey' && !form.getFieldValue('sshKey')) {
              getSSHKeys();
            }
          },
          options: [
            {
              label: i18n.t('pages.Clusters.Node.Password'),
              value: 'password',
            },
            {
              label: i18n.t('pages.Clusters.Node.SSHKey'),
              value: 'sshKey',
            },
          ],
        },
      },
      {
        type: 'input',
        label: i18n.t('pages.Clusters.Node.Username'),
        name: 'username',
        rules: [{ required: true }],
        hidden: type !== 'AGENT',
        visible: isInstall,
      },
      {
        type: 'input',
        label: i18n.t('pages.Clusters.Node.Password'),
        name: 'password',
        rules: [{ required: true }],
        hidden: type !== 'AGENT',
        visible: values =>
          isInstall &&
          (values?.identifyType === 'password' ||
            form.getFieldValue('identifyType') === 'password'),
      },
      {
        type: 'textarea',
        label: i18n.t('pages.Clusters.Node.SSHKey'),
        tooltip: i18n.t('pages.Clusters.Node.SSHKeyHelper'),
        name: 'sshKey',
        rules: [{ required: true }],
        hidden: type !== 'AGENT',
        visible: values =>
          isInstall &&
          (values?.identifyType === 'sshKey' || form.getFieldValue('identifyType') === 'sshKey'),
        props: {
          readOnly: true,
          autoSize: true,
        },
      },
      {
        type: 'input',
        label: i18n.t('pages.Clusters.Node.SSHPort'),
        name: 'sshPort',
        rules: [{ required: true }],
        hidden: type !== 'AGENT',
        visible: values => isInstall,
      },
      {
        type: 'select',
        label: i18n.t('pages.Clusters.Node.AgentInstaller'),
        name: 'installer',
        isPro: type === 'AGENT',
        hidden: type !== 'AGENT',
        props: {
          options: agentInstaller
            ?.filter(item => item.type === 'INSTALLER')
            .map(item => ({
              ...item,
              label: `${item.name} ${item.version}`,
              value: item.id,
            })),
        },
      },
    ];
  }, [isInstall, agentInstaller, modalProps.open]);

  return (
    <Modal
      {...modalProps}
      title={i18n.t('pages.Clusters.Node.Name')}
      afterClose={() => {
        form.resetFields();
      }}
      footer={[
        <Button key="cancel" onClick={e => modalProps.onCancel(e)}>
          {i18n.t('basic.Cancel')}
        </Button>,
        <Button key="save" type="primary" onClick={onOk}>
          {i18n.t('basic.Save')}
        </Button>,
        isInstall && (
          <Button key="run" type="primary" onClick={testSSHConnection}>
            {i18n.t('pages.Nodes.TestConnection')}
          </Button>
        ),
      ]}
    >
      <FormGenerator content={content(id)} form={form} useMaxWidth />
    </Modal>
  );
};

export default NodeEditModal;
