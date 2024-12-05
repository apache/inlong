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
import i18n from '@/i18n';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import request from '@/core/utils/request';
import { ModalProps } from 'antd/es/modal';
import rulesPattern from '@/core/utils/pattern';

export interface Props extends ModalProps {
  id?: string;
  type?: string;
}

const Comp: React.FC<Props> = ({ id, type, ...modalProps }) => {
  const [form] = useForm();
  const [isCreate, setCreate] = useState(false);
  const content = useMemo(() => {
    return [
      {
        type: 'input',
        label: i18n.t('pages.ModuleAgent.Config.Name'),
        name: 'name',
        rules: [{ required: true }],
      },
      {
        type: 'input',
        label: i18n.t('pages.ModuleAgent.Config.Version'),
        name: 'version',
        rules: [
          {
            required: true,
            pattern: rulesPattern.version,
            message: i18n.t('meta.Sources.File.VersionRule'),
          },
        ],
      },
      {
        type: 'select',
        label: i18n.t('pages.ModuleAgent.Config.Package'),
        name: 'packageId',
        rules: [{ required: true }],
        props: {
          showSearch: true,
          allowClear: true,
          filterOption: false,
          options: {
            requestAuto: true,
            requestTrigger: ['onOpen', 'onSearch'],
            requestService: keyword => ({
              url: '/package/list',
              method: 'POST',
              data: {
                keyword,
                pageNum: 1,
                pageSize: 9999,
                type: type,
              },
            }),
            requestParams: {
              formatResult: result =>
                result?.list?.map(item => ({
                  ...item,
                  label: item.fileName,
                  value: item.id,
                })),
            },
          },
        },
      },
      {
        type: 'textarea',
        label: i18n.t('pages.ModuleAgent.Config.CheckCommand'),
        name: 'checkCommand',
        initialValue:
          type === 'INSTALLER'
            ? 'echo "installer"'
            : "ps aux | grep core.AgentMain | grep java | grep -v grep | awk '{print $2}'",
        props: {
          showCount: true,
          maxLength: 1000,
        },
      },
      {
        type: 'textarea',
        label: i18n.t('pages.ModuleAgent.Config.InstallCommand'),
        name: 'installCommand',
        initialValue:
          type === 'INSTALLER'
            ? 'echo ""'
            : 'cd ~/inlong/inlong-agent/bin;sh agent.sh stop;rm -rf ~/inlong/inlong-agent-temp;mkdir -p ~/inlong/inlong-agent-temp;cp -r ~/inlong/inlong-agent/.localdb ',
        props: {
          showCount: true,
          maxLength: 1000,
        },
      },
      {
        type: 'textarea',
        label: i18n.t('pages.ModuleAgent.Config.StartCommand'),
        name: 'startCommand',
        initialValue:
          type === 'INSTALLER' ? 'echo ""' : 'cd ~/inlong/inlong-agent/bin;sh agent.sh start',
        props: {
          showCount: true,
          maxLength: 1000,
        },
      },
      {
        type: 'textarea',
        label: i18n.t('pages.ModuleAgent.Config.StopCommand'),
        name: 'stopCommand',
        initialValue:
          type === 'INSTALLER' ? 'echo ""' : 'cd ~/inlong/inlong-agent/bin;sh agent.sh stop',
        props: {
          showCount: true,
          maxLength: 1000,
        },
      },
      {
        type: 'textarea',
        label: i18n.t('pages.ModuleAgent.Config.UninstallCommand'),
        name: 'uninstallCommand',
        initialValue: type === 'INSTALLER' ? 'echo ""' : 'echo empty uninstall  cmd',
        props: {
          showCount: true,
          maxLength: 1000,
        },
      },
    ];
  }, [type]);

  const { data, run: getData } = useRequest(
    id => ({
      url: `/module/get/${id}`,
    }),
    {
      manual: true,
      onSuccess: result => {
        form.setFieldsValue(result);
      },
    },
  );

  const onOk = async () => {
    const values = await form.validateFields();
    const isUpdate = Boolean(id);
    if (isUpdate) {
      values.id = id;
    } else {
      values.type = type;
    }
    await request({
      url: isUpdate ? '/module/update' : '/module/save',
      method: 'POST',
      data: { ...values },
    });
    await modalProps?.onOk(values);
    message.success(i18n.t('basic.OperatingSuccess'));
  };

  useUpdateEffect(() => {
    if (modalProps.open) {
      if (id) {
        setCreate(false);
        getData(id);
      } else {
        setCreate(true);
        // here need a data which id is 1 to init create form
      }
    } else {
      form.resetFields();
    }
  }, [modalProps.open]);

  return (
    <Modal
      {...modalProps}
      width={800}
      title={id ? i18n.t('basic.Edit') : i18n.t('basic.Create')}
      footer={[
        <Button key="cancel" onClick={e => modalProps.onCancel(e)}>
          {i18n.t('basic.Cancel')}
        </Button>,
        <Button key="save" type="primary" onClick={onOk}>
          {i18n.t('basic.Save')}
        </Button>,
      ]}
    >
      <FormGenerator content={content} form={form} initialValues={id ? data : {}} useMaxWidth />
    </Modal>
  );
};

export default Comp;
