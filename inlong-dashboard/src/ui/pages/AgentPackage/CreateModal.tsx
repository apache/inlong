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

import React, { useMemo } from 'react';
import { Button, message, Modal } from 'antd';
import i18n from '@/i18n';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import request from '@/core/utils/request';
import { ModalProps } from 'antd/es/modal';

export interface Props extends ModalProps {
  // Require when edit
  id?: string;
  type?: string;
}

const Comp: React.FC<Props> = ({ id, type, ...modalProps }) => {
  const [form] = useForm();

  const content = useMemo(() => {
    return [
      {
        type: 'input',
        label: i18n.t('pages.PackageAgent.Config.FileName'),
        name: 'fileName',
        rules: [{ required: true }],
      },
      {
        type: 'input',
        label: i18n.t('pages.PackageAgent.Config.DownloadUrl'),
        name: 'downloadUrl',
        rules: [{ required: true }],
      },
      {
        type: 'input',
        label: i18n.t('pages.PackageAgent.Config.Md5'),
        name: 'md5',
        rules: [{ required: true }],
      },
      {
        type: 'input',
        label: i18n.t('pages.PackageAgent.Config.StoragePath'),
        name: 'storagePath',
        rules: [{ required: true }],
      },
    ];
  }, []);

  const { data, run: getData } = useRequest(
    id => ({
      url: `/package/get/${id}`,
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
      url: isUpdate ? '/package/update' : '/package/save',
      method: 'POST',
      data: { ...values },
    });
    await modalProps?.onOk(values);
    message.success(i18n.t('basic.OperatingSuccess'));
  };

  useUpdateEffect(() => {
    if (modalProps.open) {
      if (id) {
        getData(id);
      }
    } else {
      form.resetFields();
    }
  }, [modalProps.open]);

  return (
    <Modal
      {...modalProps}
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
