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
import { Modal, message } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useRequest, useSelector, useUpdateEffect } from '@/ui/hooks';
import i18n from '@/i18n';
import request from '@/core/utils/request';
import { State } from '@/core/stores';

export interface Props extends ModalProps {
  id?: number;
  record?: Record<string, any>;
}

const Comp: React.FC<Props> = ({ id, ...modalProps }) => {
  const [form] = useForm();
  const userName = useSelector<State, State['userName']>(state => state.userName);

  const formContent = useMemo(() => {
    return [
      {
        type: 'input',
        label: i18n.t('pages.TenantRole.config.Name'),
        name: 'tenant',
        rules: [{ required: true }],
      },
      {
        type: 'select',
        label: i18n.t('pages.TenantRole.config.UserName'),
        name: 'username',
        rules: [{ required: true }],
        props: values => ({
          disabled: values?.status === 101,
          showSearch: true,
          allowClear: true,
          filterOption: false,
          options: {
            requestTrigger: ['onOpen', 'onSearch'],
            requestService: keyword => ({
              url: '/user/listAll',
              method: 'POST',
              data: {
                keyword,
                pageNum: 1,
                pageSize: 10,
              },
            }),
            requestParams: {
              formatResult: result =>
                result?.list?.map(item => ({
                  ...item,
                  label: item.name,
                  value: item.name,
                })),
            },
          },
        }),
      },
      {
        type: 'select',
        label: i18n.t('pages.TenantRole.config.TenantRole'),
        name: 'roleCode',
        rules: [{ required: true }],
        props: {
          options: [
            {
              label: 'TENANT_ADMIN',
              value: 'TENANT_ADMIN',
            },
            {
              label: 'TENANT_OPERATOR',
              value: 'TENANT_OPERATOR',
            },
          ],
        },
      },
    ];
  }, []);

  const { data, run: getData } = useRequest(
    id => ({
      url: `/role/tenant/get/${id}`,
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
    const submitData = {
      ...values,
    };
    const isUpdate = Boolean(id);
    if (isUpdate) {
      submitData.id = id;
      submitData.version = data?.version;
    }
    await request({
      url: isUpdate ? '/role/tenant/update' : '/role/tenant/save',
      method: 'POST',
      data: { ...submitData },
    });
    await modalProps?.onOk(submitData);
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
      title={id ? i18n.t('basic.Edit') : i18n.t('pages.TenantRole.New')}
      width={600}
      onOk={onOk}
    >
      <FormGenerator
        labelCol={{ span: 4 }}
        wrapperCol={{ span: 20 }}
        content={formContent}
        form={form}
        initialValues={id ? data : ''}
        useMaxWidth
      />
    </Modal>
  );
};

export default Comp;
