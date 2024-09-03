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

import React, { useState, useMemo } from 'react';
import { Modal, message, Button } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import request from '@/core/utils/request';
import i18n from '@/i18n';
import { GroupDataTemplateInfo } from '@/plugins/groups/common/GroupDataTemplateInfo';
import { dataToValues } from '@/ui/pages/GroupDetail/DataStream/helper';

export interface Props extends ModalProps {
  // Require when edit
  id?: string;
  templateName?: string;
}

const Comp: React.FC<Props> = ({ id, templateName, ...modalProps }) => {
  const [form] = useForm();
  const [hidden, setHidden] = useState(true);
  const { data: savedData, run: getData } = useRequest(
    () => ({
      url: `/template/get`,
      method: 'GET',
      params: {
        templateName: templateName,
      },
    }),
    {
      manual: true,
      formatResult: result => ({
        ...result,
        inCharges: result.inCharges?.split(','),
      }),
      onSuccess: result => {
        if (result.visibleRange === 'TENANT') {
          setHidden(false);
        } else {
          setHidden(true);
        }
        form.setFieldsValue(dataToValues(result));
      },
    },
  );

  const onOk = async () => {
    const values = await form.validateFields();
    const isUpdate = id;
    const submitData = {
      ...values,
      inCharges: values.inCharges?.join(','),
    };

    if (isUpdate) {
      submitData.id = id;
      submitData.version = savedData?.version;
    }
    await request({
      url: `/template/${isUpdate ? 'update' : 'save'}`,
      method: 'POST',
      data: submitData,
    });
    await modalProps?.onOk(submitData);
    message.success(i18n.t('basic.OperatingSuccess'));
  };

  useUpdateEffect(() => {
    if (modalProps.open) {
      if (templateName) {
        getData();
      }
    } else {
      form.resetFields();
      setHidden(true);
    }
  }, [modalProps.open]);

  const content = useMemo(() => {
    return new GroupDataTemplateInfo().renderRow().map(item => {
      if (item.name === 'tenantList') {
        item = { ...item, hidden: hidden, rules: [{ required: !hidden }] };
      }
      return item;
    });
  }, [hidden]);
  return (
    <Modal
      width={1000}
      {...modalProps}
      title={id ? i18n.t('pages.GroupDataTemplate.Edit') : i18n.t('pages.GroupDataTemplate.Create')}
      footer={[
        <Button key="cancel" onClick={e => modalProps.onCancel(e)}>
          {i18n.t('basic.Cancel')}
        </Button>,
        <Button key="save" type="primary" onClick={onOk}>
          {i18n.t('basic.Save')}
        </Button>,
      ]}
    >
      <FormGenerator
        content={content}
        form={form}
        onValuesChange={(c, values) => {
          if (Object.keys(c)[0] === 'visibleRange') {
            if (Object.values(c)[0] === 'TENANT') {
              setHidden(false);
            } else {
              setHidden(true);
            }
          }
        }}
        useMaxWidth
      />
    </Modal>
  );
};

export default Comp;
