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
import { Modal } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/hooks';
import { useTranslation } from 'react-i18next';
import { getCreateFormContent as getFileCreateFormContent } from './FileConfig';
import { getCreateFormContent as getDbCreateFormContent } from './DbConfig';

export interface Props extends ModalProps {
  type: 'DB' | 'FILE';
  // When editing, use the ID to call the interface for obtaining details
  id?: string;
  // Pass when editing, directly echo the record data
  record?: Record<string, any>;
  // Additional form configuration
  content?: any[];
}

const Comp: React.FC<Props> = ({ type, id, content = [], record, ...modalProps }) => {
  const [form] = useForm();
  const { t } = useTranslation();

  const onOk = async () => {
    const values = await form.validateFields();
    modalProps?.onOk(values);
  };

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      form.resetFields(); // Note that it will cause the form to remount to initiate a select request
      id && getData(id);
    }
    if (!id && Object.keys(record || {})?.length) {
      form.setFieldsValue(record);
    }
  }, [modalProps.visible]);

  const { run: getData } = useRequest(
    id => ({
      url: `/datasource/${type.toLowerCase()}/getDetail/${id}`,
    }),
    {
      manual: true,
      onSuccess: result => form.setFieldsValue(result),
    },
  );

  const getCreateFormContent = useMemo(() => {
    return {
      DB: getDbCreateFormContent,
      FILE: getFileCreateFormContent,
    }[type];
  }, [type]);

  return (
    <>
      <Modal
        {...modalProps}
        title={
          type === 'DB' ? 'DB' : t('components.AccessHelper.DataSourcesEditor.CreateModal.File')
        }
        onOk={onOk}
      >
        <FormGenerator content={content.concat(getCreateFormContent())} form={form} useMaxWidth />
      </Modal>
    </>
  );
};

export default Comp;
