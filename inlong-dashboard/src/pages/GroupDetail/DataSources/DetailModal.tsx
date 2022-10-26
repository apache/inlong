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
import { Modal, message } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/hooks';
import { useTranslation } from 'react-i18next';
import { useDefaultMeta, useLoadMeta } from '@/metas';
import request from '@/utils/request';

export interface Props extends ModalProps {
  // When editing, use the ID to call the interface for obtaining details
  id?: string;
  inlongGroupId?: string;
}

const Comp: React.FC<Props> = ({ id, inlongGroupId, ...modalProps }) => {
  const [form] = useForm();
  const { t } = useTranslation();

  const { defaultValue } = useDefaultMeta('source');

  const [type, setType] = useState(defaultValue);

  const { Entity } = useLoadMeta('source', type);

  const { data, run: getData } = useRequest(
    id => ({
      url: `/source/get/${id}`,
    }),
    {
      manual: true,
      formatResult: result => new Entity()?.parse(result) || result,
      onSuccess: result => {
        form.setFieldsValue(result);
        setType(result.sourceType);
      },
    },
  );

  const onOk = async () => {
    const values = await form.validateFields();
    const submitData = new Entity()?.stringify(values) || values;
    const isUpdate = Boolean(id);
    if (isUpdate) {
      submitData.id = id;
      submitData.version = data?.version;
    }
    await request({
      url: `/source/${isUpdate ? 'update' : 'save'}`,
      method: 'POST',
      data: {
        ...submitData,
        inlongGroupId,
      },
    });
    modalProps?.onOk(submitData);
    message.success(t('pages.GroupDetail.Sources.SaveSuccessfully'));
  };

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      if (id) {
        getData(id);
      } else {
        form.setFieldsValue({ inlongGroupId });
      }
    } else {
      form.resetFields();
      setType(defaultValue);
    }
  }, [modalProps.visible]);

  const formContent = useMemo(() => {
    return Entity ? Entity.FieldList : [];
  }, [Entity]);

  return (
    <>
      <Modal {...modalProps} title="Source" width={666} onOk={onOk}>
        <FormGenerator
          content={formContent}
          onValuesChange={(c, values) => setType(values.sourceType)}
          initialValues={id ? data : { inlongGroupId }}
          form={form}
          useMaxWidth
        />
      </Modal>
    </>
  );
};

export default Comp;
