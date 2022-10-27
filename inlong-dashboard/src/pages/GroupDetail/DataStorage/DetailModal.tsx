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
import { Skeleton, Modal, message } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useRequest, useUpdateEffect } from '@/hooks';
import { useTranslation } from 'react-i18next';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import { useDefaultMeta, useLoadMeta } from '@/metas';
import request from '@/utils/request';

export interface DetailModalProps extends ModalProps {
  inlongGroupId: string;
  // (True operation, save and adjust interface) Need to upload when editing
  id?: string;
  // others
  onOk?: (values) => void;
}

const Comp: React.FC<DetailModalProps> = ({ inlongGroupId, id, ...modalProps }) => {
  const [form] = useForm();

  const { t } = useTranslation();

  const { defaultValue } = useDefaultMeta('sink');

  // Q: Why sinkType default = '' ?
  // A: Avoid the table of the fields triggering the monitoring of the column change.
  const [sinkType, setSinkType] = useState('');

  const { Entity } = useLoadMeta('sink', sinkType);

  const {
    data,
    loading,
    run: getData,
  } = useRequest(
    id => ({
      url: `/sink/get/${id}`,
    }),
    {
      manual: true,
      formatResult: result => new Entity()?.parse(result) || result,
      onSuccess: result => {
        setSinkType(result.sinkType);
        form.setFieldsValue(result);
      },
    },
  );

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      if (id) {
        getData(id);
      } else {
        form.setFieldsValue({ inlongGroupId });
        setSinkType(defaultValue);
      }
    } else {
      form.resetFields();
      setSinkType('');
    }
  }, [modalProps.visible]);

  const formContent = useMemo(() => {
    return Entity ? Entity.FieldList : [];
  }, [Entity]);

  const onOk = async () => {
    const values = await form.validateFields();
    const submitData = new Entity()?.stringify(values) || values;
    const isUpdate = Boolean(id);
    if (isUpdate) {
      submitData.id = id;
      submitData.version = data?.version;
    }
    await request({
      url: isUpdate ? '/sink/update' : '/sink/save',
      method: 'POST',
      data: {
        ...submitData,
        inlongGroupId,
      },
    });
    modalProps?.onOk(submitData);
    message.success(t('basic.OperatingSuccess'));
  };

  return (
    <Modal title="Sink" width={1200} {...modalProps} onOk={onOk}>
      {loading ? (
        <Skeleton active />
      ) : (
        <FormGenerator
          labelCol={{ span: 4 }}
          wrapperCol={{ span: 12 }}
          content={formContent}
          form={form}
          initialValues={id ? data : { inlongGroupId }}
          onValuesChange={(c, values) => setSinkType(values.sinkType)}
        />
      )}
    </Modal>
  );
};

export default Comp;
