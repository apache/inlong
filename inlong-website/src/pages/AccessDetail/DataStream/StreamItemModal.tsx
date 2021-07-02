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
import { Divider, Modal, message } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import { useUpdateEffect } from '@/hooks';
import { genDataFields } from '@/components/AccessHelper';
import request from '@/utils/request';
import { valuesToData } from '@/pages/AccessCreate/DataStream/helper';
import { pickObject } from '@/utils';

export interface Props extends ModalProps {
  bid: string;
  record?: Record<string, any>;
}

export const genFormContent = (currentValues, businessIdentifier) => {
  const extraParams = {
    businessIdentifier,
  };

  return [
    {
      type: <Divider orientation="left">基础信息</Divider>,
    },
    ...genDataFields(
      ['dataStreamIdentifier', 'name', 'inCharges', 'description'],
      currentValues,
      extraParams,
    ),
    {
      type: <Divider orientation="left">数据来源</Divider>,
    },
    ...genDataFields(['dataSourceType'], currentValues, extraParams),
    {
      type: <Divider orientation="left">数据信息</Divider>,
    },
    ...genDataFields(['dataType', 'rowTypeFields'], currentValues, extraParams),
  ].map(item => {
    const obj = { ...item };

    if (
      obj.name === 'dataStreamIdentifier' ||
      obj.name === 'dataSourceType' ||
      obj.name === 'dataType'
    ) {
      obj.type = 'text';
    }

    return obj;
  });
};

const Comp: React.FC<Props> = ({ bid, record, ...modalProps }) => {
  const [form] = useForm();
  const onOk = async () => {
    const values = {
      ...pickObject(
        ['id', 'businessIdentifier', 'dataStreamIdentifier', 'dataSourceBasicId'],
        record,
      ),
      ...(await form.validateFields()),
    };

    const data = valuesToData(values ? [values] : [], bid);
    const submitData = data.map(item =>
      pickObject(['dbBasicInfo', 'fileBasicInfo', 'streamInfo'], item),
    );
    await request({
      url: '/datastream/updateAll',
      method: 'POST',
      data: submitData?.[0],
    });
    await modalProps?.onOk(values);
    message.success('保存成功');
  };

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      form.resetFields(); // Note that it will cause the form to remount to initiate a select request
    }
    if (Object.keys(record || {})?.length) {
      form.setFieldsValue(record);
    }
  }, [modalProps.visible]);

  return (
    <Modal {...modalProps} title="数据流配置" width={1000} onOk={onOk}>
      <FormGenerator
        labelCol={{ span: 4 }}
        wrapperCol={{ span: 20 }}
        content={genFormContent(record, bid)}
        form={form}
        useMaxWidth
      />
    </Modal>
  );
};

export default Comp;
