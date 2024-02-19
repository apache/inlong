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

import React, { useEffect, useState } from 'react';
import { Modal, Spin, message, Form, Button } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useForm } from '@/ui/components/FormGenerator';
import { useRequest } from '@/ui/hooks';
import { useTranslation } from 'react-i18next';
import { useDefaultMeta, useLoadMeta } from '@/plugins';
import request from '@/core/utils/request';
import EditableTable, { ColumnsItemProps } from '../EditableTable';
import i18n from '@/i18n';
import { fieldAllTypes } from './FieldTypeConf';

export interface Props extends ModalProps {
  inlongGroupId: string;
  inlongStreamId: string;
  isSource: boolean;
}

const Comp: React.FC<Props> = ({ inlongGroupId, inlongStreamId, isSource, ...modalProps }) => {
  const [form] = useForm();
  const { t } = useTranslation();

  const [sinkType, setSinkType] = useState('');

  const { data, run: getData } = useRequest(
    streamId => ({
      url: `/stream/getBrief`,
      params: {
        groupId: inlongGroupId,
        streamId,
      },
    }),
    {
      manual: true,
    },
  );

  const { data: sinkData, run: getSinkData } = useRequest(
    {
      url: '/sink/list',
      method: 'POST',
      data: {
        inlongGroupId,
        inlongStreamId,
      },
    },
    {
      manual: true,
      onSuccess: result => {
        setSinkType(result.list[0]?.sinkType);
      },
    },
  );

  const fieldTypes = [
    'int',
    'long',
    'float',
    'double',
    'string',
    'date',
    'timestamp',
    'function',
  ].map(item => ({
    label: item,
    value: item,
  }));

  const sinkFieldList: ColumnsItemProps[] = [
    {
      title: i18n.t('meta.Sinks.SourceFieldName'),
      dataIndex: 'sourceFieldName',
      type: 'input',
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Sinks.SourceFieldType'),
      dataIndex: 'sourceFieldType',
      type: 'select',
      initialValue: fieldTypes[0].label,
      rules: [{ required: true }],
      props: {
        options: fieldTypes,
      },
    },
    {
      title: i18n.t('components.FieldList.SinkFieldName'),
      dataIndex: 'fieldName',
      type: 'input',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z_][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Stream.FieldNameRule'),
        },
      ],
    },
    {
      title: i18n.t('components.FieldList.SinkFieldType'),
      dataIndex: 'fieldType',
      type: 'select',
      initialValue: '',
      props: (text, record) => ({
        options: isSource === true ? fieldTypes : fieldAllTypes[sinkType],
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('components.FieldList.FieldComment'),
      dataIndex: 'fieldComment',
    },
  ];

  const fieldList: ColumnsItemProps[] = [
    {
      title: i18n.t('meta.Stream.FieldName'),
      dataIndex: 'fieldName',
      type: 'input',
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Stream.FieldType'),
      dataIndex: 'fieldType',
      type: 'select',
      initialValue: '',
      props: () => ({
        options: fieldTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Stream.FieldComment'),
      dataIndex: 'fieldComment',
    },
  ];

  const onOk = async () => {
    const values = await form.validateFields();
    const isUpdate = Boolean(inlongStreamId);
    const submitData = {
      ...values,
      inlongGroupId,
      inlongStreamId,
    };
    if (isUpdate) {
      submitData.version = data?.version;
    }
    if (isSource === true) {
      await request({
        url: `/stream/update`,
        method: 'POST',
        data: {
          ...submitData,
        },
      });
    } else {
      const sinkSubmitData = {
        inlongGroupId,
        inlongStreamId,
        id: sinkData.list[0].id,
        sinkType: sinkData.list[0].sinkType,
        version: sinkData.list[0].version,
        sinkFieldList: values.sinkFieldList,
      };
      await request({
        url: `/sink/update`,
        method: 'POST',
        data: {
          ...sinkSubmitData,
        },
      });
    }
    modalProps?.onOk(submitData);
    message.success(t('pages.GroupDetail.Sources.SaveSuccessfully'));
  };

  useEffect(() => {
    if (inlongStreamId && modalProps.open) {
      getData(inlongStreamId);
      getSinkData();
    }
  }, [getData, getSinkData, inlongStreamId, modalProps.open]);

  useEffect(() => {
    if (inlongStreamId && modalProps.open && isSource) {
      form.setFieldValue('fieldList', data?.fieldList);
    }
  }, [data?.fieldList, form, inlongStreamId, isSource, modalProps.open]);

  useEffect(() => {
    if (inlongStreamId && isSource === false && modalProps.open) {
      if (sinkData?.list[0]?.sinkFieldList?.length === 0) {
        form.setFieldsValue({
          sinkFieldList: data?.fieldList?.map(item => ({
            sourceFieldName: item.fieldName,
            sourceFieldType: item.fieldType,
            fieldName: item.fieldName,
            fieldType: '',
          })),
        });
      } else {
        form.setFieldValue('sinkFieldList', sinkData?.list[0]?.sinkFieldList);
      }
    }
  }, [data, form, inlongStreamId, isSource, modalProps.open, sinkData?.list]);

  return (
    <>
      <Modal
        {...modalProps}
        title={
          isSource === true
            ? t('components.FieldList.CreateSource')
            : t('components.FieldList.CreateSink')
        }
        width={1000}
        onOk={onOk}
        footer={[
          <Button key="cancel" onClick={e => modalProps.onCancel(e)}>
            {t('pages.GroupDetail.Sink.Cancel')}
          </Button>,
          <Button key="save" type="primary" onClick={() => onOk()}>
            {t('pages.GroupDetail.Sink.Save')}
          </Button>,
        ]}
      >
        <Form form={form} initialValues={isSource === true ? data : sinkData?.list[0]}>
          <Form.Item name={isSource === true ? 'fieldList' : 'sinkFieldList'}>
            <EditableTable
              columns={isSource === true ? fieldList : sinkFieldList}
              dataSource={isSource === true ? data?.fieldList : sinkData?.list[0]?.sinkFieldList}
              canBatchAdd={true}
              upsetByFieldKey={true}
            ></EditableTable>
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
};

export default Comp;
