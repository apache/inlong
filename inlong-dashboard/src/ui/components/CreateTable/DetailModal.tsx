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
import { Modal, message, Button } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useDispatch, useRequest } from '@/ui/hooks';
import { useTranslation } from 'react-i18next';
import request from '@/core/utils/request';
import i18n from '@/i18n';
import { SinkMetaType, useLoadMeta } from '@/plugins';
import EditableTable from '../EditableTable';
import { sinks } from '@/plugins/sinks';

export interface Props extends ModalProps {
  sinkType: string;
  inlongGroupId: string;
  inlongStreamId: string;
  sinkObj: any;
}

const Comp: React.FC<Props> = ({
  sinkType,
  inlongGroupId,
  inlongStreamId,
  sinkObj,
  ...modalProps
}) => {
  const [form] = useForm();
  const { t } = useTranslation();
  const dispatch = useDispatch();

  const { loading, Entity } = useLoadMeta<SinkMetaType>('sink', sinkType);

  const { data, run: getData } = useRequest(
    id => ({
      url: `/sink/get/${id}`,
    }),
    {
      manual: true,
      formatResult: result => new Entity()?.parse(result) || result,
      onSuccess: result => {
        form.setFieldsValue(result);
      },
    },
  );

  const { data: streamData, run: getStreamData } = useRequest(
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

  const onOk = async () => {
    const values = await form.validateFields();
    const isUpdate = Boolean(sinkObj?.id);
    const submitData = {
      ...sinkObj,
      ...values,
      inlongGroupId,
      inlongStreamId,
      enableCreateResource: 1,
    };
    if (isUpdate) {
      submitData.id = sinkObj?.id;
      submitData.version = sinkObj?.version;
    }
    dispatch({
      type: 'setSyncTableData',
      payload: {
        syncTableData: values,
      },
    });
    modalProps?.onOk(values);
    message.success(t('pages.GroupDetail.Sources.SaveSuccessfully'));
  };

  const getFormContent = [
    {
      type: 'text',
      label: i18n.t('components.CreateTable.TableType'),
      name: 'createType',
      initialValue: sinks.find(c => c.value === sinkType)?.label || sinkType,
    },
  ];

  const formContent = useMemo(() => {
    if (Entity) {
      const row = new Entity().renderSyncCreateTableRow();
      return [].concat(getFormContent, row).map(item => ({
        ...item,
        col: item.type === EditableTable ? 24 : 12,
      }));
    }
  }, [Entity]);

  useEffect(() => {
    if (
      Entity &&
      streamData &&
      streamData.fieldList?.length &&
      Entity.FieldList?.some(item => item.name === 'sinkFieldList')
    ) {
      form.setFieldsValue({
        sinkFieldList: streamData.fieldList.map(item => ({
          sourceFieldName: item.fieldName,
          sourceFieldType: item.fieldType,
          fieldName: item.fieldName,
          fieldType: '',
        })),
      });
    }
  }, [Entity, streamData, form]);

  useEffect(() => {
    if (inlongStreamId && modalProps.open) {
      getStreamData(inlongStreamId);
      if (sinkObj?.id) {
        getData(sinkObj?.id);
      }
    }
  }, [getData, getStreamData, inlongStreamId, modalProps.open, sinkObj?.id]);

  return (
    <>
      <Modal
        {...modalProps}
        title={i18n.t('components.CreateTable.Table')}
        width={1200}
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
        <FormGenerator
          form={form}
          col={12}
          labelCol={{ flex: '0 0 200px' }}
          wrapperCol={{ flex: '1' }}
          content={formContent}
        />
      </Modal>
    </>
  );
};

export default Comp;
